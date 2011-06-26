{-# LANGUAGE GADTs, OverloadedStrings, Rank2Types, ScopedTypeVariables #-}
module Export( fastExport ) where

import Marks
import Utils
import Stash

import Control.Monad ( when, forM, forM_, unless )
import Control.Monad.Trans ( liftIO )
import Control.Monad.State.Strict( gets )
import Control.Exception( finally )
import Data.List ( sort )
import Data.Maybe ( catMaybes, fromJust )
import Data.DateTime ( formatDateTime, fromClockTime )
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.ByteString.Lazy.UTF8 as BLU
import Data.IORef ( newIORef, modifyIORef, readIORef )
import Prelude hiding ( readFile )
import System.Directory ( canonicalizePath )
import System.FilePath ( takeFileName )
import System.Time ( toClockTime )

import Darcs.Patch.Effect ( Effect )
import Darcs.Patch.PatchInfoAnd ( PatchInfoAnd, info )
import Darcs.Repository ( Repository, RepoJob(..), readRepo, withRepository )
import Darcs.Repository.Cache ( HashedDir( HashedPristineDir ) )
import Darcs.Repository.HashedRepo ( readHashedPristineRoot )
import Darcs.Repository.HashedIO ( cleanHashdir )
import Darcs.Repository.Internal ( identifyRepositoryFor )
import Darcs.Repository.InternalTypes ( extractCache )
import Darcs.Patch ( effect, listTouchedFiles, apply, RepoPatch )
import Darcs.Patch.Info ( isTag, PatchInfo, piAuthor, piName, piLog, piDate )
import Darcs.Patch.Set ( Origin, PatchSet(..), Tagged(..), newset2FL )
import Darcs.Witnesses.Eq ( (=\/=), MyEq(..), isIsEq, unsafeCompare )
import Darcs.Witnesses.Ordered ( FL(..), RL(..), nullFL )
import Darcs.Witnesses.Sealed ( flipSeal, FlippedSeal(..), seal2, Sealed2(..)
                              , unsafeUnseal2 )
import Darcs.Utils ( withCurrentDirectory )

import Storage.Hashed.Darcs
import Storage.Hashed.Monad hiding ( createDirectory, exists )
import Storage.Hashed.Tree( findTree, emptyTree, listImmediate, findTree )
import Storage.Hashed.AnchoredPath( anchorPath, appendPath, floatPath
                                  , AnchoredPath(..) )

-- Name, Reset mark and patches.
data Branch p = Branch String Int (Sealed2 (FL (PatchInfoAnd p)))

inOrderTag :: (Effect p) => [PatchInfo] -> (PatchInfoAnd p) x y -> Bool
inOrderTag tags p = isTag (info p) && info p `elem` tags && nullFL (effect p)

next :: (Effect p) => [PatchInfo] -> Int -> (PatchInfoAnd p) x y -> Int
next tags n p = if inOrderTag tags p then n else n + 1

tagName :: (PatchInfoAnd p) x y -> String
tagName = map cleanup . drop 4 . patchName
  where cleanup x | x `elem` bad = '_'
                  | otherwise = x
        -- FIXME many more chars are probably illegal
        bad = " ."

patchName :: (PatchInfoAnd p) x y -> String
patchName = piName . info

patchDate :: (PatchInfoAnd p) x y -> String
patchDate = formatDateTime "%s +0000" . fromClockTime . toClockTime .
  piDate . info

patchAuthor :: (PatchInfoAnd p) x y -> String
patchAuthor p = case span (/='<') author of
  (_, "") -> case span (/='@') author of
                 -- john@home -> john <john@home>
                 (n, "") -> n ++ " <unknown>"
                 (name, _) -> name ++ " <" ++ author ++ ">"
  (n, rest) -> case span (/='>') $ tail rest of
    (email, _) -> n ++ "<" ++ email ++ ">"
 where author = piAuthor (info p)

patchMessage :: (PatchInfoAnd p) x y -> BLU.ByteString
patchMessage p = BL.concat [ BLU.fromString (piName $ info p)
                           , case unlines . piLog $ info p of
                                "" -> BL.empty
                                plog -> BLU.fromString ('\n' : plog)]

dumpBits :: (BLU.ByteString -> TreeIO ()) -> [BLU.ByteString] -> TreeIO ()
dumpBits printer = printer . BL.intercalate "\n"

dumpFiles :: (BLU.ByteString -> TreeIO ()) -> [AnchoredPath] -> TreeIO ()
dumpFiles printer files = forM_ files $ \file -> do
  isfile <- fileExists file
  isdir <- directoryExists file
  when isfile $ do
    bits <- readFile file
    dumpBits printer [ BLU.fromString $
                        "M 100644 inline " ++ anchorPath "" file
                     , BLU.fromString $ "data " ++ show (BL.length bits)
                     , bits ]
  when isdir $ do tt <- gets tree -- ick
                  let subs = [ file `appendPath` n | (n, _) <-
                                  listImmediate $ fromJust $ findTree tt file ]
                  dumpFiles printer subs
  when (not isfile && not isdir) $
    printer $ BLU.fromString $ "D " ++ anchorPath "" file

dumpPatch :: (RepoPatch p) => (BLU.ByteString -> TreeIO ())
  -> ((PatchInfoAnd p) x y -> Int -> TreeIO ()) -> (PatchInfoAnd p) x y
  -> Int -> Int -> String -> TreeIO ()
dumpPatch printer mark p from current bName =
  do dumpBits printer [ BLC.pack $ "progress " ++ show current ++ ": "
                          ++ patchName p
                      , BLC.pack $ "commit refs/heads/" ++ bName ]
     mark p current
     dumpBits printer
        [ BLU.fromString $ "committer " ++ patchAuthor p ++ " " ++ patchDate p
        , BLU.fromString $ "data " ++ show (BL.length (patchMessage p) + 1)
        , patchMessage p ]
     when (current > 1) $
       dumpBits printer [ BLU.fromString $ "from :" ++ show from]
     dumpFiles printer $ map floatPath $ listTouchedFiles p

dumpTag :: (BLU.ByteString -> TreeIO ()) -> (PatchInfoAnd p) x y -> Int
  -> TreeIO ()
dumpTag printer p from = dumpBits printer
    [ BLU.fromString $ "progress TAG " ++ tagName p
    , BLU.fromString $ "tag refs/tags/" ++ tagName p -- FIXME is this valid?
    , BLU.fromString $ "from :" ++ show from
    , BLU.fromString $ "tagger " ++ patchAuthor p ++ " " ++ patchDate p
    , BLU.fromString $ "data " ++ show (BL.length (patchMessage p) - 4 + 1)
    , BL.drop 4 $ patchMessage p ]

updateBranches :: forall p cX cY . (RepoPatch p) => Int -> PatchInfoAnd p cX cY
  -> [(Bool, Branch p)] -> TreeIO [(Bool, Branch p)]
updateBranches current patch bs = mapM incrementIfEqual bs where
  -- Fst is whether we have found the fork yet.
  incrementIfEqual s@(True, _) = return s
  incrementIfEqual (_, o@(Branch _ _ (Sealed2 NilFL))) =
    return (True, o)
  incrementIfEqual (_, o@(Branch bName from (Sealed2 (bp :>: bps)))) =
    do -- TODO: justify the use of unsafeCompare...
       let isFork = not $ unsafeCompare patch bp
       -- If we've just hit a fork, stash the current pristine, so we can
       -- restore to it later, to export the divergent patches.
       when isFork $ stashPristine (Just from) Nothing
       return (isFork, if isFork then o else Branch bName current $ seal2 bps)

reset :: (BLU.ByteString -> TreeIO ()) -> Maybe Int -> String -> TreeIO ()
reset printer mbMark branch = do
    printer . BLC.pack $ "reset refs/heads/" ++ branch
    maybe (return ()) (\m -> printer . BLC.pack $ "from :" ++ show m) mbMark

exportBranches :: (RepoPatch p) => (BLU.ByteString -> TreeIO ()) -> [PatchInfo]
  -> (forall cX cY .PatchInfoAnd p cX cY -> Int -> TreeIO ()) -> Int -> Int
  -> [(Bool, Branch p)] -> TreeIO ()
exportBranches _ _ _ _ _ [] = return ()
exportBranches printer tags mark from current ((_, Branch name m ps) : bs) = do
    if from /= m
      then restorePristineFromMark "exportTemp" m >> reset printer (Just m) name
      else reset printer Nothing name
    dumpPatches printer tags mark m current (unsafeUnseal2 ps) name $
      -- Reset fork markers, so we can find branch common prefixes.
      map (\(_, b) -> (False, b)) bs

dumpPatches :: forall p x y . (RepoPatch p) => (BLU.ByteString -> TreeIO ())
  -> [PatchInfo] -> (forall cX cY .PatchInfoAnd p cX cY -> Int -> TreeIO ())
  -> Int -> Int -> FL (PatchInfoAnd p) x y -> String -> [(Bool, Branch p)]
  -> TreeIO ()
dumpPatches printer tags mark from current NilFL _ bs =
  exportBranches printer tags mark from current bs
dumpPatches printer tags mark from current (p:>:ps) bName bs = do
  bs' <- updateBranches current p bs
  apply p
  if inOrderTag tags p && current > 0
     then dumpTag printer p from
     else dumpPatch printer mark p from current bName
  let nextMark = next tags current p
      fromMark = if nextMark == current then from else current
  dumpPatches printer tags mark fromMark nextMark ps bName bs'

fp2bn :: FilePath -> String
fp2bn = takeFileName

fastExport :: (BLU.ByteString -> TreeIO ()) -> String -> [FilePath] -> Marks
  -> IO Marks
fastExport printer repodir bs marks = do
  branchPaths <- sort `fmap` mapM canonicalizePath bs
  withCurrentDirectory repodir $ withRepository [] $ RepoJob $ \repo ->
    fastExport' repo printer branchPaths marks

fastExport' :: forall p r u . (RepoPatch p) => Repository p r u r
  -> (BLU.ByteString -> TreeIO ()) -> [FilePath] -> Marks -> IO Marks
fastExport' repo printer bs marks = do
  patchset <- readRepo repo
  marksref <- newIORef marks
  let patches = newset2FL patchset
      tags = optimizedTags patchset
      mark :: (PatchInfoAnd p) x y -> Int -> TreeIO ()
      mark p n = do printer $ BLU.fromString $ "mark :" ++ show n
                    liftIO $ modifyIORef marksref $
                      \m -> addMark m n (patchHash p)

      checkOne :: Int -> PatchInfoAnd p x y -> TreeIO ()
      checkOne n p = do apply p
                        unless (inOrderTag tags p ||
                                (getMark marks n == Just (patchHash p))) $
                          die $ "FATAL: Marks do not correspond: expected " ++
                                show (getMark marks n) ++ ", got "
                                ++ BC.unpack (patchHash p)

      check :: Int -> FL (PatchInfoAnd p) x y
        -> TreeIO (Int, FlippedSeal (FL (PatchInfoAnd p)) y)
      check _ NilFL = return (1, flipSeal NilFL)
      check n allps@(p:>:ps)
        | n <= lastMark marks = checkOne n p >> check (next tags n p) ps
        | lastMark marks == 0 = return (1, flipSeal allps)
        | otherwise = return (n, flipSeal allps)

      -- |equalHead checks that the first patch of two distinct FLs is equal,
      -- giving a common branch-base that we can reset to in the output stream.
      equalHead :: (RepoPatch p) => FL (PatchInfoAnd p) Origin cX
        -> FL (PatchInfoAnd p) Origin cY -> Bool
      equalHead (x :>: _) (y :>: _) = isIsEq $ x =\/= y
      equalHead _     _ = False

      readBranchRepo :: FilePath
        -> IO (Branch p)
      readBranchRepo bPath = do
        bRepo <- identifyRepositoryFor repo bPath
        bPatches <- newset2FL `fmap` readRepo bRepo
        unless (equalHead patches bPatches) $
          die $ "ERROR: cannot export branch that has unequal initial patch: "
            ++ bPath
        let bName = fp2bn bPath
        return . Branch bName 1 $ seal2 bPatches

  them <- forM bs readBranchRepo

  ((n, FlippedSeal patches'), newTree) <-
    hashedTreeIO (check 1 patches) emptyTree "_darcs/pristine.hashed"
  hashedTreeIO (dumpPatches printer tags mark (n - 1) n patches' "master" $
    map (\ps -> (False, ps)) them) newTree "_darcs/pristine.hashed"
  readIORef marksref
 `finally` do
  current <- readHashedPristineRoot repo
  cleanHashdir (extractCache repo) HashedPristineDir $ catMaybes [current]

optimizedTags :: PatchSet p x y -> [PatchInfo]
optimizedTags (PatchSet _ ts) = go ts
  where go :: RL(Tagged t1) x y -> [PatchInfo]
        go (Tagged t _ _ :<: ts') = info t : go ts'
        go NilRL = []
