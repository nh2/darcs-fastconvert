{-# LANGUAGE GADTs, OverloadedStrings, Rank2Types, ScopedTypeVariables #-}
module Export( fastExport ) where

import Marks
import Utils
import Stash

import Codec.Compression.GZip ( compress )
import Control.Monad ( when, forM, forM_, unless )
import Control.Monad.Trans ( liftIO )
import Control.Monad.State.Strict( gets )
import Control.Exception( finally )
import Data.List ( sort, intercalate )
import Data.Maybe ( catMaybes, fromJust, isJust )
import Data.DateTime ( formatDateTime, fromClockTime )
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.ByteString.Lazy.UTF8 as BLU
import Data.ByteString.Base64 ( encode )
import Data.IORef ( newIORef, modifyIORef, readIORef )
import Prelude hiding ( readFile )
import Printer ( renderString )
import System.Directory ( canonicalizePath )
import System.Time ( toClockTime )

import Darcs.Patch.Effect ( Effect )
import Darcs.Patch.PatchInfoAnd ( PatchInfoAnd, info )
import Darcs.Repository ( Repository, RepoJob(..), readRepo, withRepository )
import Darcs.Repository.Cache ( HashedDir( HashedPristineDir ) )
import Darcs.Repository.HashedRepo ( readHashedPristineRoot )
import Darcs.Repository.HashedIO ( cleanHashdir )
import Darcs.Repository.Internal ( identifyRepositoryFor )
import Darcs.Repository.InternalTypes ( extractCache )
import Darcs.Patch ( effect, listTouchedFiles, apply, RepoPatch, showPatch )
import Darcs.Patch.Info ( isTag, PatchInfo, piAuthor, piName, piLog, piDate )
import Darcs.Patch.Prim.Class ( PrimOf, primIsTokReplace, PrimClassify(..) )
import Darcs.Patch.Set ( PatchSet(..), Tagged(..), newset2FL )
import Darcs.Witnesses.Eq ( MyEq(..), unsafeCompare )
import Darcs.Witnesses.Ordered ( FL(..), RL(..), nullFL, mapFL )
import Darcs.Witnesses.Sealed ( seal2, Sealed2(..), unsafeUnseal2 )
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

generateInfoIgnores :: forall p x y . (RepoPatch p) => PatchInfoAnd p x y
  -> BL.ByteString
generateInfoIgnores p =
  packIgnores . takeStartEnd . mapFL renderReplace $ effect p
  where renderReplace :: (PrimOf p) cX cY -> Maybe String
        renderReplace pr | primIsTokReplace pr =
          Just . renderString . showPatch $ pr
        renderReplace _ = Nothing
        -- We can only handle replaces at the start and end, since we cannot
        -- represent the intermediate states, which we would need to recover
        -- the prims that were originally applied.
        takeStartEnd ps = catMaybes $ starts ++ ends where
          (startJusts, ps') = span isJust ps
          (endJusts, _) = span isJust $ reverse ps'
          prefixer pref = map (fmap (pref ++))
          starts = prefixer "S " startJusts
          ends = prefixer "E " $ reverse endJusts
        packIgnores :: [String] -> BL.ByteString
        packIgnores [] = BL.empty
        packIgnores is = BLC.fromChunks [prefix `BC.append` base64ps] where
          prefix = BC.pack "\ndarcs-patches: "
          gzipped = compress . BLC.pack $ intercalate "\n" is
          base64ps = encode . BC.concat . BLC.toChunks $ gzipped

dumpPatch :: (RepoPatch p) => (BLU.ByteString -> TreeIO ())
  -> ((PatchInfoAnd p) x y -> Int -> String -> TreeIO ())
  -> (PatchInfoAnd p) x y -> Int -> Int -> String -> TreeIO ()
dumpPatch printer doMark p from current bName =
  do dumpBits printer [ BLC.pack $ "progress " ++ show current ++ ": "
                          ++ patchName p
                      , BLC.pack $ "commit refs/heads/" ++ bName ]
     let message = patchMessage p `BL.append` generateInfoIgnores p
     doMark p current bName
     dumpBits printer
        [ BLU.fromString $ "committer " ++ patchAuthor p ++ " " ++ patchDate p
        , BLU.fromString $ "data " ++ show (BL.length message + 1)
        , message ]
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
  -> Marks
  -> (forall cX cY .PatchInfoAnd p cX cY -> Int -> String -> TreeIO ()) -> Int
  -> Int -> [(Bool, Branch p)] -> TreeIO ()
exportBranches _ _ _ _ _ _ [] = return ()
exportBranches printer tags existingMarks doMark from current
  ((_, Branch name m ps) : bs) = do
    when (from /= m) $ restorePristineFromMark "exportTemp" m
    reset printer (Just m) name
    dumpPatches printer tags existingMarks doMark m current (unsafeUnseal2 ps)
      name $
      -- Reset fork markers, so we can find branch common prefixes.
      map (\(_, b) -> (False, b)) bs

dumpPatches :: forall p x y . (RepoPatch p) => (BLU.ByteString -> TreeIO ())
  -> [PatchInfo] -> Marks
  -> (forall cX cY .PatchInfoAnd p cX cY -> Int -> String -> TreeIO ()) -> Int
  -> Int -> FL (PatchInfoAnd p) x y -> String -> [(Bool, Branch p)]
  -> TreeIO ()
dumpPatches printer tags existingMarks doMark from current NilFL _ bs =
  exportBranches printer tags existingMarks doMark from current bs
dumpPatches printer tags existingMarks doMark from current (p:>:ps) bName bs =
  do
  bs' <- updateBranches current p bs
  apply p
  -- Only export if we haven't already done so, otherwise checking that the
  -- incremental marks provided are correct for the repos being exported.
  if current > lastMark existingMarks
    then if inOrderTag tags p
           then dumpTag printer p from
           else dumpPatch printer doMark p from current bName
    else unless (inOrderTag tags p ||
          (fst `fmap` getMark existingMarks current == Just (patchHash p))) $
           die . unwords $ ["Marks do not correspond: expected"
                           , show (getMark existingMarks current), ", got "
                           , BC.unpack (patchHash p)]
  let nextMark = next tags current p
      fromMark = if nextMark == current then from else current
  dumpPatches printer tags existingMarks doMark fromMark nextMark ps bName bs'

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
      doMark :: (PatchInfoAnd p) x y -> Int -> String -> TreeIO ()
      doMark p n b = do printer $ BLU.fromString $ "mark :" ++ show n
                        let bn =
                             pb2bn . parseBranch . BC.pack $ "refs/heads/" ++ b
                        liftIO $ modifyIORef marksref $
                          \m -> addMark m n (patchHash p, bn)

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
  hashedTreeIO (dumpPatches printer tags marks doMark 0 1 patches "master" $
    map (\ps -> (False, ps)) them) emptyTree "_darcs/pristine.hashed"
  readIORef marksref
 `finally` do
  current <- readHashedPristineRoot repo
  cleanHashdir (extractCache repo) HashedPristineDir $ catMaybes [current]

optimizedTags :: PatchSet p x y -> [PatchInfo]
optimizedTags (PatchSet _ ts) = go ts
  where go :: RL(Tagged t1) x y -> [PatchInfo]
        go (Tagged t _ _ :<: ts') = info t : go ts'
        go NilRL = []
