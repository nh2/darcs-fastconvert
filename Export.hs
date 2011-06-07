{-# LANGUAGE GADTs, OverloadedStrings, Rank2Types #-}
module Export( fastExport ) where

import Prelude hiding ( readFile )

import Marks

import Data.Maybe ( catMaybes, fromJust )
import Data.DateTime ( formatDateTime, fromClockTime )
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.ByteString.Lazy.UTF8 as BLU
import Data.IORef ( newIORef, modifyIORef, readIORef )

import Control.Monad ( when, forM_, unless )
import Control.Monad.Trans ( liftIO )
import Control.Monad.State.Strict( gets )
import Control.Exception( finally )

import System.Time ( toClockTime )

import Darcs.Patch.PatchInfoAnd ( PatchInfoAnd, info )
import Darcs.Patch.Effect ( Effect )
import Darcs.Repository ( Repository, RepoJob(..), readRepo, withRepository )
import Darcs.Repository.Cache ( HashedDir( HashedPristineDir ) )
import Darcs.Repository.HashedRepo ( readHashedPristineRoot )
import Darcs.Repository.HashedIO ( cleanHashdir )
import Darcs.Repository.InternalTypes ( extractCache )
import Darcs.Patch ( effect, listTouchedFiles, apply, RepoPatch )
import Darcs.Witnesses.Ordered ( FL(..), RL(..), nullFL )
import Darcs.Witnesses.Sealed ( flipSeal, FlippedSeal(..) )
import Darcs.Patch.Info ( isTag, PatchInfo, piAuthor, piName, piLog, piDate )
import Darcs.Patch.Set ( PatchSet(..), Tagged(..), newset2FL )
import Darcs.Utils ( withCurrentDirectory )
import Utils

import Storage.Hashed.Monad hiding ( createDirectory, exists )
import Storage.Hashed.Darcs
import Storage.Hashed.Tree( emptyTree, listImmediate, findTree )
import Storage.Hashed.AnchoredPath( anchorPath, appendPath, floatPath
                                  , AnchoredPath  )

inOrderTag :: (Effect p) => [PatchInfo] -> (PatchInfoAnd p) x y -> Bool
inOrderTag tags p = isTag (info p) && info p `elem` tags && nullFL (effect p)

next :: (Effect p) => [PatchInfo] -> Int -> (PatchInfoAnd p) x y -> Int
next tags n p = if inOrderTag tags p then n else n + 1

tagName :: (PatchInfoAnd p) x y -> String
tagName = map (cleanup " .") . drop 4 . patchName -- FIXME many more chars are probably illegal
  where cleanup bad x | x `elem` bad = '_'
                      | otherwise = x

patchName :: (PatchInfoAnd p) x y -> String
patchName = piName . info

patchDate :: (PatchInfoAnd p) x y -> String
patchDate = formatDateTime "%s +0000" . fromClockTime . toClockTime . piDate . info

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

dumpBits :: [BLU.ByteString] -> TreeIO ()
dumpBits = liftIO . BL.putStrLn . BL.intercalate "\n"

dumpFiles :: [AnchoredPath] -> TreeIO ()
dumpFiles files = forM_ files $ \file -> do
  isfile <- fileExists file
  isdir <- directoryExists file
  when isfile $ do bits <- readFile file
                   dumpBits [ BLU.fromString $ "M 100644 inline " ++ anchorPath "" file
                            , BLU.fromString $ "data " ++ show (BL.length bits)
                            , bits ]
  when isdir $ do tt <- gets tree -- ick
                  let subs = [ file `appendPath` n | (n, _) <-
                                  listImmediate $ fromJust $ findTree tt file ]
                  dumpFiles subs
  when (not isfile && not isdir) $ liftIO $ putStrLn $ "D " ++ anchorPath "" file

dumpPatch :: ((PatchInfoAnd p) x y -> Int -> TreeIO ()) -> (PatchInfoAnd p) x y -> Int -> TreeIO ()
dumpPatch mark p n =
  do dumpBits [ BLC.pack $ "progress " ++ show n ++ ": " ++ patchName p
              , "commit refs/heads/master" ]
     mark p n
     dumpBits [ BLU.fromString $ "committer " ++ patchAuthor p ++ " " ++ patchDate p
              , BLU.fromString $ "data " ++ show (BL.length $ patchMessage p)
              , patchMessage p ]
     when (n > 1) $ dumpBits [ BLU.fromString $ "from :" ++ show (n - 1) ]

dumpTag :: (PatchInfoAnd p) x y -> Int -> TreeIO ()
dumpTag p n =
  dumpBits [ BLU.fromString $ "progress TAG " ++ tagName p
           , BLU.fromString $ "tag " ++ tagName p -- FIXME is this valid?
           , BLU.fromString $ "from :" ++ show (n - 1) -- the previous mark
           , BLU.fromString $ "tagger " ++ patchAuthor p ++ " " ++ patchDate p
           , BLU.fromString $ "data " ++ show (BL.length (patchMessage p) - 4)
           , BL.drop 4 $ patchMessage p ]

dumpPatches :: (RepoPatch p) => [PatchInfo] -> (forall cX cY .PatchInfoAnd p cX cY -> Int -> TreeIO ())
                 -> Int -> FL (PatchInfoAnd p) x y -> TreeIO ()
dumpPatches _ _ _ NilFL = return ()
dumpPatches tags mark n (p:>:ps) = do
  apply p
  if inOrderTag tags p && n > 0
     then dumpTag p n
     else do dumpPatch mark p n
             dumpFiles $ map floatPath $ listTouchedFiles p
  dumpPatches tags mark (next tags n p) ps

fastExport :: String -> Marks -> IO Marks
fastExport repodir marks =
  withCurrentDirectory repodir $ withRepository [] $ RepoJob $ \repo -> fastExport' repo marks

fastExport' :: (RepoPatch p) => Repository p r u r -> Marks -> IO Marks
fastExport' repo marks = do
  patchset <- readRepo repo
  marksref <- newIORef marks
  let patches = newset2FL patchset
      tags = optimizedTags patchset
      mark :: (PatchInfoAnd p) x y -> Int -> TreeIO ()
      mark p n = liftIO $ do putStrLn $ "mark :" ++ show n
                             modifyIORef marksref $ \m -> addMark m n (patchHash p)
      checkOne :: (RepoPatch p) => Int -> PatchInfoAnd p x y -> TreeIO ()
      checkOne n p = do apply p
                        unless (inOrderTag tags p ||
                                (getMark marks n == Just (patchHash p))) $
                          die $ "FATAL: Marks do not correspond: expected " ++
                                show (getMark marks n) ++ ", got " ++ BSC.unpack (patchHash p)

      -- |check drops any patches that have already been exported (as
      -- determined by the mark-number passed in), first checking they apply
      -- and their hash correspond to the expected mark. We return a
      -- FlippedSeal FL, since we may drop some patches from the start of the
      -- passed-in FL (we also may not drop any), so the caller cannot know the
      -- returned initial context.
      check :: (RepoPatch p) => Int -> FL (PatchInfoAnd p) x y -> TreeIO (Int, FlippedSeal (FL (PatchInfoAnd p)) y)
      check _ NilFL = return (1, flipSeal NilFL)
      check n allps@(p:>:ps)
        | n <= lastMark marks = checkOne n p >> check (next tags n p) ps
        | lastMark marks == 0 = return (1, flipSeal allps)
        | otherwise = return (n, flipSeal allps)

  ((n, FlippedSeal patches'), newTree) <- hashedTreeIO (check 1 patches) emptyTree "_darcs/pristine.hashed"
  hashedTreeIO (dumpPatches tags mark n patches') newTree "_darcs/pristine.hashed"
  readIORef marksref
 `finally` do
  current <- readHashedPristineRoot repo
  cleanHashdir (extractCache repo) HashedPristineDir $ catMaybes [current]

optimizedTags :: PatchSet p x y -> [PatchInfo]
optimizedTags (PatchSet _ ts) = go ts
  where go :: RL(Tagged t1) x y -> [PatchInfo]
        go (Tagged t _ _ :<: ts') = info t : go ts'
        go NilRL = []

