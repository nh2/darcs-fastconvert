{-# LANGUAGE GADTs, OverloadedStrings #-}
module Export( fastExport ) where

import Prelude hiding ( readFile )

import Marks

import Data.Maybe ( catMaybes, fromJust )
import Data.DateTime ( formatDateTime, fromClockTime )
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.ByteString.Lazy.UTF8 as BLU

import Control.Monad ( when, forM_, unless )
import Control.Monad.Trans ( liftIO )
import Control.Monad.State.Strict( gets )
import Control.Exception( finally )

import System.Time ( toClockTime )
import System.IO ( hPutStrLn, openFile, IOMode(..), stderr )
import System.Exit

import Darcs.Hopefully ( PatchInfoAnd, info )
import Darcs.Repository ( ($-), readRepo, withRepository )
import Darcs.Repository.Cache ( HashedDir( HashedPristineDir ) )
import Darcs.Repository.HashedRepo ( readHashedPristineRoot )
import Darcs.Repository.HashedIO ( cleanHashdir )
import Darcs.Repository.InternalTypes ( extractCache )
import Darcs.Patch ( effect, listTouchedFiles, apply, RepoPatch )
import Darcs.Witnesses.Ordered ( FL(..), RL(..), lengthFL, nullFL )
import Darcs.Patch.Info ( isTag, PatchInfo, piAuthor, piName, piLog, piDate, makeFilename )
import Darcs.Patch.Set ( PatchSet(..), Tagged(..), newset2FL )
import Darcs.Utils ( withCurrentDirectory )
import System.Directory( removeFile )

import Storage.Hashed.Monad hiding ( createDirectory, exists )
import Storage.Hashed.Darcs
import Storage.Hashed.Tree( emptyTree, listImmediate, findTree )
import Storage.Hashed.AnchoredPath( anchorPath, appendPath, floatPath
                                  , AnchoredPath  )

inOrderTag tags p = isTag (info p) && info p `elem` tags && nullFL (effect p)
next tags n p = if inOrderTag tags p then n else n + 1

tagName = map (cleanup " .") . drop 4 . patchName -- FIXME many more chars are probably illegal
  where cleanup bad x | x `elem` bad = '_'
                      | otherwise = x

patchName = piName . info
patchDate = formatDateTime "%s +0000" . fromClockTime . toClockTime . piDate . info

patchAuthor p = case span (/='<') $ piAuthor (info p) of
  (n, "") -> n ++ " <unknown>"
  (n, rest) -> case span (/='>') $ tail rest of
    (email, _) -> n ++ "<" ++ email ++ ">"

patchHash p = makeFilename (info p)

patchMessage p = BL.concat [ BLU.fromString (piName $ info p)
                           , case (unlines . piLog $ info p) of
                                "" -> BL.empty
                                plog -> BLU.fromString ("\n" ++ plog)]

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

dumpPatch mark p n =
  do dumpBits [ BLC.pack $ "progress " ++ show n ++ ": " ++ patchName p
              , "commit refs/heads/master" ]
     mark p n
     dumpBits [ BLU.fromString $ "committer " ++ patchAuthor p ++ " " ++ patchDate p
              , BLU.fromString $ "data " ++ show (BL.length $ patchMessage p)
              , patchMessage p ]
     when (n > 1) $ dumpBits [ BLU.fromString $ "from :" ++ show (n - 1) ]

dumpTag p n =
  dumpBits [ BLU.fromString $ "progress TAG " ++ tagName p
           , BLU.fromString $ "tag " ++ tagName p -- FIXME is this valid?
           , BLU.fromString $ "from :" ++ show (n - 1) -- the previous mark
           , BLU.fromString $ "tagger " ++ patchAuthor p ++ " " ++ patchDate p
           , BLU.fromString $ "data " ++ show (BL.length (patchMessage p) - 4)
           , BL.drop 4 $ patchMessage p ]

dumpPatches :: (RepoPatch p) => [PatchInfo] -> (PatchInfoAnd p -> Int -> TreeIO ())
               -> Int -> FL (PatchInfoAnd p) -> TreeIO ()
dumpPatches _ _ _ NilFL = liftIO $ putStrLn "progress (patches converted)"
dumpPatches tags mark n (p:>:ps) = do
  apply [] p
  if inOrderTag tags p && n > 0
     then dumpTag p n
     else do dumpPatch mark p n
             dumpFiles $ map floatPath $ listTouchedFiles p
  dumpPatches tags mark (next tags n p) ps

fastExport :: String -> IO ()
fastExport repodir = withCurrentDirectory repodir $
                     withRepository [] $- \repo -> do
  putStrLn "progress (reading repository)"
  patchset <- readRepo repo
  marks <- readMarks ".darcs-marks"
  markfile <- Just `fmap` openFile ".darcs-marks" AppendMode
  let total = show (lengthFL patches)
      patches = newset2FL patchset
      tags = optimizedTags patchset
      mark p n = do liftIO $ putStrLn $ "mark :" ++ show n -- mark the stream
                    case markfile of
                      Nothing -> return ()
                      Just h -> liftIO $ hPutStrLn h $ show n ++ ": " ++ patchHash p
      die str = liftIO (hPutStrLn stderr str >> exitWith (ExitFailure 1))
      checkOne n p = do apply [] p
                        unless (inOrderTag tags p || (getMark marks n == Just (BSC.pack $ patchHash p))) $
                                die $ "FATAL: Marks do not correspond: expected " ++
                                      (show $ getMark marks n) ++ ", got " ++ patchHash p
                        liftIO $ hPutStrLn stderr $ "OK: " ++ show n
      check _ NilFL = return (1, NilFL)
      check n allps@(p:>:ps)
        | n <= lastMark marks = do checkOne n p >> check (next tags n p) ps
        | n > lastMark marks = return (n, allps)
        | lastMark marks == 0 = return (1, allps)

  ((n, patches'), tree) <- hashedTreeIO (check 1 patches) emptyTree "_darcs/pristine.hashed"
  hashedTreeIO (dumpPatches tags mark n patches') tree "_darcs/pristine.hashed"
  return ()
 `finally` do
  putStrLn "progress (cleaning up)"
  current <- readHashedPristineRoot repo
  cleanHashdir (extractCache repo) HashedPristineDir $ catMaybes [current]
  putStrLn "progress done"
  return ()

optimizedTags :: PatchSet p -> [PatchInfo]
optimizedTags (PatchSet _ ts) = go ts
  where go :: RL(Tagged t1) -> [PatchInfo]
        go (Tagged t _ _ :<: ts') = info t : go ts'
        go NilRL = []

