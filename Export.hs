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
      dumpfiles :: [AnchoredPath] -> TreeIO ()
      dumpfiles files = forM_ files $ \file -> do
        isfile <- fileExists file
        isdir <- directoryExists file
        when isfile $ do bits <- readFile file
                         dumpBits [ BLU.fromString $ "M 100644 inline " ++ anchorPath "" file
                                  , BLU.fromString $ "data " ++ show (BL.length bits)
                                  , bits ]
        when isdir $ do tt <- gets tree -- ick
                        let subs = [ file `appendPath` n | (n, _) <-
                                        listImmediate $ fromJust $ findTree tt file ]
                        dumpfiles subs
        when (not isfile && not isdir) $ liftIO $ putStrLn $ "D " ++ anchorPath "" file

      name = piName . info
      tagname = map (cleanup " .") . drop 4 . name -- FIXME many more chars are probably illegal
        where cleanup bad x | x `elem` bad = '_'
                            | otherwise = x
      date = formatDateTime "%s +0000" . fromClockTime . toClockTime . piDate . info
      message p = BL.concat [ BLU.fromString (piName $ info p)
                            , case (unlines . piLog $ info p) of
                                 "" -> BL.empty
                                 plog -> BLU.fromString ("\n" ++ plog)]
      realTag p = isTag (info p) && info p `elem` tags && nullFL (effect p)
      author p = case span (/='<') $ piAuthor (info p) of
                          (n, "") -> n ++ " <unknown>"
                          (n, rest) -> case span (/='>') $ tail rest of
                            (email, _) -> n ++ "<" ++ email ++ ">"
      hash p = makeFilename (info p)
      dumpBits = liftIO . BL.putStrLn . BL.intercalate "\n"
      mark p n = do liftIO $ putStrLn $ "mark :" ++ show n -- mark the stream
                    case markfile of
                      Nothing -> return ()
                      Just h -> liftIO $ hPutStrLn h $ show n ++ ": " ++ hash p
      dumpPatch p n =
        do dumpBits [ BLC.pack $ "progress " ++ show n ++ " / " ++ total ++ ": " ++ name p
                    , "commit refs/heads/master" ]
           mark p n
           dumpBits [ BLU.fromString $ "committer " ++ author p ++ " " ++ date p
                    , BLU.fromString $ "data " ++ show (BL.length $ message p)
                    , message p ]
           when (n > 1) $ dumpBits [ BLU.fromString $ "from :" ++ show (n - 1) ]
      dumpTag p n =
        dumpBits [ BLU.fromString $ "progress TAG " ++ tagname p
                 , BLU.fromString $ "tag " ++ tagname p -- FIXME is this valid?
                 , BLU.fromString $ "from :" ++ show (n - 1) -- the previous mark
                 , BLU.fromString $ "tagger " ++ author p ++ " " ++ date p
                 , BLU.fromString $ "data " ++ show (BL.length (message p) - 4)
                 , BL.drop 4 $ message p ]
      dump :: (RepoPatch p) => Int -> FL (PatchInfoAnd p) -> TreeIO ()
      dump _ NilFL = liftIO $ putStrLn "progress (patches converted)"
      dump n (p:>:ps) = do
        apply [] p
        if realTag p && n > 0
           then dumpTag p n
           else do dumpPatch p n
                   dumpfiles $ map floatPath $ listTouchedFiles p
        dump (next n p) ps
      next n p = if realTag p then n else n + 1
      die str = liftIO (hPutStrLn stderr str >> exitWith (ExitFailure 1))
      checkOne n p = do apply [] p
                        unless (realTag p || (getMark marks n == Just (BSC.pack $ hash p))) $
                                die $ "FATAL: Marks do not correspond: expected " ++
                                      (show $ getMark marks n) ++ ", got " ++ hash p
                        liftIO $ hPutStrLn stderr $ "OK: " ++ show n
      check _ NilFL = return (1, NilFL)
      check n allps@(p:>:ps)
        | n <= lastMark marks = do checkOne n p >> check (next n p) ps
        | n > lastMark marks = return (n, allps)
        | lastMark marks == 0 = return (1, allps)

  ((n, patches'), tree) <- hashedTreeIO (check 1 patches) emptyTree "_darcs/pristine.hashed"
  -- putStrLn "reset refs/heads/master"
  hashedTreeIO (dump n patches') tree "_darcs/pristine.hashed"
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

