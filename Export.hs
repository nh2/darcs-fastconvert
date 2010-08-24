{-# LANGUAGE GADTs #-}
module Export( fastExport ) where

import Prelude hiding ( readFile )

import Data.Maybe ( catMaybes, fromJust )
import Data.DateTime ( formatDateTime, fromClockTime )
import qualified Data.ByteString.Lazy.Char8 as BL

import Control.Monad ( when, forM_ )
import Control.Monad.Trans ( liftIO )
import Control.Monad.State.Strict( gets )
import Control.Exception( finally )

import System.Time ( toClockTime )

import Darcs.Hopefully ( PatchInfoAnd, info )
import Darcs.Repository ( ($-), readRepo, withRepository )
import Darcs.Repository.Cache ( HashedDir( HashedPristineDir ) )
import Darcs.Repository.HashedRepo ( readHashedPristineRoot )
import Darcs.Repository.HashedIO ( cleanHashdir )
import Darcs.Repository.InternalTypes ( extractCache )
import Darcs.Patch ( effect, listTouchedFiles, apply, RepoPatch )
import Darcs.Witnesses.Ordered ( FL(..), RL(..), lengthFL, nullFL )
import Darcs.Patch.Info ( isTag, PatchInfo, piAuthor, piName, piLog, piDate )
import Darcs.Patch.Set ( PatchSet(..), Tagged(..), newset2FL )
import Darcs.Utils ( withCurrentDirectory )

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
  let total = show (lengthFL patches)
      patches = newset2FL patchset
      tags = optimizedTags patchset
      dumpfiles :: [AnchoredPath] -> TreeIO ()
      dumpfiles files = forM_ files $ \file -> do
        isfile <- fileExists file
        isdir <- directoryExists file
        when isfile $ do bits <- readFile file
                         liftIO $ putStrLn $ "M 100644 inline " ++ anchorPath "" file
                         liftIO $ putStrLn $ "data " ++ show (BL.length bits)
                         liftIO $ putStrLn (BL.unpack bits)
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
      message p = (piName $ info p) ++ case (unlines . piLog $ info p) of
        "" -> ""
        plog -> "\n" ++ plog
      realTag p = isTag (info p) && info p `elem` tags && nullFL (effect p)
      author p = case span (/='<') $ piAuthor (info p) of
                          (n, "") -> n ++ " <unknown>"
                          (n, rest) -> case span (/='>') $ tail rest of
                            (email, _) -> n ++ "<" ++ email ++ ">"
      dumpPatch p n = liftIO $ putStr $ unlines
          [ "progress " ++ show n ++ " / " ++ total ++ ": " ++ name p
          , "commit refs/heads/master"
          , "mark :" ++ show n -- mark the stream
          , "committer " ++ author p ++ " " ++ date p
          , "data " ++ show (length $ message p)
          , message p ]
      dumpTag p n = liftIO $ putStr $ unlines
          [ "progress TAG " ++ tagname p
          , "tag " ++ tagname p -- FIXME is this valid?
          -- , "mark :" ++ show n -- no marks for tags?
          , "from :" ++ show (n - 1) -- the previous mark
          , "tagger " ++ author p ++ " " ++ date p
          , "data " ++ show (length (message p) - 4)
          , drop 4 $ message p ]
      dump :: (RepoPatch p) => Int -> FL (PatchInfoAnd p) cX cY -> TreeIO ()
      dump _ NilFL = liftIO $ putStrLn "progress (patches converted)"
      dump n (p:>:ps) = do
        apply p
        if realTag p && n > 0
           then dumpTag p n
           else do dumpPatch p n
                   dumpfiles $ map floatPath $ listTouchedFiles p
        dump (n + 1) ps
  putStrLn "reset refs/heads/master"
  hashedTreeIO (dump 1 patches) emptyTree "_darcs/pristine.hashed"
  return ()
 `finally` do
  putStrLn "progress (cleaning up)"
  current <- readHashedPristineRoot repo
  cleanHashdir (extractCache repo) HashedPristineDir $ catMaybes [current]
  putStrLn "progress done"
  return ()

optimizedTags :: PatchSet p cS cX -> [PatchInfo]
optimizedTags (PatchSet _ ts) = go ts
  where go :: RL(Tagged t1) cT cY -> [PatchInfo]
        go (Tagged t _ _ :<: ts') = info t : go ts'
        go NilRL = []

