{-# LANGUAGE GADTs #-}
module Export( fastExport ) where

import Prelude hiding ( readFile )
import System.Directory ( setCurrentDirectory, doesDirectoryExist, doesFileExist,
                   createDirectory, createDirectoryIfMissing )
import Workaround ( getCurrentDirectory )
import Control.Monad ( when, forM_, unless )
import Control.Applicative ( (<|>) )
import Control.Monad.Trans ( liftIO )
import Control.Monad.State.Strict( gets, modify )
import Control.Exception( finally )
import Data.Maybe ( catMaybes, fromJust )
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as BL

import Darcs.Hopefully ( PatchInfoAnd, n2pia, info, hopefully )
import Darcs.Commands ( DarcsCommand(..), nodefaults, putInfo, putVerbose )
import Darcs.Flags( Compression( .. ) )
import Darcs.Repository ( Repository, withRepoLock, ($-), withRepositoryDirectory, readRepo,
                          readTentativeRepo,
                          createRepository, invalidateIndex,
                          optimizeInventory,
                          tentativelyMergePatches, patchSetToPatches,
                          createPristineDirectoryTree,
                          revertRepositoryChanges, finalizeRepositoryChanges,
                          applyToWorking, setScriptsExecutable, withRepository )
import Darcs.Repository.Cache ( HashedDir( HashedPristineDir ), Cache(..) )
import Darcs.Repository.HashedRepo ( readHashedPristineRoot, addToTentativeInventory )
import Darcs.Repository.HashedIO ( cleanHashdir )
import Darcs.Repository.InternalTypes ( extractCache )
import Darcs.Repository.Prefs( FileType(..) )
import Darcs.Global ( darcsdir )
import Darcs.Patch ( RealPatch, Patch, Named, showPatch, patch2patchinfo, fromPrims, infopatch,
                     modernizePatch,
                     adddeps, getdeps, effect, flattenFL, isMerger, patchcontents,
                     listTouchedFiles, apply, RepoPatch, identity )
import Darcs.Patch.Depends ( getTagsRight )
import Darcs.Patch.Prim ( canonizeFL, sortCoalesceFL, Prim )
import Darcs.Witnesses.Ordered ( FL(..), RL(..), EqCheck(..), (=/\=), bunchFL, mapFL, mapFL_FL,
                                 concatFL, mapRL, lengthFL, nullFL )
import Darcs.Patch.Info ( piRename, piTag, isTag, PatchInfo, piAuthor, piName, piLog, piDate
                        , patchinfo )
import Darcs.Patch.Commute ( publicUnravel )
import Darcs.Patch.Real ( mergeUnravelled )
import Darcs.Patch.Set ( PatchSet(..), Tagged(..), newset2RL, newset2FL )
import Darcs.RepoPath ( ioAbsoluteOrRemote, toPath )
import Darcs.Repository.Format(identifyRepoFormat, formatHas, RepoProperty(Darcs2))
import Darcs.Repository.Motd ( showMotd )
import Darcs.Utils ( clarifyErrors, askUser, catchall, withCurrentDirectory )
import Darcs.ProgressPatches ( progressFL )
import Darcs.Witnesses.Sealed ( FlippedSeal(..), Sealed(..), unFreeLeft, unseal )
import Printer ( text, ($$) )
import Darcs.ColorPrinter ( traceDoc )
import Darcs.Lock ( writeBinFile )
import Darcs.External
import System.FilePath.Posix
import System.Time ( toClockTime )
import Data.DateTime ( formatDateTime, parseDateTime, fromClockTime, startOfTime )
import System.IO ( stdin )

import Storage.Hashed.Monad hiding ( createDirectory, exists )
import qualified Storage.Hashed.Monad as TM
import qualified Storage.Hashed.Tree as T
import Darcs.IO()
import Storage.Hashed.Darcs
import Storage.Hashed.Hash( encodeBase16, sha256, Hash(..) )
import Storage.Hashed.Tree( emptyTree, listImmediate, findTree, Tree
                          , treeHash, readBlob, TreeItem(..) )
import Storage.Hashed.AnchoredPath( floatPath, AnchoredPath(..), Name(..), anchorPath, appendPath )
import Darcs.Diff( treeDiff )

import qualified Data.Attoparsec.Char8 as A
import Data.Attoparsec.Char8( (<?>) )

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
                          (name, "") -> name ++ " <unknown>"
                          (name, rest) -> case span (/='>') $ tail rest of
                            (email, _) -> name ++ "<" ++ email ++ ">"
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
        go (Tagged t _ _ :<: ts) = info t : go ts
        go NilRL = []

