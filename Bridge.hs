{-# LANGUAGE DeriveDataTypeable #-}
module Bridge( createBridge, syncBridge, VCSType(..), listBranches, trackBranch
             , untrackBranch, RepoBranch(..) ) where

import Export ( fastExport )
import Import ( fastImportIncremental )
import Marks ( handleCmdMarks )
import Stash ( topLevelBranchDir, parseBranch )
import Utils ( die, fp2bn, equalHead )

import Control.Monad ( when, unless, forM, foldM, forM_ )
import Control.Monad.Error ( join )
import Control.Monad.IO.Class
import Control.Monad.Trans.Error
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.ByteString.Char8 as BC
import Data.ConfigFile ( emptyCP, set, to_string, readfile, get, sections
                       , setshow, has_section, add_section )
import Data.Data ( Data, Typeable )
import Data.List ( isPrefixOf, partition, sortBy )
import Data.Ord ( comparing )
import Data.Either.Utils ( forceEither )
import System.Directory ( createDirectory, canonicalizePath
                        , setCurrentDirectory, getCurrentDirectory, copyFile
                        , removeFile, renameFile, doesDirectoryExist )
import System.Environment ( getEnvironment )
import System.Exit ( exitFailure, exitSuccess, ExitCode(ExitSuccess) )
import System.FilePath ( (</>), takeDirectory, joinPath, splitFileName
                       , dropFileName, takeFileName )
import System.IO ( withFile, IOMode(ReadMode,WriteMode) )
import System.PosixCompat.Files
import System.Posix.Types ( FileMode )
import System.Process ( runCommand, runProcess, ProcessHandle, waitForProcess
                      , readProcess )

import Darcs.Commands ( commandCommand )
import qualified Darcs.Commands.Get as DCG
import Darcs.Flags ( DarcsFlag(WorkRepoDir, UseFormat2, Quiet) )
import Darcs.Lock ( withLockCanFail )
import Darcs.Patch.Set ( newset2FL )
import Darcs.Repository ( RepoJob(..), readRepo, withRepository
                        , amNotInRepository, createRepository )
import Darcs.Repository.Internal ( identifyRepositoryFor )
import Darcs.Repository.Prefs ( addToPreflist )
import Darcs.Utils ( withCurrentDirectory )

data VCSType = BareGit
             | Git
             | Darcs
             deriving (Eq, Data, Typeable)

instance Show VCSType where
    show Darcs = "darcs"
    show _ = "git"

data BridgeConfig = BridgeConfig { darcs_path :: FilePath
                                 , git_path   :: FilePath
                                 , cloned     :: Bool
                                 , branches   :: [(String, FilePath)]
                                 } deriving Show

-- New branches are either a Darcs repo path, or a Git branch name.
data RepoBranch = DarcsBranch FilePath
                | GitBranch String

otherVCS :: VCSType -> VCSType
otherVCS Darcs = Git
otherVCS _     = Darcs

data Converter = Converter { exporter :: FilePath -> IO ()
                           , importer :: FilePath -> IO ()
                           , markUpdater :: FilePath -> IO ()
                           , sourceExportMarks :: FilePath
                           , targetExportMarks :: FilePath
                           , sourceImportMarks :: FilePath
                           }

bridgeDirName :: String
bridgeDirName = ".darcs_bridge"

darcsExportMarksName, darcsImportMarksName :: String
darcsExportMarksName = "darcs_export_marks"
darcsImportMarksName = "darcs_import_marks"
gitExportMarksName, gitImportMarksName :: String
gitExportMarksName = "git_export_marks"
gitImportMarksName = "git_import_marks"

-- |findBridgeDir will return the filepath of the bridge dir of a given
-- directory. If foo is given and foo/.darcs_bridge exists, then the
-- canonicalized version of foo/.darcs_bridge will be returned, otherwise only
-- .darcs_bridge will be accepted, canonicalized and returned.
findBridgeDir :: FilePath -> IO (Maybe FilePath)
findBridgeDir dir = do
  fullPath <- canonicalizePath dir `catch` \_ -> die $ "Invalid path: " ++ dir
  if takeFileName fullPath == bridgeDirName
    then return $ Just fullPath
    else do
      let subDirPath = fullPath </> bridgeDirName
      isSubDirBridgeDir <- doesDirectoryExist subDirPath
      if isSubDirBridgeDir
        then return $ Just subDirPath
        else return Nothing

-- |createBridge sets up a Darcs bridge. If shouldClone is true, a dedicated
-- bridge directory is created, and the source repo is cloned into it;
-- otherwise, the target repository is created alongside the source repo.
createBridge :: FilePath -> Bool -> IO ()
createBridge repoPath shouldClone = do
    -- TODO: Create bridge for repos with branches?
    fullOrigRepoPath <- canonicalizePath repoPath
    repoType <- identifyRepoType fullOrigRepoPath
    putStrLn $ unwords
      ["Identified", show repoType, "repo at", fullOrigRepoPath]
    (topLevelDir, sourceRepoPath) <-
      cloneIfNeeded shouldClone repoType fullOrigRepoPath
    targetRepoPath <- initTargetRepo sourceRepoPath repoType
    setCurrentDirectory topLevelDir
    mbBridgeDir <- findBridgeDir "."
    case mbBridgeDir of
      Nothing -> return ()
      Just path -> do
        putStrLn $ "Existing bridge directory detected: " ++ path
        exitFailure
    createDirectory bridgeDirName
    putStrLn $ unwords ["Initialised target", show $ otherVCS repoType,
                        "repo at", targetRepoPath]
    let config = createConfig repoType sourceRepoPath targetRepoPath
          shouldClone []
    putStrLn $ "Created " ++ bridgeDirName ++ " in " ++ topLevelDir
    withCurrentDirectory bridgeDirName $ do
        writeFile "config" $ stringifyConfig config
        createDirectory "marks"
        -- Create empty marks files for import/export for both repos.
        mapM_ (\f -> writeFile ("marks" </> f) "") [darcsExportMarksName,
            darcsImportMarksName, gitExportMarksName, gitImportMarksName]
        putStrLn "Wrote new marks files."
        let writeSetExec f hook = writeFile f hook >> setFileMode f fullPerms
        let bridgeDirPath = topLevelDir </> bridgeDirName
        writeSetExec "hook" $ createPreHook bridgeDirPath
        putStrLn "Wrote hook."
        setupHooks repoType sourceRepoPath targetRepoPath bridgeDirPath
        putStrLn $ "Wired up hook in both repos. Now syncing from "
          ++ show repoType
        syncBridge True "." (otherVCS repoType)
  where
    cloneIfNeeded :: Bool -> VCSType -> FilePath -> IO (FilePath, FilePath)
    cloneIfNeeded False _ path = return (takeDirectory path, path)
    cloneIfNeeded True vcsType origRepoPath = do
        cwd <- getCurrentDirectory
        let (_, repoName) = splitFileName origRepoPath
            topLevelDir = cwd </> repoName ++ "_bridge"
            clonedRepoPath = topLevelDir </> repoName
        tldExists <- doesDirectoryExist topLevelDir
        when tldExists (do
            putStrLn $ "Existing bridge directory detected: " ++ topLevelDir
            exitFailure)
        createDirectory topLevelDir
        putStrLn $ unwords ["Cloning source repo from", origRepoPath
                           , "to", clonedRepoPath]
        cloneRepo vcsType origRepoPath clonedRepoPath
        return (topLevelDir, clonedRepoPath)

    identifyRepoType :: FilePath -> IO VCSType
    identifyRepoType path = do
        let searchDirs = [(["_darcs"], Darcs),
                          ([".git"], Git),
                          (["branches", "objects", "refs"], BareGit)]
        mbType <- identifyRepoType' path searchDirs
        case mbType of
            Nothing -> die $ "Could not determine repo-type of " ++ path
            Just vcsType -> return vcsType
      where
        identifyRepoType' :: FilePath -> [([FilePath], VCSType)]
          -> IO (Maybe VCSType)
        identifyRepoType' _ [] = return Nothing
        identifyRepoType' p ((paths, vcsType):fs) = do
            exists <- mapM (doesDirectoryExist.(p </>)) paths
            if and exists
                then return (Just vcsType)
                else identifyRepoType' p fs

    createConfig :: VCSType -> FilePath -> FilePath -> Bool
      -> [(String, FilePath)] -> BridgeConfig
    createConfig Darcs = BridgeConfig
    createConfig _     = flip BridgeConfig

    fullPerms :: FileMode
    fullPerms = foldr unionFileModes nullFileMode
        [ownerModes, groupModes, otherModes]

    setupHooks :: VCSType -> FilePath -> FilePath -> FilePath -> IO ()
    setupHooks Darcs   = setupHooks'
    setupHooks BareGit = flip setupHooks'
    setupHooks Git = \gPath dPath -> setupHooks' dPath (gPath </> ".git")

    setupHooks' :: FilePath -> FilePath -> FilePath -> IO ()
    setupHooks' darcsPath gitPath bridgePath = do
        let gitHookPath = joinPath [gitPath, "hooks", "pre-receive"]
            darcsHookDir = joinPath [darcsPath, "_darcs", "hooks"]
            darcsHookPath = darcsHookDir </> "pre-apply"
            -- Writes a simple shell script hook file. The hook file written
            -- simply calls the hook script in the bridge directory, passing
            -- the appropriate VCS type.
            writeHookFile (path, vcsType) = do
                let hookCall =
                     (bridgePath </> "hook") ++ " " ++ vcsType ++ "\n"
                writeFile path hookCall
                setFileMode path fullPerms
        darcsHookDirExists <- doesDirectoryExist darcsHookDir
        unless darcsHookDirExists (createDirectory darcsHookDir)
        -- Write out hook files.
        mapM_ writeHookFile [(darcsHookPath, "darcs"), (gitHookPath, "git")]
        -- Update "apply" defaults, for Darcs.
        withCurrentDirectory darcsPath $ do
            addToPreflist "defaults" "apply prehook ./_darcs/hooks/pre-apply"
            forM_ darcsCommandsToDisable
              (\c -> addToPreflist "defaults" (c ++ " disable"))

-- We want to prevent a user running Darcs commands within the bridged repo, to
-- prevent non-synced patches (git commits must be pulled in first, as enabled
-- by the pre-hook of apply). We can't use "ALL disable" since we can't do
-- something like "apply no-disable".
darcsCommandsToDisable = [ "help", "add", "remove", "move", "replace"
                         , "revert", "unrevert", "whatsnew", "record"
                         , "unrecord", "amend-record", "mark-conflicts", "tag"
                         , "setpref", "diff", "changes", "annotate", "dist"
                         , "trackdown", "show", "pull", "fetch", "obliterate"
                         , "rollback", "push", "send", "get", "put"
                         , "initialize", "optimize", "check", "repair"
                         , "convert" ]

cloneRepo :: VCSType -> FilePath -> FilePath -> IO ()
-- This feels like a hack, since getCmd isn't exported.
cloneRepo Darcs old new = commandCommand DCG.get [Quiet] [old, new]
cloneRepo _ old new = do
    cloneProcHandle <- runProcess
        "git" ["clone", "-q", old, new]
    --  workDir Env     stdin   stdout  stderr
        Nothing Nothing Nothing Nothing Nothing
    cloneEC <- waitForProcess cloneProcHandle
    when (cloneEC /= ExitSuccess) (die "Git clone failed!")

nonMasterBranches :: BridgeConfig -> [(String, FilePath)]
nonMasterBranches = filter ((/= "master") . fst) . branches

stringifyConfig :: BridgeConfig -> String
stringifyConfig conf = to_string $ forceEither $
  set emptyCP "DEFAULT" "darcs_path" (darcs_path conf)
  >>= (\cp -> set cp "DEFAULT" "git_path" (git_path conf))
  >>= (\cp -> setshow cp "DEFAULT" "cloned" (cloned conf))
  >>= (\cp -> foldM (\cp' (bName, bDarcsPath) ->
    let sectionName = "branch/" ++ bName
        isNewBranch = not $ has_section cp' sectionName in
    (if isNewBranch then add_section cp' sectionName else return cp')
    >>= (\cp'' -> set cp'' sectionName "name" bName)
    >>= (\cp'' -> set cp'' sectionName "darcs_path" bDarcsPath))
    cp $ nonMasterBranches conf)

initTargetRepo :: FilePath -> VCSType -> IO FilePath
initTargetRepo fullRepoPath repoType = do
    let newPath = fullRepoPath ++ "_" ++ show (otherVCS repoType)
    initTargetRepo' repoType newPath
    return newPath

initTargetRepo' :: VCSType -> FilePath -> IO ()
initTargetRepo' Darcs newPath = do
    initProcHandle <- runCommand $ "git init -q --bare " ++ newPath
    initEC <- waitForProcess initProcHandle
    when (initEC /= ExitSuccess) (die "Git init failed!")
initTargetRepo' _ newPath = do
    amNotInRepository [WorkRepoDir newPath] -- create repodir
    createRepository [UseFormat2] -- create repo

-- |syncBridge takes a bridge folder location and a target vcs-type, and
-- attempts to pull in any changes from the other repo, having obtained the
-- lock, to prevent concurrent access. If this is the first sync, out-of-date
-- warnings and exitFailure aren't performed.
syncBridge :: Bool -> FilePath -> VCSType -> IO ()
syncBridge firstSync bridgePath repoType =
  withBridgeLock bridgePath (\bPath -> syncBridge' firstSync bPath repoType)

-- |syncBridge' actually does the sync, and assumes that a bridge lock is
-- currently held.
syncBridge' :: Bool -> FilePath -> VCSType -> IO ()
syncBridge' firstSync fullBridgePath repoType = do
    config <- getConfig fullBridgePath
    let converter = createConverter repoType config fullBridgePath
        bridgeFile fn = fullBridgePath </> fn
        marksFile fn = joinPath [fullBridgePath, "marks", fn]
        exportData = bridgeFile "darcs_bridge_export"
        oldSourceMarks = bridgeFile "old_source_marks"
        sourceMarks = marksFile $ sourceExportMarks converter
        oldTargetMarks = bridgeFile "old_target_marks"
        targetMarks = marksFile $ targetExportMarks converter
        importMarks = marksFile $ sourceImportMarks converter
        tempImportMarks = bridgeFile "import_marks"
        tempUpdateMarks = bridgeFile "temp_update_marks"

    putStrLn $ "Copying old sourcemarks: " ++ sourceMarks
    copyFile sourceMarks oldSourceMarks
    putStrLn "Doing export."
    exporter converter exportData
    putStrLn "Doing import."
    importer converter exportData
    putStrLn $ "Copying old targetmarks: " ++ targetMarks
    -- We need to ensure that the exporting repo knows about the
    -- mark ids of the just-imported data. We export on the
    -- just-imported repo, to update the marks file.
    copyFile targetMarks oldTargetMarks
    putStrLn "Doing mark update export."
    -- No /dev/null on windows, so output to temp file.
    markUpdater converter tempUpdateMarks
    -- We diff the marks files on both sides, to discover the newly
    -- added marks. This will allow us to manually update the
    -- import marks file of the original exporter.
    putStrLn "Diffing marks."
    newSourceExportMarks <-
      diffExportMarks oldSourceMarks sourceMarks
    newTargetExportMarks <-
      diffExportMarks oldTargetMarks targetMarks
    -- We want the source patch ids with the target mark ids.
    -- We drop the mark, but keep the rest of the line for patchIDs, since
    -- darcs-marks have a trailing branch name, which we want to keep.
    let patchIDs = map snd newSourceExportMarks
        markIDs = map fst newTargetExportMarks
        makeMarkLine m p = ":" ++ show m ++ " " ++ p
        newEntries = zipWith makeMarkLine markIDs patchIDs
    putStrLn $ show (length newEntries) ++ " marks to append."
    (exitMsg, exitRoutine) <- if (null newEntries)
      then return ("No changes to pull in via the bridge.", exitSuccess)
      else do
      -- Prepend new entries to the marks file
      writeFile tempImportMarks $ unlines newEntries
      existingImportMarks <- readFile importMarks
      appendFile tempImportMarks existingImportMarks
      renameFile tempImportMarks importMarks
      putStrLn "Import marks updated."
      if firstSync
        then return ("Bridge successfully synced.", exitSuccess)
        else return
          ("Changes were pulled in via the bridge, update your local repo."
          , -- non-zero exit-code to signal to the VCS that action is
            -- required by the user before allowing the push/apply.
            exitFailure)
    mapM_ removeFile [oldTargetMarks, oldSourceMarks, exportData,
        tempUpdateMarks]
    putStrLn exitMsg
    exitRoutine
  where
    diffExportMarks :: FilePath -> FilePath -> IO [(Int, String)]
    diffExportMarks old new = do
        oldLines <- getLines old
        newLines <- getLines new
        let diffedLines = diffExportMarks' oldLines newLines
        return $ sortBy (comparing fst) $ map splitMarkLine diffedLines

    -- New marks may be prepended or appended to the marks file (Git seemingly
    -- prepends for import and appends for export) so find the set of new
    -- mark-lines, at either end.
    diffExportMarks' :: [String] -> [String] -> [String]
    diffExportMarks' [] new = new
    diffExportMarks' (x:xs) (y:ys) | x == y = dropLeadingEqual xs ys
    diffExportMarks' (x:_) ys = takeWhile (/= x) ys

    dropLeadingEqual :: [String] -> [String] -> [String]
    dropLeadingEqual [] ys = ys
    dropLeadingEqual _ [] = error "New mark list is shorter!"
    dropLeadingEqual (x:xs) (y:ys) | x == y = dropLeadingEqual xs ys
    dropLeadingEqual _ ys = ys

    splitMarkLine :: String -> (Int, String)
    splitMarkLine line = let ws = words line in
      (read . drop 1 $ head ws, unwords $ tail ws)

    getLines :: FilePath -> IO [String]
    getLines file = fmap lines $ readFile file


putConfig :: FilePath -> BridgeConfig -> IO ()
putConfig fullBridgePath config = withCurrentDirectory fullBridgePath $
  writeFile "config" $ stringifyConfig config

getConfig :: FilePath -> IO BridgeConfig
getConfig fullBridgePath = do
  errConfig <- runErrorT $ readConfig fullBridgePath
  case errConfig of
      Left e -> die $ "Malformed/missing config file in " ++ fullBridgePath
        ++ "\nerror: " ++ show e
      Right config -> return config
  where
    readConfig bridgePath = do
      configFile <- join $ liftIO $ readfile emptyCP (bridgePath </> "config")
      wasCloned <- get configFile "DEFAULT" "cloned"
      darcsPath <- get configFile "DEFAULT" "darcs_path"
      gitPath   <- get configFile "DEFAULT" "git_path"
      let bSections = filter ("branch/" `isPrefixOf`) $ sections configFile
          readBranchSection section = do
           name <- get configFile section "name"
           path <- get configFile section "darcs_path"
           return (name, path)
          masterBranch = ("master", darcsPath)
      bs <- (masterBranch :) `fmap` forM bSections readBranchSection
      return $ BridgeConfig darcsPath gitPath wasCloned bs

-- createConverter creates a converter that will pull changes into the repo of
-- type targetRepoType
createConverter :: VCSType -> BridgeConfig -> FilePath -> Converter
createConverter targetRepoType config fullBridgePath = case targetRepoType of
        Darcs -> Converter
            gitExport darcsImport darcsExport gitExportMarksName
              darcsExportMarksName gitImportMarksName
        _     -> Converter
            darcsExport gitImport gitExport darcsExportMarksName
              gitExportMarksName darcsImportMarksName
  where
    makeMarkPath :: String -> FilePath
    makeMarkPath name = joinPath [fullBridgePath, "marks", name]

    darcsPath = darcs_path config
    gitPath   = git_path config

    darcsFCCmd file mode marksFile toRun = do
        let marksPath = makeMarkPath marksFile
        withFile file mode (handleCmdMarks marksPath marksPath . toRun)

    darcsImport source = darcsFCCmd source ReadMode darcsImportMarksName $
        \input -> fastImportIncremental True input putStrLn darcsPath

    darcsExport target = darcsFCCmd target WriteMode darcsExportMarksName $
        \output -> fastExport (printer output) darcsPath branchNames
      where
        printer h s = liftIO $ BL.hPut h s >> BL.hPut h (BL.singleton '\n')
        branchNames = map snd $ nonMasterBranches config

    waitForGit :: IO ProcessHandle -> IO ()
    waitForGit cmd = do exporterProcHandle <- cmd
                        exportEC <- waitForProcess exporterProcHandle
                        when (exportEC /= ExitSuccess)
                          (die "A subcommand failed!")

    gitExport target = waitForGit $ rawGitExport target
      (map fst $ branches config)
    gitImport source = waitForGit $ rawGitImport source

    rawGitExport target branchList = do
        let marksPath = makeMarkPath gitExportMarksName
        withFile target WriteMode (\output ->
           runProcess "git"
                (["fast-export", "--export-marks="++marksPath,
                 "--import-marks="++marksPath, "-M", "-C"] ++ branchList
                 -- We append "--" to signify that we mean revisions rather
                 -- than files (e.g. a file of the same name as a branch)
                 ++ ["--"])
                (Just gitPath) Nothing Nothing (Just output) Nothing)

    rawGitImport source = do
        let marksPath = makeMarkPath gitImportMarksName
        env <- getEnvironment
        -- Git sets GIT_DIR, preventing git commands from running in any
        -- other cwd.
        let unGitEnv = filter (\(k,_) -> k /= "GIT_DIR") env
        withFile source ReadMode (\input ->
            runProcess "git"
                ["fast-import", "--quiet", "--export-marks="++marksPath,
                 "--import-marks="++marksPath]
                (Just gitPath) (Just unGitEnv) (Just input) Nothing Nothing)

-- |createPreHook returns a string containing a shell script that takes a
-- single argument (the target vcs type) and pulls any changes into that repo.
createPreHook :: FilePath -> String
createPreHook bridgeDir = unlines hook where
  hook = [ "#!/bin/sh"
         , "[[ $1 != \"darcs\" && $1 != \"git\" ]] && "
           ++ "echo >&2 \"Invalid repo-type: $1\" && exit 1;\n"
         , "if [[ $1 == d* ]]; then other=\"git\"; else other=\"darcs\"; fi"
         , "echo \"Pulling in any changes from the $other repo...\"\n"
         , "darcs-fastconvert sync --bridge=" ++ bridgeDir
           ++ " --repo-type=$1\n" ]

-- |listBranches returns a list of branches that are managed by the bridge.
listBranches :: FilePath -> IO [(String, FilePath)]
listBranches bridgePath = withBridgeLock bridgePath $ \fullBridgePath ->
  branches `fmap` getConfig fullBridgePath

trackBranch :: FilePath -> RepoBranch -> IO ()
trackBranch bridgePath (GitBranch bName) =
  withBridgeLock bridgePath $ \fullBridgePath -> do
    config <- getConfig fullBridgePath
    withCurrentDirectory (git_path config) $ do
      gitBranches <- splitBranches `fmap` readProcess "git" ["branch", "-a"] ""
      unless (bName `elem` gitBranches) $
        die $ "git branch " ++  bName ++ " doesn't exist."
      let darcsRepoDir = topLevelBranchDir (darcs_path config)
            (parseBranch . BC.pack $ "refs/heads/" ++ bName)
      createDirectory darcsRepoDir
      initTargetRepo' Git darcsRepoDir
      addBranchToConfig fullBridgePath config (bName, darcsRepoDir)
      syncBridge' True fullBridgePath Darcs
  where
    splitBranches = map dropLeading . lines
    -- |dropLeading removes any leading whitespace and current-branch markers.
    dropLeading ('*':s) = dropLeading s
    dropLeading (' ':s) = dropLeading s
    dropLeading s       = s

trackBranch bridgePath (DarcsBranch bPath) =
  withBridgeLock bridgePath $ \fullBridgePath -> do
    config <- getConfig fullBridgePath
    let mainDarcsPath = darcs_path config
    fullBranchPath <- canonicalizePath bPath
    withCurrentDirectory mainDarcsPath $
      withRepository [] $ RepoJob $ \repo -> do
        bRepo <- identifyRepositoryFor repo fullBranchPath
        mainPatches <- newset2FL `fmap` readRepo repo
        branchPatches <- newset2FL `fmap` readRepo bRepo
        -- We'd rather fail now, than on the next export.
        unless (equalHead mainPatches branchPatches) $
          die "Cannot add branch that doesn't share patch #1 with master."
        let bName = fp2bn fullBranchPath
            clonePath = dropFileName mainDarcsPath </> bName
        repoLocation <-
          if cloned config
            then cloneRepo Darcs fullBranchPath clonePath >> return clonePath
            else return fullBranchPath
        addBranchToConfig fullBridgePath config (bName, repoLocation)
        syncBridge' True fullBridgePath Git

addBranchToConfig :: FilePath -> BridgeConfig -> (String, FilePath) -> IO ()
addBranchToConfig fullBridgePath config branch = do
  when (branch `elem` branches config) $
    die $ "Branch " ++ fst branch ++ " is already tracked by the bridge!"
  let newBranches = branch : branches config
  putConfig fullBridgePath $ config { branches = newBranches }

untrackBranch :: FilePath -> String -> IO ()
untrackBranch _ "master" = die "Cannot remove master branch."
untrackBranch bridgePath bName =
  withBridgeLock bridgePath $ \fullBridgePath -> do
    config <- getConfig fullBridgePath
    let (branch, others) = partition ((== bName) . fst) $ branches config
    if null branch
      then die $ "Branch " ++ bName ++ " is not tracked."
      else do
        putConfig fullBridgePath $ config { branches = others }
        putStrLn $ "No longer tracking branch " ++ bName

-- |withBridgeLock attempts to take the bridge lock and execute the given
-- action, which takes the fullBridgePath as its argument.
withBridgeLock :: FilePath -> (FilePath -> IO a) -> IO a
withBridgeLock bridgePath action = do
    mbFullBridgePath <- findBridgeDir bridgePath
    case mbFullBridgePath of
      Nothing -> die $ "Cannot find .darcs_bridge in " ++ bridgePath
      Just fullBridgePath -> do
        let lockPath = fullBridgePath </> "lock"
        gotLock <- withLockCanFail lockPath $ action fullBridgePath
        case gotLock of
            Left _  -> do putStrLn $ "Cannot take bridge lock:" ++ lockPath
                          exitFailure
            Right a -> return a
