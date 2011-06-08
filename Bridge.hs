{-# LANGUAGE DeriveDataTypeable #-}
module Bridge( createBridge, syncBridge, VCSType(..) ) where

import Utils ( die )
import Marks ( handleCmdMarks )
import Import ( fastImportIncremental )
import Export ( fastExport )

import Control.Monad ( when, unless )
import Control.Monad.Error ( join )
import Control.Monad.Trans.Error
import Control.Monad.IO.Class
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.ConfigFile ( emptyCP, set, to_string, readfile, get )
import Data.Data ( Data, Typeable )
import Data.Either.Utils ( forceEither )
import Darcs.Lock ( withLockCanFail )
import System.Directory ( createDirectory, canonicalizePath, setCurrentDirectory, getCurrentDirectory, copyFile, removeFile, renameFile, doesDirectoryExist )
import System.Environment ( getEnvironment )
import System.Exit ( exitFailure, exitSuccess, ExitCode(ExitSuccess) )
import System.FilePath ( (</>), takeDirectory, joinPath, splitFileName )
import System.IO ( hFileSize, withFile, IOMode(ReadMode,WriteMode) )
import System.Process ( runCommand, runProcess, ProcessHandle, waitForProcess )
import System.PosixCompat.Files
import System.Posix.Types ( FileMode )

import Darcs.Commands ( commandCommand )
import qualified Darcs.Commands.Get as DCG
import Darcs.Repository ( amNotInRepository, createRepository )
import Darcs.Repository.Prefs ( addToPreflist )
import Darcs.Flags ( DarcsFlag(WorkRepoDir, UseFormat2, Quiet) )
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
                                 }

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

-- |createBridge sets up a Darcs bridge. If shouldClone is true, a dedicated
-- bridge directory is created, and the source repo is cloned into it;
-- otherwise, the target repository is created alongside the source repo.
createBridge :: FilePath -> Bool -> IO ()
createBridge repoPath shouldClone = do
    fullOrigRepoPath <- canonicalizePath repoPath
    repoType <- identifyRepoType fullOrigRepoPath
    putStrLn $ unwords ["Identified", show repoType, "repo at", fullOrigRepoPath]
    (topLevelDir, sourceRepoPath) <- cloneIfNeeded shouldClone repoType fullOrigRepoPath
    targetRepoPath <- initTargetRepo sourceRepoPath repoType
    setCurrentDirectory topLevelDir
    bridgeDirExists <- doesDirectoryExist bridgeDirName
    when bridgeDirExists (do
        putStrLn $ "Existing bridge directory detected: " ++ (topLevelDir </> bridgeDirName)
        exitFailure)
    createDirectory bridgeDirName
    putStrLn $ unwords ["Initialised target", show $ otherVCS repoType,
                        "repo at", targetRepoPath]
    let config   = createConfig repoType sourceRepoPath targetRepoPath
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
        putStrLn $ "Wired up hook in both repos. Now syncing from " ++ show repoType
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

    cloneRepo :: VCSType -> FilePath -> FilePath -> IO ()
    -- This feels like a hack, since getCmd isn't exported.
    cloneRepo Darcs old new = commandCommand DCG.get [Quiet] [old, new]
    cloneRepo _ old new = do
        cloneProcHandle <- runProcess
            "git" ["clone", "-q", old, new] Nothing Nothing Nothing Nothing Nothing
        cloneEC <- waitForProcess cloneProcHandle
        when (cloneEC /= ExitSuccess) (die "Git clone failed!")

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
        identifyRepoType' :: FilePath -> [([FilePath], VCSType)] -> IO (Maybe VCSType)
        identifyRepoType' _ [] = return Nothing
        identifyRepoType' p ((paths, vcsType):fs) = do
            exists <- mapM (doesDirectoryExist.(p </>)) paths
            if and exists then return (Just vcsType) else identifyRepoType' p fs

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

    createConfig :: VCSType -> FilePath -> FilePath -> BridgeConfig
    createConfig Darcs = BridgeConfig
    createConfig _     = flip BridgeConfig

    stringifyConfig :: BridgeConfig -> String
    stringifyConfig conf = to_string $ forceEither $ do
        cp <- set emptyCP "DEFAULT" "darcs_path" (darcs_path conf)
        set cp "DEFAULT" "git_path" (git_path conf)

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
                let hookCall = (bridgePath </> "hook") ++ " " ++ vcsType ++ "\n"
                writeFile path hookCall
                setFileMode path fullPerms -- TODO: Windows?
        darcsHookDirExists <- doesDirectoryExist darcsHookDir
        unless darcsHookDirExists (createDirectory darcsHookDir)
        -- Write out hook files.
        mapM_ writeHookFile [(darcsHookPath, "darcs"), (gitHookPath, "git")]
        -- Update "apply" defaults, for Darcs.
        withCurrentDirectory darcsPath $
            addToPreflist "defaults" "apply prehook ./_darcs/hooks/pre-apply"

-- |syncBridge takes a bridge folder location and a target vcs-type, and
-- attempts to pull in any changes from the other repo, having obtained the
-- lock, to prevent concurrent access. If this is the first sync, out-of-date
-- warnings and exitFailure aren't performed.
syncBridge :: Bool -> FilePath -> VCSType -> IO ()
syncBridge firstSync bridgePath repoType = do
    fullBridgePath <- canonicalizePath bridgePath
    setCurrentDirectory fullBridgePath
    gotLock <- withLockCanFail "lock" (syncBridge' firstSync fullBridgePath repoType)
    case gotLock of
        Left _ -> putStrLn "Cannot take bridge lock!" >> exitFailure
        _      -> exitSuccess

syncBridge' :: Bool -> FilePath -> VCSType -> IO ()
syncBridge' firstSync fullBridgePath repoType = do
    errConfig <- runErrorT $ getConfig fullBridgePath
    case errConfig of
        Left _ -> die $ "Malformed/missing config file in " ++ fullBridgePath
        Right config -> do
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
            size <- withFile exportData ReadMode hFileSize
            when (size > 0) (do
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
                newSourceExportMarks <- diffExportMarks oldSourceMarks sourceMarks
                newTargetExportMarks <- diffExportMarks oldTargetMarks targetMarks
                -- We want the source patch ids with the target mark ids
                let patchIDs = map ((!! 1).words) newSourceExportMarks
                    markIDs = map ((!! 0).words) newTargetExportMarks
                    markFudger m p = alterMark m ++ " " ++ p
                    newEntries = zipWith markFudger markIDs patchIDs
                putStrLn $ show (length newEntries) ++ " marks to append."
                -- Prepend new entries to the marks file
                writeFile tempImportMarks $ unlines newEntries
                existingImportMarks <- readFile importMarks
                appendFile tempImportMarks existingImportMarks
                renameFile tempImportMarks importMarks
                putStrLn "Import marks updated."
                mapM_ removeFile [oldTargetMarks, oldSourceMarks, exportData,
                    tempUpdateMarks]

                if firstSync
                  then do
                      putStrLn "Bridge successfully synced."
                      exitSuccess
                  else do
                      putStrLn "Changes were pulled in via the bridge, update your local repo."
                      -- non-zero exit-code to signal to the VCS that action is
                      -- required by the user before allowing the push/apply.
                      exitFailure)
            mapM_ removeFile [oldSourceMarks, exportData]
            putStrLn "No changes to pull in via the bridge."
            exitSuccess
  where
    -- Darcs and Git marks both contain a colon, but it is situated at opposite
    -- ends of the mark. Since we are copying the marks from one repo to
    -- another, we need to convert them.
    alterMark :: String -> String
    alterMark (':' : ms) = ms ++ ":"
    alterMark ms         = ':' : init ms

    diffExportMarks :: FilePath -> FilePath -> IO [String]
    diffExportMarks old new = do
        oldLines <- getLines old
        newLines <- getLines new
        return $ diffExportMarks' oldLines newLines

    getLines :: FilePath -> IO [String]
    getLines file = fmap lines $ readFile file

    -- New marks are prepended to the marksfile, so take until we hit a
    -- matching line.
    diffExportMarks' :: [String] -> [String] -> [String]
    diffExportMarks' []     = id
    diffExportMarks' (x:_) = takeWhile (/= x)

    getConfig bridgePath = do
        configFile <- join $ liftIO $ readfile emptyCP (bridgePath </> "config")
        darcsPath <- get configFile "DEFAULT" "darcs_path"
        gitPath   <- get configFile "DEFAULT" "git_path"
        return $ BridgeConfig darcsPath gitPath

-- createConverter creates a converter that will pull changes into the repo of
-- type targetRepoType
createConverter :: VCSType -> BridgeConfig -> FilePath -> Converter
createConverter targetRepoType config fullBridgePath = case targetRepoType of
        Darcs -> Converter
            gitExport darcsImport darcsExport gitExportMarksName darcsExportMarksName gitImportMarksName
        _     -> Converter
            darcsExport gitImport gitExport darcsExportMarksName gitExportMarksName darcsImportMarksName
  where
    makeMarkPath :: String -> FilePath
    makeMarkPath name = joinPath [fullBridgePath, "marks", name]

    darcsPath = darcs_path config
    gitPath   = git_path config

    darcsFCCmd file mode marksFile toRun = do
        let marksPath = makeMarkPath marksFile
        withFile file mode (\h -> handleCmdMarks marksPath marksPath (toRun h))

    darcsImport source = darcsFCCmd source ReadMode darcsImportMarksName $
        \input -> fastImportIncremental input (liftIO.putStrLn) darcsPath

    darcsExport target = darcsFCCmd target WriteMode darcsExportMarksName $
        \output -> fastExport (\s -> liftIO $ BL.hPut output s >> BL.hPut output (BL.singleton '\n')) darcsPath

    waitForGit :: IO ProcessHandle -> IO ()
    waitForGit cmd = do exporterProcHandle <- cmd
                        exportEC <- waitForProcess exporterProcHandle
                        when (exportEC /= ExitSuccess) (die "A subcommand failed!")

    gitExport target = waitForGit $ rawGitExport target
    gitImport source = waitForGit $ rawGitImport source

    rawGitExport target = do
        let marksPath = makeMarkPath gitExportMarksName
        withFile target WriteMode (\output ->
           runProcess "git"
                ["fast-export", "--export-marks="++marksPath,
                 "--import-marks="++marksPath, "-M", "-C", "HEAD"]
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
