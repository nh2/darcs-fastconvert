{-# LANGUAGE DeriveDataTypeable #-}
module Bridge( createBridge, syncBridge, VCSType(..) ) where

import Utils ( die )
import Control.Monad ( when, void )
import Control.Monad.Error ( join )
import Control.Monad.Trans.Error
import Control.Monad.IO.Class
import Data.ConfigFile ( emptyCP, set, to_string, readfile, get )
import Data.Data ( Data, Typeable )
import Data.Either.Utils ( forceEither )
import Darcs.Lock ( withLockCanFail )
import System.Directory ( createDirectory, canonicalizePath, setCurrentDirectory, copyFile, removeFile, renameFile, doesDirectoryExist )
import System.Environment ( getEnvironment )
import System.Exit ( exitFailure, exitSuccess, ExitCode(ExitSuccess) )
import System.FilePath ( (</>), takeDirectory, joinPath )
import System.IO ( hFileSize, withFile, IOMode(ReadMode,WriteMode) )
import System.Process ( runCommand, runProcess, ProcessHandle, waitForProcess )
import System.PosixCompat.Files
import System.Posix.Types ( FileMode )

import Darcs.Repository ( amNotInRepository, createRepository )
import Darcs.Repository.Prefs ( addToPreflist )
import Darcs.Flags ( DarcsFlag(WorkRepoDir, UseFormat2) )
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

data Converter = Converter { exporter :: FilePath -> IO ProcessHandle
                           , importer :: FilePath -> IO ProcessHandle
                           , markUpdater :: FilePath -> IO ProcessHandle
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

-- |createBridge sets up a Darcs bridge. It creates a bridge folder alongside
-- the given repository path, and creates and syncs a clone, using the "other"
-- VCS.
createBridge :: FilePath -> IO ()
createBridge repoPath = do
    fullRepoPath <- canonicalizePath repoPath
    repoType <- identifyRepoType fullRepoPath
    putStrLn $ unwords $ ["Identified", show repoType, "repo at", fullRepoPath]
    targetRepoPath <- initTargetRepo fullRepoPath repoType
    putStrLn $ unwords ["Initialised target", show $ otherVCS repoType,
                        "repo at", targetRepoPath]
    let topLevelDir = takeDirectory fullRepoPath
        config   = createConfig repoType fullRepoPath targetRepoPath
    setCurrentDirectory topLevelDir
    createDirectory bridgeDirName
    putStrLn $ "Created " ++ bridgeDirName ++ " in " ++ topLevelDir
    withCurrentDirectory bridgeDirName $ do
        writeFile "config" $ stringifyConfig config
        createDirectory "marks"
        -- Create empty marks files for import/export for both repos.
        mapM_ (\f -> writeFile ("marks" </> f) "") [darcsExportMarksName,
            darcsImportMarksName, gitExportMarksName, gitImportMarksName]
        putStrLn $ "Wrote new marks files."
        let writeSetExec f hook = writeFile f hook >> setFileMode f fullPerms
        writeSetExec "hook" $ createPreHook (topLevelDir </> bridgeDirName)
        putStrLn $ "Wrote hook."
        setupHooks repoType fullRepoPath targetRepoPath (topLevelDir </> bridgeDirName)
        putStrLn $ "Wired up hook in both repos. Now syncing from " ++ show repoType
        syncBridge "." (otherVCS repoType)
  where
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
        let newPath = fullRepoPath ++ "_" ++ (show $ otherVCS repoType)
        initTargetRepo' repoType newPath
        return newPath

    initTargetRepo' :: VCSType -> FilePath -> IO ()
    initTargetRepo' Darcs newPath = do
        initProcHandle <- runCommand $ "git init --bare " ++ newPath
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
            darcsHookPath = joinPath [darcsPath, "_darcs", "hooks", "pre-apply"]
            -- Writes a simple shell script hook file. The hook file written
            -- simply calls the hook script in the bridge directory, passing
            -- the appropriate VCS type.
            writeHookFile (path, vcsType) = do
                let hookCall = (bridgePath </> "hook") ++ " " ++ vcsType ++ "\n"
                writeFile path hookCall
                setFileMode path fullPerms -- TODO: Windows?
        -- Create Darcs hooks dir.
        createDirectory (takeDirectory darcsHookPath)
        -- Write out hook files.
        mapM_ writeHookFile [(darcsHookPath, "darcs"), (gitHookPath, "git")]
        -- Update "apply" defaults, for Darcs.
        withCurrentDirectory darcsPath $
            addToPreflist "defaults" "apply prehook ./_darcs/hooks/pre-apply"

-- |syncBridge takes a bridge folder location and a target vcs-type, and
-- attempts to pull in any changes from the other repo, having obtained the
-- lock, to prevent concurrent access.
syncBridge :: FilePath -> VCSType -> IO ()
syncBridge bridgePath repoType = do
    fullBridgePath <- canonicalizePath bridgePath
    setCurrentDirectory fullBridgePath
    gotLock <- withLockCanFail "lock" (syncBridge' fullBridgePath repoType)
    case gotLock of
        Left _ -> putStrLn "Cannot take bridge lock!" >> exitFailure
        _      -> exitSuccess

syncBridge' :: FilePath -> VCSType -> IO ()
syncBridge' fullBridgePath repoType = do
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
            exporterProcHandle <- exporter converter exportData
            exportEC <- waitForProcess exporterProcHandle
            when (exportEC /= ExitSuccess) (die "Export failed!")
            size <- withFile exportData ReadMode hFileSize
            when (size > 0) (do
                putStrLn "Doing import."
                importerProcHandle <- importer converter exportData
                importEC <- waitForProcess importerProcHandle
                when (importEC /= ExitSuccess) (die "Import failed!")
                putStrLn $ "Copying old targetmarks: " ++ targetMarks
                -- We need to ensure that the exporting repo knows about the
                -- mark ids of the just-imported data. We export on the
                -- just-imported repo, to update the marks file.
                copyFile targetMarks oldTargetMarks
                putStrLn "Doing mark update export."
                -- No /dev/null on windows, so output to temp file.
                updaterProcHandle <- markUpdater converter tempUpdateMarks
                updaterEC <- waitForProcess updaterProcHandle
                when (updaterEC /= ExitSuccess) (die "Updater failed!")
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
                putStrLn $ (show $ length newEntries) ++ " marks to append."
                -- Prepend new entries to the marks file
                writeFile tempImportMarks $ unlines newEntries
                existingImportMarks <- readFile importMarks
                appendFile tempImportMarks existingImportMarks
                renameFile tempImportMarks importMarks
                putStrLn "Import marks updated."
                mapM_ removeFile [oldTargetMarks, oldSourceMarks, exportData,
                    tempUpdateMarks]

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

    darcsImport source = do
        let marksPath = makeMarkPath darcsImportMarksName
        withFile source ReadMode (\input ->
            runProcess "darcs-fastconvert"
                ["import", "--create=no", "--write-marks="++marksPath,
                "--read-marks="++marksPath, darcsPath]
                Nothing Nothing (Just input) Nothing Nothing)

    darcsExport target = do
        let marksPath = makeMarkPath darcsExportMarksName
        withFile target WriteMode (\output ->
            runProcess "darcs-fastconvert"
                ["export", "--write-marks="++marksPath,
                 "--read-marks="++marksPath, darcsPath]
                Nothing Nothing Nothing (Just output) Nothing)

    gitExport target = do
        let marksPath = makeMarkPath gitExportMarksName
        withFile target WriteMode (\output ->
           runProcess "git"
                ["fast-export", "--export-marks="++marksPath,
                 "--import-marks="++marksPath, "-M", "-C", "HEAD"]
                (Just gitPath) Nothing Nothing (Just output) Nothing)

    gitImport source = do
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
