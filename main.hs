{-# LANGUAGE DeriveDataTypeable #-}
import qualified Marks
import Import
import Export
import Bridge
import Patch

import Control.Monad ( forM_ )
import Control.Monad.Trans ( liftIO )
import qualified Data.ByteString.Lazy as BL
import System.Console.CmdLib
import System.IO ( stdin )

data Cmd = Import { debug :: Bool
                  , repo :: String
                  , format :: RepoFormat
                  , create :: Bool
                  , readMarks :: FilePath
                  , writeMarks :: FilePath }
         | Export { repo :: String
                  , readMarks :: FilePath
                  , writeMarks :: FilePath
                  , branches :: [FilePath] }
         | CreateBridge { inputRepo :: String
                        , clone :: Bool}
         | Sync   { bridgePath :: String
                  , repoType :: VCSType }
         | ApplyPatch { repo :: String
                      , prompt :: Bool
                      , patchFile :: FilePath }
         | Branch {}
         deriving (Eq, Typeable, Data)

data BranchCmd = List { branchBridgePath :: String }
               | Track { branchBridgePath :: String
                     , branchType :: VCSType
                     , branchPath :: String }
               | Untrack { branchBridgePath :: String
                        , branchPath :: String }
               deriving (Eq, Typeable, Data)

instance Attributes Cmd where
  attributes _ =
    repo %> [ Positional 0 ] %%
    patchFile %> [ Positional 1 ] %%
    inputRepo %> [ Positional 0 ] %% group "Options"
    [ format     %> [ Help
      "repository type to create: darcs-2 (default) or hashed"
                    , Default Darcs2Format ]
    , create     %> [ Help "create a new repository", Default True ]
    , readMarks  %> [ Help
      "continue conversion, previously checkpointed by --write-marks"
                    , ArgHelp "FILE" ]
    , writeMarks %> [ Help "checkpoint conversion to continue it later"
                    , ArgHelp "FILE" ]
    , repoType   %> [ Help "conversion source repo type"
                    , ArgHelp "(git|darcs)"]
    , bridgePath %> [ Help "directory containing an existing darcs bridge"
                    , ArgHelp "DIR"]
    , clone      %> [ Help "clone source repo into dedicated bridge dir"
                    , Default True]
    , debug      %> [ Help "output extra activity information"
                    , Default False ]
    , prompt     %> [ Help "prompt user if file hash does not match patch hash"
                    , Default True ]
    , branches   %> [ Help "branches to be exported"
                    , Extra True ] ]

  readFlag _ = readCommon <+< readFormat <+< readInputType
    where readFormat "darcs-2" = Darcs2Format
          readFormat "hashed" = HashedFormat
          readFormat x = error $ "No such repository format " ++ show x

readInputType :: String -> VCSType
readInputType "git" = Git
readInputType "darcs" = Darcs
readInputType x = error $ "No such input-type " ++ show x

instance Attributes BranchCmd where
  attributes _ =
    branchBridgePath %> [ Positional 0 ] %%
    branchPath %> [ Positional 1 ] %%
    branchType %> [ Help "Branch repo type"
                  , ArgHelp "(git|darcs)"
                  , Default Darcs]

  readFlag _ = readCommon <+< readInputType

instance RecordCommand Cmd where
  mode_summary Import {} = "Import a git-fast-export dump into darcs."
  mode_summary Export {} =
    "Export a darcs repository to a git-fast-import stream."
  mode_summary CreateBridge {} =
    "Create a darcs bridge, importing from git or darcs."
  mode_summary Sync   {} = "Sync an existing darcs bridge."
  mode_summary ApplyPatch {} = "Apply a git patch to a darcs repo."
  mode_summary Branch {} = "Manage bridged branches."

  mode_help Branch {} = helpCommands (recordCommands (undefined :: BranchCmd))
  mode_help _ = ""

  run' c@(Import {}) _ = handleImport c
  run' c@(Export {}) _ = handleExport c
  run' c@(CreateBridge {}) _ = handleCreateBridge c
  run' c@(Sync {}) _ = handleSync c
  run' c@(ApplyPatch {}) _ = handleApplyPatch c
  run' (Branch {}) opts =
    dispatch [] (recordCommands (undefined :: BranchCmd)) opts

  -- Don't process Branch options, since we want to dispatch on them.
  rec_optionStyle (Branch {}) = NoOptions
  rec_optionStyle _ = Permuted

instance RecordCommand BranchCmd where
  mode_summary List {} = "List all managed branches."
  mode_summary Track {}  = "Add a branch, so it is tracked."
  mode_summary Untrack {}  = "No longer track a branch."

  run' c@(List {}) _ = checkBridgeArg c >> handleListBranches c
  run' c@(Track {}) _ = checkBridgeArg c >> handleTrackBranch c
  run' c@(Untrack {}) _ = checkBridgeArg c >> handleUntrackBranch c

runCmdWithMarks :: Cmd -> (Marks.Marks -> IO Marks.Marks) -> IO ()
runCmdWithMarks c = Marks.handleCmdMarks (readMarks c) (writeMarks c)

checkBridgeArg :: BranchCmd -> IO ()
checkBridgeArg c = case branchBridgePath c of
  "" -> die "Missing bridge path argument."
  _  -> return ()

handleListBranches :: BranchCmd -> IO ()
handleListBranches c = do
  bs <- listBranches (branchBridgePath c)
  forM_ bs (\(name, darcsPath) -> putStrLn $
    unwords ["Name:", name, "--", "Darcs path:", darcsPath])

handleTrackBranch :: BranchCmd -> IO ()
handleTrackBranch c = case branchType c of
  Darcs -> trackBranch (branchBridgePath c) $ DarcsBranch (branchPath c)
  _ -> trackBranch (branchBridgePath c) $ GitBranch (branchPath c)

handleUntrackBranch  :: BranchCmd -> IO ()
handleUntrackBranch c = untrackBranch (branchBridgePath c) (branchPath c)

handleImport :: Cmd -> IO ()
handleImport c | create c =
  case readMarks c of
    [] -> format c `seq` runCmdWithMarks c
      (const $
        fastImport (debug c) stdin putStrLn (repo c) (format c))
    _  -> die "cannot create repo, with existing marksfile."

handleImport c = runCmdWithMarks c $
  fastImportIncremental (debug c) stdin putStrLn (repo c)

handleExport :: Cmd -> IO ()
handleExport c = runCmdWithMarks c $
  fastExport (liftIO . BL.putStrLn) (repo c) (branches c)

handleCreateBridge :: Cmd -> IO ()
handleCreateBridge c = case inputRepo c of
  [] -> die "missing input-repo argument."
  r  -> createBridge r (clone c)

handleSync :: Cmd -> IO ()
handleSync c = case bridgePath c of
  [] -> die "missing bridge-path argument."
  b  -> syncBridge False b (repoType c)

handleApplyPatch :: Cmd -> IO ()
handleApplyPatch c = readAndApplyGitEmail (repo c) (prompt c) (patchFile c)

main :: IO ()
main = getArgs >>= dispatch [] (recordCommands (undefined :: Cmd))
