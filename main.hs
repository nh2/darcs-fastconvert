{-# LANGUAGE DeriveDataTypeable #-}
import qualified Marks
import Import
import Export
import Bridge

import Control.Monad.Trans ( liftIO )
import qualified Data.ByteString.Lazy as BL
import System.Console.CmdLib
import System.IO ( stdin )

data Cmd = Import { repo :: String
                  , format :: RepoFormat
                  , create :: Bool
                  , readMarks :: FilePath
                  , writeMarks :: FilePath }
         | Export { repo :: String
                  , readMarks :: FilePath
                  , writeMarks :: FilePath }
         | CreateBridge { inputRepo :: String
                        , clone :: Bool}
         | Sync   { bridgePath :: String
                  , repoType :: VCSType }
         deriving (Eq, Typeable, Data)

instance Attributes Cmd where
  attributes _ =
    repo %> [ Positional 0 ] %% group "Options"
    [ format     %> [ Help "repository type to create: darcs-2 (default) or hashed"
                    , Default Darcs2Format ]
    , create     %> [ Help "create a new repository", Default True ]
    , readMarks  %> [ Help "continue conversion, previously checkpointed by --write-marks"
                    , ArgHelp "FILE" ]
    , writeMarks %> [ Help "checkpoint conversion to continue it later"
                    , ArgHelp "FILE" ]
    , inputRepo  %> [ Help "top-level dir of existing git/darcs repo to bridge"
                    , ArgHelp "PATH"]
    , repoType   %> [ Help "conversion source repo type"
                    , ArgHelp "(git|darcs)"]
    , bridgePath %> [ Help "directory containing an existing darcs bridge"
                    , ArgHelp "DIR"]
    , clone      %> [ Help "clone source repo into dedicated bridge dir"
                    , Default True] ]

  readFlag _ = readCommon <+< readFormat <+< readInputType
    where readFormat "darcs-2" = Darcs2Format
          readFormat "hashed" = HashedFormat
          readFormat x = error $ "No such repository format " ++ show x
          readInputType "git" = Git
          readInputType "darcs" = Darcs
          readInputType x = error $ "No such input-type " ++ show x

instance RecordCommand Cmd where
  mode_summary Import {} = "Import a git-fast-export dump into darcs."
  mode_summary Export {} = "Export a darcs repository to a git-fast-import stream."
  mode_summary CreateBridge {} = "Create a darcs bridge, importing from git or darcs."
  mode_summary Sync   {} = "Sync an existing darcs bridge."

runCmdWithMarks :: Cmd -> (Marks.Marks -> IO Marks.Marks) -> IO ()
runCmdWithMarks c = Marks.handleCmdMarks (readMarks c) (writeMarks c)

handleCmd :: Cmd -> IO ()
handleCmd c = case c of
  Import {} | create c -> case readMarks c of
               [] -> format c `seq` -- avoid late failure
                       runCmdWithMarks c (const $ fastImport stdin (liftIO . putStrLn) (repo c) (format c))
               _  -> die "cannot create repo, with existing marksfile."
            | otherwise -> runCmdWithMarks c $ fastImportIncremental stdin (liftIO . putStrLn) (repo c)
  Export {} -> runCmdWithMarks c $ fastExport (liftIO . BL.putStrLn) (repo c)
  CreateBridge {} -> case inputRepo c of
        [] -> die "missing input-repo argument."
        r  -> createBridge r (clone c)
  Sync {}   -> case bridgePath c of
        [] -> die "missing bridge-path argument."
        b  -> syncBridge False b (repoType c)

main :: IO ()
main = getArgs >>= dispatchR [] >>= handleCmd
