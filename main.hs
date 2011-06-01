{-# LANGUAGE DeriveDataTypeable #-}
import qualified Marks
import Import
import Export
import System.Console.CmdLib

data Cmd = Import { repo :: String
                  , format :: RepoFormat
                  , create :: Bool
                  , readMarks :: FilePath
                  , writeMarks :: FilePath }
         | Export { repo :: String
                  , readMarks :: FilePath
                  , writeMarks :: FilePath }
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
                    , ArgHelp "FILE" ] ]

  readFlag _ = readCommon <+< readFormat
    where readFormat "darcs-2" = Darcs2Format
          readFormat "hashed" = HashedFormat
          readFormat x = error $ "No such repository format " ++ show x

instance RecordCommand Cmd where
  mode_summary Import {} = "Import a git-fast-export dump into darcs."
  mode_summary Export {} = "Export a darcs repository to a git-fast-import stream."

runCmdWithMarks :: Cmd -> (Marks.Marks -> IO Marks.Marks) -> IO ()
runCmdWithMarks c act = Marks.handleCmdMarks (readMarks c) (writeMarks c) act

handleCmd :: Cmd -> IO ()
handleCmd c = case c of
  Import {} | create c -> case readMarks c of
               [] -> (format c) `seq` -- avoid late failure
                       runCmdWithMarks c (const $ fastImport (repo c) (format c))
               _  -> die "cannot create repo, with existing marksfile."
            | otherwise -> runCmdWithMarks c $ fastImportIncremental (repo c)
  Export {} -> runCmdWithMarks c $ fastExport (repo c)

main :: IO ()
main = getArgs >>= dispatchR [] >>= handleCmd
