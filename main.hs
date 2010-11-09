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

handleMarks cmd act = do
  do marks <- case readMarks cmd of
       [] -> return Marks.emptyMarks
       x -> Marks.readMarks x
     marks' <- act marks
     case writeMarks cmd of
       [] -> return ()
       x -> Marks.writeMarks x marks'

main = getArgs >>= dispatchR [] undefined >>= \x -> case x of
  Import {} | create x && null (readMarks x) -> case readMarks x of
    [] -> (format x) `seq` -- avoid late failure
            handleMarks x (const $ fastImport (repo x) (format x))
            | otherwise -> handleMarks x $ fastImportIncremental (repo x)
  Export {} -> handleMarks x $ fastExport (repo x)
