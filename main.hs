{-# LANGUAGE DeriveDataTypeable #-}
import Import
import Export
import System.Console.CmdLib
import System.Environment

data Cmd = Import { repo :: String }
         | Export { repo :: String }
         deriving (Eq, Typeable, Data)

instance Attributes Cmd where
  attributes _ = repo %> [ Positional 0 ]

instance RecordCommand Cmd where
  mode_summary Import {} = "Import a git-fast-export dump into darcs."
  mode_summary Export {} = "Export a darcs repository to a git-fast-import stream."
main = getArgs >>= dispatchR [] undefined >>= \x -> case x of
  Import {} -> fastImport (repo x)
  Export {} -> fastExport (repo x)
