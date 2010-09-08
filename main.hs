{-# LANGUAGE DeriveDataTypeable #-}
import Import
import Export
import System.Console.CmdLib

data Cmd = Import { repo :: String, format :: RepoFormat }
         | Export { repo :: String }
         deriving (Eq, Typeable, Data)

instance Attributes Cmd where
  attributes _ = repo %> [ Positional 0 ] %%
                 format %> [ Help "Repository type to create: darcs-2 (default) or hashed."
                           , Default Darcs2Format ]
  readFlag _ = readCommon <+< readFormat
    where readFormat "darcs-2" = Darcs2Format
          readFormat "hashed" = HashedFormat
          readFormat x = error $ "No such repository format " ++ show x

instance RecordCommand Cmd where
  mode_summary Import {} = "Import a git-fast-export dump into darcs."
  mode_summary Export {} = "Export a darcs repository to a git-fast-import stream."
main = getArgs >>= dispatchR [] undefined >>= \x -> case x of
  Import {} -> (format x) `seq` fastImport (repo x) (format x) -- XXX hack
  Export {} -> fastExport (repo x)
