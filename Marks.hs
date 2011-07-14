module Marks where

import qualified Data.IntMap as M
import qualified Data.ByteString.Char8 as BS
import Prelude hiding ( id, lines )
import System.Directory( removeFile )

type MarkContent = (BS.ByteString, BS.ByteString)
type Marks = M.IntMap MarkContent

emptyMarks :: Marks
emptyMarks = M.empty

lastMark :: Marks -> Int
lastMark m = if M.null m then 0 else fst $ M.findMax m

getMark :: Marks -> M.Key -> Maybe MarkContent
getMark marks key = M.lookup key marks

addMark :: Marks -> M.Key -> MarkContent -> Marks
addMark marks key value = M.insert key value marks

listMarks :: Marks -> [(M.Key, MarkContent)]
listMarks = M.assocs

lastMarkForBranch :: BS.ByteString -> Marks -> Maybe Int
lastMarkForBranch branchToSearch = doFind 0 . listMarks where
  doFind :: Int -> [(M.Key, MarkContent)] -> Maybe Int
  doFind n [] | n <= 0 = Nothing
  doFind n [] = Just n
  doFind n ((mark, (_, branch)) : ms) =
    if branch == branchToSearch
      then doFind (if mark > n then mark else n) ms
      else doFind n ms

readMarks :: FilePath -> IO Marks
readMarks p = do lines <- BS.split '\n' `fmap` BS.readFile p
                 return $ foldl merge M.empty lines
               `catch` \_ -> return emptyMarks
  where merge set line = case BS.split ':' line of
          [id, hashBranch] ->
            case BS.split ' ' . BS.dropWhile (== ' ') $ hashBranch of
              [hash, branch] ->
                M.insert (read $ BS.unpack id) (hash, branch) set
              _ -> set -- ignore, although it is maybe not such a great idea...
          _ -> set

writeMarks :: FilePath -> Marks -> IO ()
writeMarks fp m = do removeFile fp `catch` \_ -> return () -- unlink
                     BS.writeFile fp marks
  where marks = BS.concat $ map format $ reverse $ listMarks m
        format (k, (h, b)) = BS.concat
          [BS.pack $ show k, BS.pack ": ", h, BS.pack " ", b, BS.pack "\n"]

handleCmdMarks :: FilePath -> FilePath -> (Marks -> IO Marks) -> IO ()
handleCmdMarks inFile outFile act =
  do marks <- case inFile of
       [] -> return emptyMarks
       x -> readMarks x
     marks' <- act marks
     case outFile of
       [] -> return ()
       x -> writeMarks x marks'
