module Marks where

import qualified Data.IntMap as IM
import qualified Data.Map as M
import qualified Data.ByteString.Char8 as BS
import Prelude hiding ( id, lines )
import System.Directory( removeFile )

-- Patch Hash, Branch Name, Context Hash (Hash of all patches <= this mark)
type MarkContent = (BS.ByteString, BS.ByteString, BS.ByteString)
-- Mark number, patch hash, branch name.
type ContextContent = (Int, BS.ByteString, BS.ByteString)

type Marks = (IM.IntMap MarkContent, M.Map BS.ByteString ContextContent)

emptyMarks :: Marks
emptyMarks = (IM.empty, M.empty)

lastMark :: Marks -> Int
lastMark (marks, _)  = if IM.null marks then 0 else fst $ IM.findMax marks

getMark :: Marks -> IM.Key -> Maybe MarkContent
getMark (marks, _) key = IM.lookup key marks

getContext :: Marks -> BS.ByteString -> Maybe ContextContent
getContext (_, ctxMarks) key = M.lookup key ctxMarks

addMark :: Marks -> IM.Key -> MarkContent -> Marks
addMark (marks, ctxMarks) key value@(hash,bName,ctx) =
  (IM.insert key value marks, M.insert ctx (key, hash, bName) ctxMarks)

listMarks :: Marks -> [(IM.Key, MarkContent)]
listMarks (marks, _) = IM.assocs marks

lastMarkForBranch :: BS.ByteString -> Marks -> Maybe Int
lastMarkForBranch branchToSearch = doFind 0 . listMarks where
  doFind :: Int -> [(IM.Key, MarkContent)] -> Maybe Int
  doFind n [] | n <= 0 = Nothing
  doFind n [] = Just n
  doFind n ((mark, (_, branch, _)) : ms) =
    if branch == branchToSearch
      then doFind (if mark > n then mark else n) ms
      else doFind n ms

findMarkForCtx :: String -> Marks -> Maybe Int
findMarkForCtx s m = (\(mark,_,_) -> mark) `fmap` getContext m (BS.pack s)

readMarks :: FilePath -> IO Marks
readMarks p = do lines <- BS.split '\n' `fmap` BS.readFile p
                 return $ foldl merge (IM.empty, M.empty) lines
               `catch` \_ -> return emptyMarks
  where merge set@(marksSet, ctxSet) line = case BS.split ':' line of
          [id, hashBranchCtx] ->
            case BS.split ' ' . BS.dropWhile (== ' ') $ hashBranchCtx of
              [hash, branch, ctx] ->
                let mark = read $ BS.unpack id in
                ( IM.insert mark (hash, branch, ctx) marksSet
                , M.insert ctx (mark, hash, branch) ctxSet)
              _ -> set -- ignore, although it is maybe not such a great idea...
          _ -> set

writeMarks :: FilePath -> Marks -> IO ()
writeMarks fp m = do removeFile fp `catch` \_ -> return () -- unlink
                     BS.writeFile fp marks
  where marks = BS.concat $ map format $ reverse $ listMarks m
        format (k, (h, b, c)) = BS.concat
          [ BS.pack $ show k, BS.pack ": ", h, BS.pack " ", b, BS.pack " "
          , c, BS.pack "\n"]

handleCmdMarks :: FilePath -> FilePath -> (Marks -> IO Marks) -> IO ()
handleCmdMarks inFile outFile act =
  do marks <- case inFile of
       [] -> return emptyMarks
       x -> readMarks x
     marks' <- act marks
     case outFile of
       [] -> return ()
       x -> writeMarks x marks'
