{-# LANGUAGE ScopedTypeVariables #-}
module Marks where

import Control.Exception
import qualified Data.IntMap as IM
import qualified Data.Map as M
import qualified Data.ByteString.Char8 as BS
import Data.List ( find, foldl' )
import Prelude hiding ( id, lines )
import System.Directory( removeFile )

-- Patch Hash, Branch Name, Context Hash (Hash of all patches <= this mark)
type MarkContent = (BS.ByteString, BS.ByteString, BS.ByteString)
-- Mark number, patch hash, branch name.
type ContextContent = (Int, BS.ByteString, BS.ByteString)

-- Provide a 2-way map between mark and markcontent and context and context
-- content.
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

listBranches :: Marks -> M.Map BS.ByteString Int
listBranches =
  (foldl' doInsert M.empty) . extractBranches . reverse . listMarks where
  doInsert bs (m, b) = if b `M.member` bs then bs else M.insert b m bs
  extractBranches = map (\(m, (_, b, _)) -> (m, b))

lastMarkForBranch :: BS.ByteString -> Marks -> Maybe Int
lastMarkForBranch branchToFind marks = fst `fmap` branchEntry
 where branchEntry = find isRightBranch . reverse . listMarks $ marks
       isRightBranch (_, (_, branch, _)) = branch == branchToFind

findMarkForCtx :: String -> Marks -> Maybe Int
findMarkForCtx s m = (\(mark,_,_) -> mark) `fmap` getContext m (BS.pack s)

readMarks :: FilePath -> IO Marks
readMarks p = do lines <- BS.split '\n' `fmap` BS.readFile p
                 return $ foldl merge (IM.empty, M.empty) lines
               `catch` \(_ :: SomeException) -> return emptyMarks
  where merge set@(marksSet, ctxSet) line = case BS.split ' ' line of
              [id, hash, branch, ctx] ->
                let mark = read . BS.unpack $ BS.drop 1 id in
                ( IM.insert mark (hash, branch, ctx) marksSet
                , M.insert ctx (mark, hash, branch) ctxSet)
              _ -> set -- ignore, although it is maybe not such a great idea...

writeMarks :: FilePath -> Marks -> IO ()
writeMarks fp m = do removeFile fp `catch` \(_ :: SomeException) -> return () -- unlink
                     BS.writeFile fp marks
  where marks = BS.concat $ map format $ reverse $ listMarks m
        format (k, (h, b, c)) = BS.concat
          [ BS.pack ":", BS.pack $ show k, BS.pack " ", h, BS.pack " ", b
          , BS.pack " ", c, BS.pack "\n"]

handleCmdMarks :: FilePath -> FilePath -> (Marks -> IO Marks) -> IO ()
handleCmdMarks inFile outFile act =
  do marks <- case inFile of
       [] -> return emptyMarks
       x -> readMarks x
     marks' <- act marks
     case outFile of
       [] -> return ()
       x -> writeMarks x marks'
