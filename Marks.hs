module Marks where

import qualified Data.IntMap as IM
import Data.List ( find )
import qualified Data.ByteString.Char8 as BS
import Prelude hiding ( id, lines )
import System.Directory( removeFile )

-- Patch Hash, Branch Name, Context Hash (Hash of all patches <= this mark)
type MarkContent = (BS.ByteString, BS.ByteString, BS.ByteString)
type Marks = IM.IntMap MarkContent

emptyMarks :: Marks
emptyMarks = IM.empty

lastMark :: Marks -> Int
lastMark m = if IM.null m then 0 else fst $ IM.findMax m

getMark :: Marks -> IM.Key -> Maybe MarkContent
getMark marks key = IM.lookup key marks

addMark :: Marks -> IM.Key -> MarkContent -> Marks
addMark marks key value = IM.insert key value marks

listMarks :: Marks -> [(IM.Key, MarkContent)]
listMarks = IM.assocs

lastMarkForBranch :: BS.ByteString -> Marks -> Maybe Int
lastMarkForBranch branchToSearch = doFind 0 . listMarks where
  doFind :: Int -> [(IM.Key, MarkContent)] -> Maybe Int
  doFind n [] | n <= 0 = Nothing
  doFind n [] = Just n
  doFind n ((mark, (_, branch, _)) : ms) =
    if branch == branchToSearch
      then doFind (if mark > n then mark else n) ms
      else doFind n ms

-- TODO: This is no doubt slow. Use 2 maps for faster lookup?
findMarkForCtx :: String -> Marks -> Maybe Int
findMarkForCtx = findMarkForCtx' . BS.pack where
  findMarkForCtx' ctx = fmap fst . find (\(_, (_,_,c)) -> c == ctx) . listMarks

readMarks :: FilePath -> IO Marks
readMarks p = do lines <- BS.split '\n' `fmap` BS.readFile p
                 return $ foldl merge IM.empty lines
               `catch` \_ -> return emptyMarks
  where merge set line = case BS.split ':' line of
          [id, hashBranchCtx] ->
            case BS.split ' ' . BS.dropWhile (== ' ') $ hashBranchCtx of
              [hash, branchCtx] ->
                case BS.split ' ' . BS.dropWhile (== ' ') $ branchCtx of
                  [branch, ctx] ->
                    IM.insert (read $ BS.unpack id) (hash, branch, ctx) set
                  _ -> set
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
