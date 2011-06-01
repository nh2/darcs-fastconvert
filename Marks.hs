module Marks where
import Prelude hiding ( id, lines )
import qualified Data.IntMap as M
import qualified Data.ByteString.Char8 as BS
import System.Directory( removeFile )

type Marks = M.IntMap BS.ByteString

emptyMarks :: Marks
emptyMarks = M.empty

lastMark :: Marks -> Int
lastMark m = if M.null m then 0 else fst $ M.findMax m

getMark :: Marks -> M.Key -> Maybe BS.ByteString
getMark marks key = M.lookup key marks

addMark :: Marks -> M.Key -> BS.ByteString -> Marks
addMark marks key value = M.insert key value marks

listMarks :: Marks -> [(M.Key, BS.ByteString)]
listMarks = M.assocs

readMarks :: FilePath -> IO Marks
readMarks p = do lines <- BS.split '\n' `fmap` BS.readFile p
                 return $ foldl merge M.empty lines
               `catch` \_ -> return emptyMarks
  where merge set line = case (BS.split ':' line) of
          [id, hash] -> M.insert (read $ BS.unpack id) (BS.dropWhile (== ' ') hash) set
          _ -> set -- ignore, although it is maybe not such a great idea...

writeMarks :: FilePath -> Marks -> IO ()
writeMarks fp m = do removeFile fp `catch` \_ -> return () -- unlink
                     BS.writeFile fp marks
  where marks = BS.concat $ map format $ listMarks m
        format (k, s) = BS.concat [BS.pack $ show k, BS.pack ": ", s, BS.pack "\n"]

handleCmdMarks :: FilePath -> FilePath -> (Marks -> IO Marks) -> IO ()
handleCmdMarks inFile outFile act = do
  do marks <- case inFile of
       [] -> return emptyMarks
       x -> readMarks x
     marks' <- act marks
     case outFile of
       [] -> return ()
       x -> writeMarks x marks'
