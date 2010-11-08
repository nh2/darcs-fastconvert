module Marks where
import qualified Data.IntMap as M
import qualified Data.ByteString.Char8 as BS
import System.Directory( removeFile )

type Marks = M.IntMap BS.ByteString

emptyMarks = M.empty
lastMark m = if M.null m then 0 else fst $ M.findMax m

getMark marks key = M.lookup key marks
addMark marks key value = M.insert key value marks

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
  where marks = BS.concat $ map format $ M.assocs m
        format (k, s) = BS.concat [BS.pack $ show k, BS.pack ": ", s, BS.pack "\n"]
