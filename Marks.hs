module Marks where
import qualified Data.Map as M
import qualified Data.ByteString.Char8 as BS

type Marks = M.Map Int BS.ByteString

emptyMarks = M.empty
lastMark m = if M.null m then 0 else fst $ M.findMax m

readMarks :: FilePath -> IO Marks
readMarks p = do lines <- BS.split '\n' `fmap` BS.readFile p
                 return $ foldl merge M.empty lines
               `catch` \_ -> return emptyMarks
  where merge set line = case (BS.split ':' line) of
          [id, hash] -> M.insert (read $ BS.unpack id) (BS.dropWhile (== ' ') hash) set
          _ -> set -- ignore, although it is maybe not such a great idea...
