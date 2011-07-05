{-# LANGUAGE ScopedTypeVariables, FlexibleContexts #-}
module Patch (readAndApplyGitEmail) where

import Utils

import Control.Applicative ( Alternative, (<|>) )
import Control.Monad ( unless, when )
import Control.Monad.Trans ( liftIO )
import qualified Data.Attoparsec.Char8 as A
import Data.Attoparsec.Combinator( many )
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.DateTime ( formatDateTime, parseDateTime, DateTime )
import Data.List ( inits )
import Data.Maybe ( fromJust )
import Prelude hiding ( lex )
import System.Directory ( doesFileExist, doesDirectoryExist )
import System.IO ( stdin )
import System.FilePath ( splitPath, joinPath, takeDirectory, isPathSeparator )
import SHA1 ( sha1PS )

import Darcs.Flags( Compression(..) )
import Darcs.Patch ( RepoPatch, fromPrims, infopatch, apply )
import Darcs.Patch.Info ( patchinfo, PatchInfo )
import Darcs.Patch.PatchInfoAnd ( n2pia )
import Darcs.Patch.Prim ( sortCoalesceFL, hunk, addfile, rmfile, adddir )
import Darcs.Patch.Prim.Class ( PrimOf )
import Darcs.Repository ( withRepoLock, RepoJob(..), Repository
                        , finalizeRepositoryChanges, tentativelyAddPatch )
import Darcs.SignalHandler ( withSignalsBlocked )
import Darcs.Utils ( withCurrentDirectory )
import Darcs.Witnesses.Ordered ( FL(..), (+>+) )
import Darcs.Witnesses.Sealed ( Sealed(..), joinGap, emptyGap, freeGap
                              , unFreeLeft, FreeLeft )

type Author = B.ByteString
type Message = [B.ByteString]

newtype GitHash = GitHash String

instance Show GitHash where
  show (GitHash h) = case length h of
    40 -> h
    n  -> h ++ replicate (40 - n) 'X'

-- | @eqOrX@ supports Git's shortest unique prefixes of Hashes. This could
-- allow arbitrary patch application, since Git does not know the state of our
-- files (however, --full-index does as it suggests for git-format-patch).
eqOrX :: Char -> Char -> Bool
eqOrX a b | a == b = True
eqOrX 'X' _        = True
eqOrX _ 'X'        = True
eqOrX _ _          = False

instance Eq GitHash where
  g1 == g2 = and $ zipWith eqOrX (show g1) (show g2)

data HunkChange = HunkChange Int [B.ByteString] [B.ByteString] deriving Show

data Change = AddFile FilePath [B.ByteString]
            | RmFile FilePath [B.ByteString]
            | Hunk FilePath GitHash [HunkChange]
  deriving Show

data GitPatch = GitPatch Author DateTime Message [Change]
  deriving Show

data HunkLine = ContextLine B.ByteString
              | AddedLine B.ByteString
              | RemovedLine B.ByteString
  deriving Show

readAndApplyGitEmail :: String -> IO ()
readAndApplyGitEmail repoPath = withCurrentDirectory repoPath $ do
  putStrLn "Attempting to parse input."
  ps <- parseGitEmail
  putStrLn $ "Successfully parsed " ++ (show . length $ ps) ++ " patches."
  putStrLn "Attempting to apply patches."
  mapM_ applyGitPatch ps
  putStrLn "Succesfully applied patches."

applyGitPatch :: GitPatch -> IO ()
applyGitPatch (GitPatch author date message changes) = do
  let (name:descr) = map BC.unpack message
      dateStr = formatDateTime "%Y%m%d%H%M%S" date
  info <- patchinfo dateStr name (BC.unpack author) descr
  withRepoLock [] $ RepoJob $ applyChanges info changes

applyChanges :: forall p r u . (RepoPatch p) => PatchInfo -> [Change]
  -> Repository p r u r -> IO ()
applyChanges info changes repo = do
  (Sealed ps) <- unFreeLeft `fmap` changesToPrims changes
  (prims :: FL p r x) <- return . fromPrims . sortCoalesceFL $ ps
  let patch = infopatch info prims
  _ <- tentativelyAddPatch repo GzipCompression (n2pia patch)
  withSignalsBlocked $ do
    finalizeRepositoryChanges repo
    apply ps
  where
  changesToPrims :: [Change] -> IO (FreeLeft (FL (PrimOf p)))
  changesToPrims [] = return $ emptyGap NilFL
  changesToPrims (c:cs) = do
    cPrims <- case c of
      (AddFile fp new) -> do
        parentDirAdds <- addParentDirs fp
        let initFile = freeGap $ addfile fp :>: hunk fp 1 [] new :>: NilFL
        return $ joinGap (+>+) parentDirAdds initFile
      (RmFile fp old) -> return $
        freeGap $ hunk fp 1 old [] :>: rmfile fp :>: NilFL
      (Hunk fp hash hunkChanges) -> do
        testHunkHash fp hash
        let tohunks (HunkChange line old new) =
              joinGap (:>:) (freeGap $ hunk fp line old new)
        return $ foldr tohunks (emptyGap NilFL) hunkChanges
    csPrims <- changesToPrims cs
    return $ joinGap (+>+) cPrims csPrims

  addParentDirs :: FilePath -> IO (FreeLeft (FL (PrimOf p)))
  addParentDirs fp = do
    dirs <- dropWhileDirsExist $ getParents fp
    let joiner d = joinGap (:>:) (freeGap $ adddir d)
    return $ foldr joiner (emptyGap NilFL) dirs

  getParents = tail . map joinPath . inits . splitPath . takeDirectory

  dropWhileDirsExist :: [FilePath] -> IO [FilePath]
  dropWhileDirsExist [] = return []
  dropWhileDirsExist ds'@(d : ds) = do
    exists <- doesDirectoryExist d
    if exists then dropWhileDirsExist ds else return ds'

  testHunkHash fp expectedHash = do
    actualHash <- gitHashFile fp
    -- TODO: Warn and offer to apply anyway...
    when (actualHash /= expectedHash) $ die . unwords $
      ["invalid hash of", fp, "expected:", show expectedHash
      , "actual: ", show actualHash]

-- |@gitHashFile@ calculates the Git hash of a given file's current state.
gitHashFile :: FilePath -> IO GitHash
gitHashFile fp = do
  exists <- doesFileExist fp
  if exists
    then do
      fileContents <- BL.readFile fp
      let len = BL.length fileContents
          header = BLC.pack $ "blob " ++ show len ++ "\0"
          toHash = B.concat . BL.toChunks $ header `BL.append` fileContents
      return.GitHash $ sha1PS toHash
    else return missingFileHash

missingFileHash, emptyFileHash :: GitHash
missingFileHash = GitHash $ replicate 40 '0'
emptyFileHash = GitHash "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"

parseGitEmail :: IO [GitPatch]
parseGitEmail = go (A.parse $ many p_gitPatch) BC.empty where
  lex :: A.Parser b -> A.Parser b
  lex p = p >>= \x -> A.skipSpace >> return x

  lexString s = lex $ A.string (BC.pack s)
  line = lex $ A.takeWhile (/= '\n')

  -- | @toEOL@ will consume to the end of the line, but not any leading
  -- whitespace on the following line, unline @line@.
  toEOL = A.takeWhile (/= '\n') >>= \x -> A.char '\n' >> return x

  optional :: (Alternative f, Monad f) => f a -> f (Maybe a)
  optional p = Just `fmap` p <|> return Nothing

  p_gitPatch = do
    p_commitHeader
    author <- p_author
    date <- p_date
    msg <- p_commitMsg
    _ <- specialLineDelimited BC.empty id -- skip the diff summary
    diffs <- many p_diff
    _ <- p_endMarker
    return $ GitPatch author date msg diffs

  p_endMarker = do
    lexString "--" >> toEOL
    line -- This line contains a git version number.

  p_commitHeader = do
    lexString "From"
    p_hash
    lexString "Mon Sep 17 00:00:00 2001"

  p_author = lexString "From:" >> line

  p_date = do
    lexString "Date:"
    dateStr <- BC.unpack `fmap` line
    let mbDate = parseDateTime "%a, %e %b %Y %k:%M:%S %z" dateStr
    case mbDate of
      Nothing -> error $ "Unexpected dateformat: " ++ dateStr
      Just date' -> return date'

  p_commitMsg = do
    lexString "Subject:"
    p_logLines

  p_logLines = specialLineDelimited (BC.pack "---") reverse

  specialLineDelimited = specialLineDelimited' [] where
  specialLineDelimited' ls endCase endMod = do
    perhaps <- toEOL
    if perhaps == endCase
      then return . endMod $ ls
      else specialLineDelimited' (perhaps : ls) endCase endMod

  p_hash = (GitHash . BC.unpack) `fmap` lex (A.takeWhile1
    (A.inClass "0-9a-fA-F"))

  p_diff = do
    fn <- BC.unpack `fmap` p_diffHeader
    -- TODO: handle new file modes?
    mbAddRemove <- optional $ p_addFile <|> p_removeFile
    (oldIndex, newIndex, _) <- p_indexDiff
    case mbAddRemove of
      Nothing -> Hunk fn oldIndex `fmap` p_unifiedDiff
      Just (addRem, _) -> if addRem == BC.pack "new"
        then if newIndex == emptyFileHash
               then return $ AddFile fn []
               else p_allAddLines >>= \ls -> return $ AddFile fn ls
        else p_allRemLines >>= \ls -> return $ RmFile fn ls

  p_addFile = p_addRemoveFile "new"
  p_removeFile = p_addRemoveFile "deleted"

  p_addRemoveFile x = do
    x' <- lexString x
    lexString "file mode"
    mode <- p_mode
    return (x', mode)

  p_unifiedFiles = lexString "---" >> line >> lexString "+++" >> line

  p_unifiedDiff = do
    p_unifiedFiles
    many p_unifiedHunk

  p_unifiedHunk = do
    lexString "@@ -"
    (_, oldLength) <- p_linePosLengthPair
    A.char '+'
    (newPos, newLength) <- p_linePosLengthPair
    toEOL
    (oldLines, newLines) <- p_diffLines oldLength newLength
    checkLength oldLength oldLines
    checkLength newLength newLines
    -- We use new pos, since old pos assumes that no other hunks have
    -- been applied to the original file, whereas we want our changes to
    -- be cumulative.
    return . minimiseHunkChange $ HunkChange newPos oldLines newLines
    where
    checkLength expectedLen list = let realLen = length list in
      unless (realLen == expectedLen) $
        error . unwords $ ["Malformed diff: expected length"
                          , show expectedLen , "got" , show realLen, "in"
                          , show list]

    -- |@minimiseHunkChange@ removes any leading equal lines
    -- (incrementing the line number appropriately) and any trailing
    -- equal lines.
    minimiseHunkChange x@(HunkChange _ _ _) = HunkChange p' o'' n'' where
      x'@(HunkChange p' _ _) = dropWhileEqual x

      (HunkChange _ o'' n'') =
        reverseHunkChange . dropWhileEqual . reverseHunkChange $ x'

      reverseHunkChange (HunkChange p o n) =
        HunkChange p (reverse o) (reverse n)

      dropWhileEqual orig@(HunkChange _ [] _) = orig
      dropWhileEqual orig@(HunkChange _ _ []) = orig
      dropWhileEqual orig@(HunkChange n (p : ps) (q : qs)) = if p == q
        then dropWhileEqual $ HunkChange (n + 1) ps qs
        else orig

  p_diffLines oldCount newCount =
    p_diffLines' (oldCount, newCount) ([], []) where
      p_diffLines' (0, 0) (olds, news) =
        return (reverse olds, reverse news)
      p_diffLines' (o, n) (olds, news) = do
        l <- p_addLine <|> p_removeLine <|> p_contextLine
        case l of
          (ContextLine l') ->
            p_diffLines' (o - 1, n - 1) (l' : olds, l' : news)
          (AddedLine l') -> p_diffLines' (o, n - 1) (olds, l' : news)
          (RemovedLine l') -> p_diffLines' (o - 1, n) (l' : olds, news)

      p_addLine = A.char '+' >> AddedLine `fmap` toEOL
      p_removeLine = A.char '-' >> RemovedLine `fmap` toEOL
      p_contextLine = A.char ' ' >> ContextLine `fmap` toEOL

  p_linePosLengthPair = do
    l <- p_readLineInt
    A.char ','
    s <- lex p_readLineInt
    -- If the chunksize is 0, then we need to increment the offset
    return (if s == 0 then l + 1 else l, s)

  p_readLineInt =
    (fst . fromJust . BC.readInt) `fmap` A.takeWhile (A.inClass "0-9")

  p_allAddLines = do
    [HunkChange _ [] new] <- p_unifiedDiff
    return new

  p_allRemLines = do
    [HunkChange _ old []] <- p_unifiedDiff
    return old

  p_diffHeader = do
    lexString "diff --git"
    (oldName, _) <- splitNames `fmap` line
    return . BC.tail . BC.dropWhile (not . isPathSeparator) $ oldName
    where
    -- Something of the form: prefixA/foo prefixB/foo where foo may
    -- contain spaces (so we have to split at half way).
    splitNames l = let (a,b) = BC.splitAt (BC.length l `div` 2) l
      in (a, BC.tail b)

  p_indexDiff = do
    lexString "index"
    oldHash <- p_hash
    lexString ".."
    newHash <- p_hash
    mbMode <- optional p_mode
    return (oldHash, newHash, mbMode)

  p_mode = lex $ A.takeWhile (A.inClass "0-9")

  go :: (B.ByteString -> A.Result [GitPatch]) -> B.ByteString -> IO [GitPatch]
  go parser rest = do
    chunk <- if B.null rest then liftIO $ B.hGet stdin (64 * 1024)
                            else return rest
    go_chunk parser chunk
  go_chunk parser chunk =
    case parser chunk of
      A.Done _ result -> return result
      A.Partial cont -> go cont B.empty
      A.Fail _ ctx err -> do
        let ch = "\n=== chunk ===\n" ++ BC.unpack chunk ++
              "\n=== end chunk ===="
        fail $ unwords ["Error parsing stream.", err, ch,  "\nContext:"
                       , show ctx]
