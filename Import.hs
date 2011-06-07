{-# LANGUAGE DeriveDataTypeable, GADTs, ScopedTypeVariables, ExplicitForAll #-}
module Import( fastImport, fastImportIncremental, RepoFormat(..) ) where

import Prelude hiding ( readFile, lex, maybe, log )
import Data.Data
import Data.DateTime ( formatDateTime, parseDateTime, startOfTime )
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.IORef ( newIORef, modifyIORef, readIORef )

import Control.Monad ( when )
import Control.Applicative ( Alternative, (<|>) )
import Control.Monad.Trans ( liftIO )
import Control.Monad.State.Strict( gets, modify )
import Data.Maybe ( isNothing, fromMaybe )
import System.Directory ( doesFileExist, createDirectory )
import System.IO ( Handle )

import Darcs.Patch.PatchInfoAnd ( PatchInfoAnd, n2pia )
import Darcs.Flags( Compression( .. )
                  , DarcsFlag( UseHashedInventory, UseFormat2 ) )
import Darcs.Repository ( Repository, withRepoLock, RepoJob(..)
                        , readTentativeRepo, readRepo
                        , createRepository
                        , createPristineDirectoryTree
                        , finalizeRepositoryChanges
                        , cleanRepository )
import Darcs.Repository.State( readRecorded )

import Darcs.Repository.HashedRepo ( addToTentativeInventory )
import Darcs.Repository.InternalTypes ( extractCache )
import Darcs.Repository.Prefs( FileType(..) )

import Darcs.Patch ( RepoPatch, fromPrims, infopatch, adddeps, rmfile, rmdir, adddir, move )
import Darcs.Patch.V2 ( RealPatch )
import Darcs.Patch.Prim.V1 ( Prim )
import Darcs.Patch.Prim.Class ( PrimOf )
import Darcs.Patch.Depends ( getTagsRight )
import Darcs.Patch.Prim ( sortCoalesceFL )
import Darcs.Patch.Info ( PatchInfo, patchinfo )
import Darcs.Patch.Set ( newset2FL )
import Darcs.Witnesses.Ordered ( FL(..), RL(..), (+<+), reverseFL, reverseRL)
import Darcs.Witnesses.Sealed ( Sealed(..), unFreeLeft )

import Storage.Hashed.Monad hiding ( createDirectory, exists )
import qualified Storage.Hashed.Monad as TM
import qualified Storage.Hashed.Tree as T
import Storage.Hashed.Darcs
import Storage.Hashed.Hash( encodeBase16, sha256, Hash(..) )
import Storage.Hashed.Tree( Tree, treeHash, readBlob, TreeItem(..), findTree )
import Storage.Hashed.AnchoredPath( floatPath, AnchoredPath(..), Name(..)
                                  , appendPath, parents, anchorPath )
import Darcs.Diff( treeDiff )
import Darcs.Utils ( nubsort, withCurrentDirectory, treeHasDir, treeHasFile )

import Utils
import Marks

import qualified Data.Attoparsec.Char8 as A
import Data.Attoparsec.Char8( (<?>) )

data RepoFormat = Darcs2Format | HashedFormat deriving (Eq, Data, Typeable)

type Marked = Maybe Int
type Branch = B.ByteString
type AuthorInfo = B.ByteString
type Message = B.ByteString
type Content = B.ByteString

data RefId = MarkId Int | HashId B.ByteString | Inline
           deriving Show

data Object = Blob (Maybe Int) Content
            | Reset Branch (Maybe RefId)
            | Commit Branch Marked AuthorInfo Message
            | Tag Int AuthorInfo Message
            | Modify (Either Int Content) B.ByteString -- (mark or content), filename
            | Gitlink B.ByteString
            | Copy B.ByteString B.ByteString -- source/target filenames
            | Rename B.ByteString B.ByteString -- orig/new filenames
            | Delete B.ByteString -- filename
            | From Int
            | Merge Int
            | Progress B.ByteString
            | End
            deriving Show

type Ancestors = (Marked, [Int])
data State p where
    Toplevel :: Marked -> Branch -> State p
    InCommit :: Marked -> Ancestors -> Branch -> Tree IO -> RL (PrimOf p) cX cY -> PatchInfo -> State p
    Done :: State p

instance Show (State p) where
  show (Toplevel _ _) = "Toplevel"
  show (InCommit _ _ _ _ _ _) = "InCommit"
  show Done =  "Done"

fastImport :: Handle -> (String -> TreeIO ()) -> String -> RepoFormat -> IO Marks
fastImport inHandle printer outrepo fmt =
  do createDirectory outrepo
     withCurrentDirectory outrepo $ do
       createRepository $ case fmt of
         Darcs2Format -> [UseFormat2]
         HashedFormat -> [UseHashedInventory]
       withRepoLock [] $ RepoJob $ \repo -> do
         let initState = Toplevel Nothing $ BC.pack "refs/heads/master"
         marks <- fastImport' inHandle printer repo emptyMarks initState
         createPristineDirectoryTree repo "." -- this name is really confusing
         return marks

fastImportIncremental :: Handle -> (String -> TreeIO ()) -> String -> Marks -> IO Marks
fastImportIncremental inHandle printer repodir marks = withCurrentDirectory repodir $
    withRepoLock [] $ RepoJob $ \repo -> do
        -- Read the last mark we processed on a previous import, to prevent non
        -- linear history errors.
        let ancestor = case lastMark marks of
                0 -> Nothing
                n -> Just n
            initState = Toplevel ancestor $ BC.pack "refs/heads/master"
        fastImport' inHandle printer repo marks initState

fastImport' :: forall p r u . (RepoPatch p) => Handle ->
    (String -> TreeIO ()) -> Repository p r u r -> Marks -> State p -> IO Marks
fastImport' inHandle printer repo marks initial = do
    pristine <- readRecorded repo
    patches <- newset2FL `fmap` readRepo repo
    marksref <- newIORef marks
    let check :: FL (PatchInfoAnd p) x y -> [(Int, BC.ByteString)] -> IO ()
        check NilFL [] = return ()
        check (p:>:ps) ((_,h):ms) = do
          when (patchHash p /= h) $ die "FATAL: Marks do not correspond."
          check ps ms
        check _ _ = die "FATAL: Patch and mark count do not agree."

        go :: State p -> B.ByteString -> TreeIO ()
        go state rest = do (rest', item) <- parseObject inHandle printer rest
                           state' <- process state item
                           case state' of
                             Done -> return ()
                             _ -> go state' rest'

        -- sort marks into buckets, since there can be a *lot* of them
        markpath :: Int -> AnchoredPath
        markpath n = floatPath "_darcs/marks"
                        `appendPath` (Name $ BC.pack $ show (n `div` 1000))
                        `appendPath` (Name $ BC.pack $ show (n `mod` 1000))

        makeinfo author message tag = do
          let (name:log) = lines $ BC.unpack message
              (author'', date'') = span (/='>') $ BC.unpack author
              date' = dropWhile (`notElem` "0123456789") date''
              author' = author'' ++ ">"
              date = formatDateTime "%Y%m%d%H%M%S" $ fromMaybe startOfTime (parseDateTime "%s %z" date')
          liftIO $ patchinfo date (if tag then "TAG " ++ name else name) author' log

        addtag author msg =
          do info <- makeinfo author msg True
             gotany <- liftIO $ doesFileExist "_darcs/tentative_hashed_pristine"
             deps <- if gotany then liftIO $ getTagsRight `fmap` readTentativeRepo repo
                               else return []
             let ident = NilFL :: FL (RealPatch Prim) cX cX
                 patch = adddeps (infopatch info ident) deps
             liftIO $ addToTentativeInventory (extractCache repo)
                                              GzipCompression (n2pia patch)
             return ()

        -- processing items
        updateHashes = do
          let nodarcs (AnchoredPath (Name x:_)) _ = x /= BC.pack "_darcs"
              hashblobs (File blob@(T.Blob con NoHash)) =
                do hash <- sha256 `fmap` readBlob blob
                   return $ File (T.Blob con hash)
              hashblobs x = return x
          tree' <- liftIO . T.partiallyUpdateTree hashblobs nodarcs =<< gets tree
          modify $ \s -> s { tree = tree' }
          return $ T.filter nodarcs tree'

        diffCurrent (InCommit mark ancestors branch start ps info) = do
          current <- updateHashes
          Sealed diff
                <- unFreeLeft `fmap` liftIO (treeDiff (const TextFile) start current)
          return $ InCommit mark ancestors branch current (reverseFL diff +<+ ps) info
        diffCurrent _ = error "This is never valid outside of a commit."

        process :: State p -> Object -> TreeIO (State p)
        process s (Progress p) = do
          printer ("progress " ++ BC.unpack p)
          return s

        process (Toplevel _ _) End = do
          tree' <- (liftIO . darcsAddMissingHashes) =<< updateHashes
          modify $ \s -> s { tree = tree' } -- lets dump the right tree, without _darcs
          let root = encodeBase16 $ treeHash tree'
          printer "\\o/ It seems we survived. Enjoy your new repo."
          liftIO $ B.writeFile "_darcs/tentative_pristine" $ 
            BC.concat [BC.pack "pristine:", root]
          return Done

        process (Toplevel n b) (Tag what author msg) = do
          if Just what == n
             then addtag author msg
             else printer $ "WARNING: Ignoring out-of-order tag " ++
                             head (lines $ BC.unpack msg)
          return (Toplevel n b)

        process (Toplevel n _) (Reset branch from) =
          do case from of
               (Just (MarkId k)) | Just k == n ->
                 addtag (BC.pack "Anonymous Tagger <> 0 +0000") branch
               _ -> printer $ "WARNING: Ignoring out-of-order reset " ++
                                        BC.unpack branch
             return $ Toplevel n branch

        process (Toplevel n b) (Blob (Just m) bits) = do
          TM.writeFile (markpath m) (BL.fromChunks [bits])
          return $ Toplevel n b

        process x (Gitlink link) = do
          printer $ "WARNING: Ignoring gitlink " ++ BC.unpack link
          return x

        process (Toplevel previous pbranch) (Commit branch mark author message) = do
          when (pbranch /= branch) $ do
            printer ("Tagging branch: " ++ BC.unpack pbranch)
            addtag author pbranch
          info <- makeinfo author message False
          startstate <- updateHashes
          return $ InCommit mark (previous, []) branch startstate NilRL info

        process s@(InCommit _ _ _ _ _ _) (Modify (Left m) path) = do
          TM.copy (markpath m) (floatPath $ BC.unpack path)
          diffCurrent s

        process s@(InCommit _ _ _ _ _ _) (Modify (Right bits) path) = do
          TM.writeFile (floatPath $ BC.unpack path) (BL.fromChunks [bits])
          diffCurrent s

        process (InCommit mark ancestors branch _ ps info) (Delete path) = do
          let filePath = BC.unpack path
              rmPatch = rmfile filePath
          TM.unlink $ floatPath filePath
          current <- updateHashes
          return $ InCommit mark ancestors branch current (rmPatch :<: ps) info

        process (InCommit mark (prev, current) branch start ps info) (From from) =
          return $ InCommit mark (prev, from:current) branch start ps info

        process (InCommit mark (prev, current) branch start ps info) (Merge from) =
          return $ InCommit mark (prev, from:current) branch start ps info

        process s@(InCommit _ _ _ _ _ _) (Copy from to) = do
          let unpackFloat = floatPath.BC.unpack
          TM.copy (unpackFloat from) (unpackFloat to)
          -- We can't tell Darcs that a file has been copied, so it'll show as
          -- an addfile.
          diffCurrent s

        process (InCommit mark ancestors branch start ps info) (Rename from to) = do
          let uFrom = BC.unpack from
              uTo = BC.unpack to
          targetDirExists <- liftIO $ treeHasDir start uTo
          targetFileExists <- liftIO $ treeHasFile start uTo
          -- If the target exists, remove it; if it doesn't, add all its parent
          -- directories.
          preparePatchesRL <-
            if targetDirExists || targetFileExists
              then do
                TM.unlink $ floatPath uTo
                let rmPatch = if targetDirExists then rmdir uTo else rmfile uTo
                return $ rmPatch :<: NilRL
              else do
                let parentPaths = parents $ floatPath uTo
                    missing = filter (isNothing . findTree start) parentPaths
                    missingPaths = nubsort (map (anchorPath "") missing)
                return $ foldl (\dirPs dir -> adddir dir :<: dirPs) NilRL missingPaths
          let movePatches = move uFrom uTo :<: preparePatchesRL
          TM.rename (floatPath uFrom) (floatPath uTo)
          current <- updateHashes
          return $ InCommit mark ancestors branch current (movePatches +<+ ps) info

        -- When we leave the commit, create a patch for the cumulated prims.
        process (InCommit mark ancestors branch _ ps info) x = do
          case ancestors of
            (_, []) -> return () -- OK, previous commit is the ancestor
            (Just n, list)
              | n `elem` list -> return () -- OK, we base off one of the ancestors
              | otherwise -> printer $
                               "WARNING: Linearising non-linear ancestry:" ++
                               " currently at " ++ show n ++ ", ancestors " ++ show list
            (Nothing, list) ->
              printer $ "WARNING: Linearising non-linear ancestry " ++ show list

          (prims :: FL p cX cY)  <- return $ fromPrims $ sortCoalesceFL $ reverseRL ps
          let patch = infopatch info prims
          liftIO $ addToTentativeInventory (extractCache repo)
                                           GzipCompression (n2pia patch)
          case mark of
            Nothing -> return ()
            Just n -> case getMark marks n of
              Nothing -> liftIO $ modifyIORef marksref $ \m -> addMark m n (patchHash $ n2pia patch)
              Just n' -> die $ "FATAL: Mark already exists: " ++ BC.unpack n'
          process (Toplevel mark branch) x

        process state obj = do
          liftIO $ print obj
          fail $ "Unexpected object in state " ++ show state

    check patches (listMarks marks)
    hashedTreeIO (go initial B.empty) pristine "_darcs/pristine.hashed"
    finalizeRepositoryChanges repo
    cleanRepository repo
    readIORef marksref

parseObject :: Handle -> (String -> TreeIO ()) -> BC.ByteString -> TreeIO (BC.ByteString, Object)
parseObject inHandle printer = next inHandle printer object
  where object = A.parse p_object
        lex :: A.Parser b -> A.Parser b
        lex p = p >>= \x -> A.skipSpace >> return x
        lexString s = A.string (BC.pack s) >> A.skipSpace
        line = lex $ A.takeWhile (/='\n')

        maybe :: (Alternative f, Monad f) => f a -> f (Maybe a)
        maybe p = Just `fmap` p <|> return Nothing
        p_object = p_blob
                   <|> p_reset
                   <|> p_commit
                   <|> p_tag
                   <|> p_modify
                   <|> p_rename
                   <|> p_copy
                   <|> p_from
                   <|> p_merge
                   <|> p_delete
                   <|> (lexString "progress" >> Progress `fmap` line)
                   <|> (A.endOfInput >> return End)
        p_author name = lexString name >> line
        p_reset = do lexString "reset"
                     branch <- line
                     refid <- maybe $ lexString "from" >> p_refid
                     return $ Reset branch refid
        p_commit = do lexString "commit"
                      branch <- line
                      mark <- maybe p_mark
                      _ <- maybe $ p_author "author"
                      committer <- p_author "committer"
                      message <- p_data
                      return $ Commit branch mark committer message
        p_tag = do lexString "tag" >> line -- FIXME we ignore branch for now
                   lexString "from"
                   mark <- p_marked
                   author <- p_author "tagger"
                   message <- p_data
                   return $ Tag mark author message

        p_blob = do lexString "blob"
                    mark <- maybe p_mark
                    Blob mark `fmap` p_data
                  <?> "p_blob"

        p_mark :: A.Parser Int
        p_mark = do lexString "mark"
                    lex $ A.char ':'
                    lex A.decimal
                  <?> "p_mark"

        p_refid :: A.Parser RefId
        p_refid = MarkId `fmap` p_marked
                  <|> (lexString "inline" >> return Inline)
                  <|> HashId `fmap` p_hash
        p_data = do lexString "data"
                    len <- A.decimal
                    A.char '\n'
                    lex $ A.take len
                  <?> "p_data"
        p_marked = lex $ A.char ':' >> A.decimal
        p_hash = lex $ A.takeWhile1 (A.inClass "0123456789abcdefABCDEF")
        p_from = lexString "from" >> From `fmap` p_marked
        p_merge = lexString "merge" >> Merge `fmap` p_marked
        p_delete = lexString "D" >> Delete `fmap` line
        p_quotedName = lex $ do A.char '"'
                                name <- A.takeWhile (/= '"')
                                A.char '"'
                                return name
        p_copy = do lexString "C"
                    source <- p_quotedName
                    target <- p_quotedName
                    return $ Copy source target
        p_rename = do lexString "R"
                      oldName <- p_quotedName
                      newName <- p_quotedName
                      return $ Rename oldName newName
        p_modify = do lexString "M"
                      mode <- lex $ A.takeWhile (A.inClass "01234567890")
                      mark <- p_refid
                      path <- line
                      case mark of
                        HashId hash | mode == BC.pack "160000" -> return $ Gitlink hash
                                    | otherwise -> fail ":(("
                        MarkId n -> return $ Modify (Left n) path
                        Inline -> do bits <- p_data
                                     return $ Modify (Right bits) path

        next :: Handle -> (String -> TreeIO ()) -> (B.ByteString -> A.Result Object) -> B.ByteString -> TreeIO (B.ByteString, Object)
        next inHandle printer parser rest =
          do chunk <- if B.null rest then liftIO $ B.hGet inHandle (64 * 1024)
                                     else return rest
             next_chunk parser chunk
        next_chunk parser chunk =
          case parser chunk of
             A.Done rest result -> return (rest, result)
             A.Partial cont -> next inHandle printer cont B.empty
             A.Fail _ ctx err -> do
               printer $ "=== chunk ===\n" ++ BC.unpack chunk ++ "\n=== end chunk ===="
               fail $ "Error parsing stream. " ++ err ++ "\nContext: " ++ show ctx

