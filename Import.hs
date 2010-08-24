module Import( fastImport ) where

import Prelude hiding ( readFile )
import System.Directory ( setCurrentDirectory, doesDirectoryExist, doesFileExist,
                   createDirectory, createDirectoryIfMissing )
import Workaround ( getCurrentDirectory )
import Control.Monad ( when, forM_, unless )
import Control.Applicative ( (<|>) )
import Control.Monad.Trans ( liftIO )
import Control.Monad.State.Strict( gets, modify )
import Control.Exception( finally )
import Data.Maybe ( catMaybes, fromJust )
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as BL

import Darcs.Hopefully ( PatchInfoAnd, n2pia, info, hopefully )
import Darcs.Commands ( DarcsCommand(..), nodefaults, putInfo, putVerbose )
import Darcs.Flags( Compression( .. ) )
import Darcs.Repository ( Repository, withRepoLock, ($-), withRepositoryDirectory, readRepo,
                          readTentativeRepo,
                          createRepository, invalidateIndex,
                          optimizeInventory,
                          tentativelyMergePatches, patchSetToPatches,
                          createPristineDirectoryTree,
                          revertRepositoryChanges, finalizeRepositoryChanges,
                          applyToWorking, setScriptsExecutable, withRepository,
                        cleanRepository )
import Darcs.Repository.Cache ( HashedDir( HashedPristineDir ), Cache(..) )
import Darcs.Repository.HashedRepo ( readHashedPristineRoot, addToTentativeInventory )
import Darcs.Repository.HashedIO ( cleanHashdir )
import Darcs.Repository.InternalTypes ( extractCache )
import Darcs.Repository.Prefs( FileType(..) )
import Darcs.Global ( darcsdir )
import Darcs.Patch ( RealPatch, Patch, Named, showPatch, patch2patchinfo, fromPrims, infopatch,
                     modernizePatch,
                     adddeps, getdeps, effect, flattenFL, isMerger, patchcontents,
                     listTouchedFiles, apply, RepoPatch, identity )
import Darcs.Patch.Depends ( getTagsRight )
import Darcs.Patch.Prim ( canonizeFL, sortCoalesceFL, Prim )
import Darcs.Witnesses.Ordered ( FL(..), RL(..), EqCheck(..), (=/\=), bunchFL, mapFL, mapFL_FL,
                                 concatFL, mapRL, lengthFL, nullFL )
import Darcs.Patch.Info ( piRename, piTag, isTag, PatchInfo, piAuthor, piName, piLog, piDate
                        , patchinfo )
import Darcs.Patch.Commute ( publicUnravel )
import Darcs.Patch.Real ( mergeUnravelled )
import Darcs.Patch.Set ( PatchSet(..), Tagged(..), newset2RL, newset2FL )
import Darcs.RepoPath ( ioAbsoluteOrRemote, toPath )
import Darcs.Repository.Format(identifyRepoFormat, formatHas, RepoProperty(Darcs2))
import Darcs.Repository.Motd ( showMotd )
import Darcs.Utils ( clarifyErrors, askUser, catchall, withCurrentDirectory )
import Darcs.ProgressPatches ( progressFL )
import Darcs.Witnesses.Sealed ( FlippedSeal(..), Sealed(..), unFreeLeft, unseal )
import Printer ( text, ($$) )
import Darcs.ColorPrinter ( traceDoc )
import Darcs.Lock ( writeBinFile )
import Darcs.External
import System.FilePath.Posix
import System.Time ( toClockTime )
import Data.DateTime ( formatDateTime, parseDateTime, fromClockTime, startOfTime )
import System.IO ( stdin )

import Storage.Hashed.Monad hiding ( createDirectory, exists )
import qualified Storage.Hashed.Monad as TM
import qualified Storage.Hashed.Tree as T
import Storage.Hashed.Darcs
import Storage.Hashed.Hash( encodeBase16, sha256, Hash(..) )
import Storage.Hashed.Tree( emptyTree, listImmediate, findTree, Tree
                          , treeHash, readBlob, TreeItem(..) )
import Storage.Hashed.AnchoredPath( floatPath, AnchoredPath(..), Name(..), anchorPath, appendPath )
import Darcs.Diff( treeDiff )

import qualified Data.Attoparsec.Char8 as A
import Data.Attoparsec.Char8( (<?>) )

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
            | Delete B.ByteString -- filename
            | From Int
            | Merge Int
            | Progress B.ByteString
            | End
            deriving Show

type Ancestors = (Marked, [Int])
data State = Toplevel Marked Branch
           | InCommit Marked Ancestors Branch (Tree IO) PatchInfo
           | Done

instance Show State where
  show (Toplevel _ _) = "Toplevel"
  show (InCommit _ _ _ _ _) = "InCommit"

fastImport :: String -> IO ()
fastImport outrepo =
  do createDirectory outrepo
     setCurrentDirectory outrepo
     createRepository []
     withRepository [] $- \repo -> do
       fastImport' repo
       cleanRepository repo
       finalizeRepositoryChanges repo
       createPristineDirectoryTree repo "." -- this name is really confusing

fastImport' repo =
  do hashedTreeIO (go startstate B.empty) emptyTree "_darcs/pristine.hashed"
     return ()
  where startstate = Toplevel Nothing $ BC.pack "refs/branches/master"
        go :: State -> B.ByteString -> TreeIO ()
        go state rest = do (rest', item) <- next object rest
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
              date = formatDateTime "%Y%m%d%H%M%S" $ case (parseDateTime "%s %z" date') of
                Just x -> x
                Nothing -> startOfTime
          liftIO $ patchinfo date (if tag then "TAG " ++ name else name) author' log

        addtag author msg =
          do info <- makeinfo author msg True
             gotany <- liftIO $ doesFileExist $ darcsdir </> "tentative_hashed_pristine"
             deps <- if gotany then liftIO $ getTagsRight `fmap` readTentativeRepo repo
                               else return []
             let ident = identity :: FL RealPatch cX cX
                 patch = adddeps (infopatch info ident) deps
             liftIO $ addToTentativeInventory (extractCache repo)
                                              GzipCompression (n2pia patch)
             return ()

        -- processing items
        updateHashes = do
          let nodarcs = (\(AnchoredPath (Name x:_)) _ -> x /= BC.pack "_darcs")
              hashblobs (File blob@(T.Blob con NoHash)) =
                do hash <- sha256 `fmap` readBlob blob
                   return $ File (T.Blob con hash)
              hashblobs x = return x
          tree' <- liftIO . T.partiallyUpdateTree hashblobs nodarcs =<< gets tree
          modify $ \s -> s { tree = tree' }
          return $ T.filter nodarcs tree'

        process :: State -> Object -> TreeIO State
        process s (Progress p) = do
          liftIO $ putStrLn ("progress " ++ BC.unpack p)
          return s

        process (Toplevel _ _) End = do
          tree' <- (liftIO . darcsAddMissingHashes) =<< updateHashes
          modify $ \s -> s { tree = tree' } -- lets dump the right tree, without _darcs
          let root = encodeBase16 $ treeHash tree'
          liftIO $ do
            putStrLn $ "\\o/ It seems we survived. Enjoy your new repo."
            B.writeFile (darcsdir </> "tentative_pristine") $
              BC.concat [BC.pack "pristine:", root]
          return Done

        process (Toplevel n b) (Tag what author msg) = do
          if Just what == n
             then addtag author msg
             else liftIO $ putStrLn $ "WARNING: Ignoring out-of-order tag " ++
                             (head $ lines $ BC.unpack msg)
          return (Toplevel n b)

        process (Toplevel n b) (Reset branch from) =
          do case from of
               (Just (MarkId k)) | Just k == n -> addtag (BC.pack "Anonymous Tagger <> 0 +0000") branch
               _ -> liftIO $ putStrLn $ "WARNING: Ignoring out-of-order tag " ++ BC.unpack branch
             return $ Toplevel n branch

        process (Toplevel n b) (Blob (Just m) bits) = do
          TM.writeFile (markpath m) $ (BL.fromChunks [bits])
          return $ Toplevel n b

        process x (Gitlink link) = do
          liftIO $ putStrLn $ "WARNING: Ignoring gitlink " ++ BC.unpack link
          return x

        process (Toplevel last pbranch) (Commit branch mark author message) = do
          when (pbranch /= branch) $ do
            liftIO $ putStrLn ("Tagging branch: " ++ BC.unpack pbranch)
            addtag author pbranch
          info <- makeinfo author message False
          startstate <- updateHashes
          return $ InCommit mark (last, []) branch startstate info

        process s@(InCommit _ _ _ _ _) (Modify (Left m) path) = do
          TM.copy (markpath m) (floatPath $ BC.unpack path)
          return s

        process s@(InCommit _ _ _ _ _) (Modify (Right bits) path) = do
          TM.writeFile (floatPath $ BC.unpack path) (BL.fromChunks [bits])
          return s

        process s@(InCommit _ _ _ _ _) (Delete path) = do
          TM.unlink (floatPath $ BC.unpack path)
          return s

        process s@(InCommit mark (prev, current) branch start info) (From from) = do
          return $ InCommit mark (prev, from:current) branch start info

        process s@(InCommit mark (prev, current) branch start info) (Merge from) = do
          return $ InCommit mark (prev, from:current) branch start info

        process s@(InCommit mark ancestors branch start info) x = do
          case ancestors of
            (_, []) -> return () -- OK, previous commit is the ancestor
            (Just n, list)
              | n `elem` list -> return () -- OK, we base off one of the ancestors
              | otherwise -> liftIO $ putStrLn $
                               "WARNING: Linearising non-linear ancestry:" ++
                               " currently at " ++ show n ++ ", ancestors " ++ show list
            (Nothing, list) ->
              liftIO $ putStrLn $ "WARNING: Linearising non-linear ancestry " ++ show list

          current <- updateHashes
          Sealed diff
                <- unFreeLeft `fmap` (liftIO $ treeDiff (const TextFile) start current)
          prims <- return $ fromPrims $ sortCoalesceFL diff
          let patch = infopatch info ((identity :: RealPatch cX cX) :>: prims)
          liftIO $ addToTentativeInventory (extractCache repo) GzipCompression (n2pia patch)
          process (Toplevel mark branch) x

        process state object = do
          liftIO $ print object
          fail $ "Unexpected object in state " ++ show state

        -- parser follows ------------------------
        object = A.parse p_object
        lex p = p >>= \x -> A.skipSpace >> return x
        lexString s = A.string (BC.pack s) >> A.skipSpace
        line = lex $ A.takeWhile (/='\n')
        maybe p = Just `fmap` p <|> return Nothing
        p_object = p_blob
                   <|> p_reset
                   <|> p_commit
                   <|> p_tag
                   <|> p_modify
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
                      author <- maybe $ p_author "author"
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
        p_mark = do lexString "mark"
                    lex $ A.char ':'
                    lex A.decimal
                  <?> "p_mark"
        p_refid = MarkId `fmap` p_marked
                  <|> (lexString "inline" >> return Inline)
                  <|> HashId `fmap` p_hash
        p_data = do lexString "data"
                    length <- A.decimal
                    A.char '\n'
                    lex $ A.take length
                  <?> "p_data"
        p_marked = lex $ A.char ':' >> A.decimal
        p_hash = lex $ A.takeWhile1 (A.inClass "0123456789abcdefABCDEF")
        p_from = lexString "from" >> From `fmap` p_marked
        p_merge = lexString "merge" >> Merge `fmap` p_marked
        p_delete = lexString "D" >> Delete `fmap` line
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

        next :: (B.ByteString -> A.Result Object) -> B.ByteString -> TreeIO (B.ByteString, Object)
        next parser rest =
          do chunk <- if B.null rest then liftIO $ B.hGet stdin (64 * 1024)
                                     else return rest
             next_chunk parser chunk
        next_chunk parser chunk =
          do case parser chunk of
               A.Done rest result -> return (rest, result)
               A.Partial cont -> next cont B.empty
               A.Fail _ ctx err -> do
                 liftIO $ putStrLn $ "=== chunk ===\n" ++ BC.unpack chunk ++ "\n=== end chunk ===="
                 fail $ "Error parsing stream. " ++ err ++ "\nContext: " ++ show ctx

