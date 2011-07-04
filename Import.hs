{-# LANGUAGE NoMonoLocalBinds, DeriveDataTypeable, GADTs, ScopedTypeVariables, ExplicitForAll #-}
module Import( fastImport, fastImportIncremental, RepoFormat(..) ) where

import Utils
import Marks
import Stash

import Codec.Compression.GZip ( decompress )
import qualified Data.Attoparsec.Char8 as A
import Data.Attoparsec.Char8( (<?>) )
import Data.Data
import Data.DateTime ( formatDateTime, parseDateTime, startOfTime )
import Data.ByteString.Base64 ( decode )
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.IORef ( newIORef, modifyIORef, readIORef )
import Control.Applicative ( Alternative, (<|>) )
import Control.Monad ( when, forM_, foldM )
import Control.Monad.Trans ( liftIO )
import Control.Monad.State.Strict( gets, modify )
import Data.Maybe ( isNothing, fromMaybe, fromJust )
import Prelude hiding ( readFile, lex, log )
import System.Directory ( doesFileExist, createDirectory
                        , createDirectoryIfMissing, getDirectoryContents
                        , copyFile, removeFile )
import System.IO ( Handle )
import System.FilePath ( (</>) )
import System.PosixCompat.Files ( createLink )

import Darcs.Diff( treeDiff )
import Darcs.Flags( Compression( .. )
                  , DarcsFlag( UseHashedInventory, UseFormat2 ) )
import Darcs.Repository ( Repository, withRepoLock, RepoJob(..)
                        , readTentativeRepo, readRepo
                        , readRepoUsingSpecificInventory , withRepository
                        , createRepository , createPristineDirectoryTree
                        , finalizeRepositoryChanges , cleanRepository )
import Darcs.Repository.HashedRepo ( addToTentativeInventory )
import Darcs.Repository.InternalTypes ( extractCache )
import Darcs.Repository.Prefs( FileType(..) )
import Darcs.Repository.State( readRecorded )
import Darcs.Patch ( RepoPatch, fromPrims, infopatch, adddeps, rmfile, rmdir
                   , adddir, move, addfile, hunk, invert )
import Darcs.Patch.Apply ( Apply(..), applyToTree )
import Darcs.Patch.Depends ( getTagsRight, newsetUnion, findUncommon
                           , merge2FL )
import Darcs.Patch.FileName ( fp2fn, normPath )
import Darcs.Patch.Info ( PatchInfo, patchinfo )
import Darcs.Patch.PatchInfoAnd ( PatchInfoAnd, n2pia )
import Darcs.Patch.Prim ( sortCoalesceFL )
import Darcs.Patch.Prim.Class ( PrimOf, is_filepatch )
import Darcs.Patch.Prim.V1 ( Prim )
import Darcs.Patch.Read ( readPatch )
import Darcs.Patch.Set ( newset2FL )
import Darcs.Patch.V2 ( RealPatch )
import Darcs.Utils ( nubsort, withCurrentDirectory, treeHasDir, treeHasFile )
import Darcs.Witnesses.Ordered ( FL(..), RL(..), (+<+), reverseFL, reverseRL
                               , (:\/:)(..), mapFL, filterRL, lengthRL )
import Darcs.Witnesses.Sealed ( seal, Sealed(..), unFreeLeft, seal2,
                                Sealed2(..), unsafeUnseal2 )
import Darcs.Witnesses.Unsafe ( unsafeCoercePEnd )

import Storage.Hashed.Monad hiding ( createDirectory, exists )
import qualified Storage.Hashed.Monad as TM
import qualified Storage.Hashed.Tree as T
import Storage.Hashed.Darcs
import Storage.Hashed.Tree( Tree, findTree )
import Storage.Hashed.AnchoredPath( floatPath, Name(..), parents, anchorPath )

data RepoFormat = Darcs2Format | HashedFormat deriving (Eq, Data, Typeable)

type Marked = Maybe Int
type Merges = [Int]
type AuthorInfo = B.ByteString
type Message = B.ByteString
type Content = B.ByteString

data ModifyData = ModifyMark Int
                | ModifyHash B.ByteString
                | Inline Content
                deriving Show

data Committish = MarkId Int
                | HashId B.ByteString
                | BranchName B.ByteString
                deriving Show

data Object = Blob (Maybe Int) Content
            | Reset ParsedBranchName (Maybe Committish)
            | Commit ParsedBranchName Marked AuthorInfo Message Marked Merges
            | Tag Int AuthorInfo Message
            | Modify ModifyData B.ByteString
            | Gitlink B.ByteString
            | Copy B.ByteString B.ByteString -- source/target filenames
            | Rename B.ByteString B.ByteString -- orig/new filenames
            | Delete B.ByteString -- filename
            | Progress B.ByteString
            deriving Show

data ObjectStream = Elem B.ByteString Object
                  | End

data State p where
    Toplevel :: Marked -> ParsedBranchName -> State p
    InCommit :: Marked -> ParsedBranchName -> Tree IO -> RL (PrimOf p) cX cY
      -> RL (PrimOf p) cA cB -> PatchInfo -> State p

instance Show (State p) where
  show (Toplevel _ _) = "Toplevel"
  show (InCommit _ _ _ _ _ _) = "InCommit"

masterBranchName :: ParsedBranchName
masterBranchName = parseBranch . BC.pack $ "refs/heads/master"

fastImport :: Bool -> Handle -> (String -> IO ()) -> String -> RepoFormat
  -> IO Marks
fastImport debug inHandle printer repodir fmt =
  do when debug $ printer "Creating new repo dir."
     createDirectory repodir
     withCurrentDirectory repodir $ do
       createRepository $ case fmt of
         Darcs2Format -> [UseFormat2]
         HashedFormat -> [UseHashedInventory]
       withRepoLock [] $ RepoJob $ \repo -> do
         let initState = Toplevel Nothing masterBranchName
         fastImport' debug repodir inHandle printer repo emptyMarks initState

fastImportIncremental :: Bool -> Handle -> (String -> IO ()) -> String
  -> Marks -> IO Marks
fastImportIncremental debug inHandle printer repodir marks =
  withCurrentDirectory repodir $
    withRepoLock [] $ RepoJob $ \repo -> do
        -- Read the last mark we processed on a previous import, to prevent non
        -- linear history errors.
        let ancestor = case lastMark marks of
                0 -> Nothing
                n -> Just n
            initState = Toplevel ancestor masterBranchName
        fastImport' debug repodir inHandle printer repo marks initState

fastImport' :: forall p r u . (RepoPatch p) => Bool -> FilePath -> Handle ->
    (String -> IO ()) -> Repository p r u r -> Marks -> State p -> IO Marks
fastImport' debug repodir inHandle printer repo marks initial = do
    let doDebug x = when debug $ printer $ "Import debug: " ++ x
    initPristine <- readRecorded repo
    patches <- newset2FL `fmap` readRepo repo
    marksref <- newIORef marks
    let check :: FL (PatchInfoAnd p) x y -> [(Int, BC.ByteString)] -> IO ()
        check NilFL [] = return ()
        check (p:>:ps) ((_,h):ms) = do
          when (patchHash p /= h) $ die "FATAL: Marks do not correspond."
          check ps ms
        check _ _ = die "FATAL: Patch and mark count do not agree."

        go :: State p -> B.ByteString -> TreeIO [BC.ByteString]
        go state rest = do objectStream <- liftIO $ parseObject inHandle rest
                           case objectStream of
                             End -> handleEndOfStream state
                             Elem rest' obj -> do
                                state' <- process state obj
                                go state' rest'

        handleEndOfStream :: State p -> TreeIO [BC.ByteString]
        handleEndOfStream state = do
          liftIO $ doDebug "Handling end-of-stream"
          -- We won't necessarily be InCommit at the EOS.
          case state of
            s@(InCommit _ _ _ _ _ _) -> finalizeCommit s
            _ -> return ()
          -- The stream may not reset us to master, so do it manually.
          restoreFromBranch "" $ parseBranch $ BC.pack "refs/heads/master"
          fullTree <- gets tree
          let branchTree = fromJust . findTree fullTree $
               floatPath "_darcs/branches"
              entries = map ((\(Name n) -> n) . fst) . T.listImmediate $
                branchTree
              branches = filter (/= BC.pack "head-master") entries
          mapM_ (initBranch . ParsedBranchName) branches
          liftIO $ doDebug "Writing tentative pristine."
          (pristine, tree') <- getTentativePristineContents
          liftIO $ BL.writeFile "_darcs/tentative_pristine" pristine
          -- dump the right tree, without _darcs
          modify $ \s -> s { tree = tree' }
          return branches

        topLevelBranchDir :: ParsedBranchName -> FilePath
        topLevelBranchDir (ParsedBranchName b) = concat
          [repodir, "-", BC.unpack b]

        initBranch :: ParsedBranchName -> TreeIO ()
        initBranch b@(ParsedBranchName bName) = do
          liftIO $ doDebug $ "Setting up branch dir for: " ++ BC.unpack bName
          inventory <- readFile $ branchInventoryPath b
          pristine <- readFile $ branchPristinePath b
          liftIO $ withCurrentDirectory ".." $ do
            let bDir = topLevelBranchDir b </> "_darcs"
                dirs = ["inventories", "patches", "pristine.hashed"]
            mapM_ (createDirectoryIfMissing True . (bDir </>)) dirs
            BL.writeFile (bDir </> "tentative_hashed_inventory") inventory
            BL.writeFile (bDir </> "tentative_pristine") pristine
            BL.writeFile (bDir </> "format") $ BL.pack $
              unlines ["hashed", "darcs-2"]


        switchBranch :: Marked -> ParsedBranchName -> Maybe Committish -> TreeIO Int
        switchBranch currentMark currentBranch newHead = do
          stashInventoryAndPristine currentMark (Just currentBranch)
          case newHead of
             Nothing -> return (fromJust currentMark) -- Base on current state
             Just newHead' -> case newHead' of
               HashId _ -> error "Cannot branch to commit id"
               MarkId mark -> restoreFromMark "" mark
               BranchName bName -> restoreFromBranch "" $ parseBranch bName

        makeinfo author message tag = do
          let (name:log) = lines $ BC.unpack message
              (author'', date'') = span (/='>') $ BC.unpack author
              date' = dropWhile (`notElem` "0123456789") date''
              author' = author'' ++ ">"
              date = formatDateTime "%Y%m%d%H%M%S" $
                fromMaybe startOfTime (parseDateTime "%s %z" date')
          liftIO $ patchinfo date
            (if tag then "TAG " ++ name else name) author' log

        addToTentativeInv :: (RepoPatch pa) => PatchInfoAnd pa x y
          -> IO FilePath
        addToTentativeInv = addToTentativeInventory (extractCache repo)
          GzipCompression

        addtag author msg =
          do info <- makeinfo author msg True
             gotany <- liftIO $
               doesFileExist "_darcs/tentative_hashed_pristine"
             deps <- if gotany then liftIO $
               getTagsRight `fmap` readTentativeRepo repo
                               else return []
             let ident = NilFL :: FL (RealPatch Prim) cX cX
                 patch = adddeps (infopatch info ident) deps
             liftIO . addToTentativeInv $ n2pia patch
             return ()

        primsAffectingFile :: FilePath -> RL (PrimOf p) cX cY
          -> [Sealed2 (PrimOf p)]
        primsAffectingFile fp =
          filterRL (\p -> (normPath `fmap` is_filepatch p) == (Just $ fp2fn fp))

        updateTreeWithPrims :: FilePath -> RL (PrimOf p) cX cY
          -> TreeIO (Tree IO) -> TreeIO (Tree IO)
        updateTreeWithPrims fn endps treeProvider = do
          let affectingPrims :: [Sealed2 (PrimOf p)]
              affectingPrims = primsAffectingFile fn endps
              invertedPrims = map (invert.unsafeUnseal2) affectingPrims
          liftIO $ doDebug $ (show.length $ affectingPrims) ++ " affecting prims."
          current <- treeProvider
          liftIO $ foldM (flip applyToTree) current invertedPrims

        diffCurrent (InCommit mark branch start ps endps info) fn = do
          current <- updateTreeWithPrims fn endps updateHashes
          Sealed diff <- unFreeLeft `fmap`
            liftIO (treeDiff (const TextFile) start current)
          let newps = reverseFL diff +<+ ps
          return $ InCommit mark branch current newps endps info
        diffCurrent _ _ =
          error "diffCurrent is never valid outside of a commit."

        checkForNewFile s@(InCommit mark branch start ps endps info) path = do
          let rawPath = BC.unpack path
              floatedPath = floatPath rawPath
              -- Only update the hash of the newly added file.
              updateFileHash = filteredUpdateHashes $
                Just (\itemPath _ -> itemPath == floatedPath)
          current <- updateTreeWithPrims rawPath endps updateFileHash
          case T.findFile start floatedPath of
            -- If we've just added a new file, we do not need to do a full
            -- diff, instead just add the raw prims to our incremental RL.
            Nothing -> do
              contents <- (BC.lines . BC.concat . BL.toChunks)
                `fmap` readFile floatedPath
              let hunkPatch = hunk rawPath 1 [] contents
                  addPatches = hunkPatch :<: addfile rawPath :<: ps
              return $ InCommit mark branch current addPatches endps info
            -- If the file already existed, just diff.
            (Just _) -> diffCurrent s rawPath
        checkForNewFile _ _ = error
          "checkForNewFile is never valid outside of a commit."

        mergeIfNecessary :: Marked -> Merges -> TreeIO ()
        mergeIfNecessary _ [] = return ()
        mergeIfNecessary currentMark merges  = do
          let cleanup :: Int -> TreeIO ()
              cleanup m = liftIO $ forM_ ["pristine", "hashed_inventory"] $
                   removeFile . (("_darcs" </> show m ++ "tentative_") ++)
              getMarkPatches m = do
                restoreFromMark (show m) m
                liftIO $ seal `fmap` readRepoUsingSpecificInventory
                  (show m ++ "tentative_hashed_inventory") repo
          liftIO $ mapM_ (doDebug . ("Merging branch: " ++) . show) merges
          (Sealed them) <- newsetUnion `fmap` mapM getMarkPatches merges
          restoreFromMark "" (fromJust currentMark)
          us <- liftIO $ readTentativeRepo repo
          us' :\/: them' <- return $ findUncommon us them
          (Sealed merged) <- return $ merge2FL us' them'
          liftIO . sequence_ $ mapFL addToTentativeInv merged
          apply merged
          mapM_ cleanup merges

        tryParseDarcsPatches :: B.ByteString ->
          (B.ByteString, (Sealed2 (RL (PrimOf p)), Sealed2 (RL (PrimOf p))))
        tryParseDarcsPatches msg = case findSubStr of
          (_, after) | B.null after -> (msg, (seal2 NilRL, seal2 NilRL))
          (prefix, patchBlob)->
            let headerLen = length "darcs-patches: "
                blobLines = BC.lines . BC.drop headerLen $ patchBlob
                nonBlob = BC.unlines $ tail blobLines
                base64Str = head blobLines
                gzipped = decode base64Str in
            case gzipped of
              Left e -> corrupt $ BC.pack e `BC.append` base64Str
              Right gzipped' ->
                let strictify = BC.concat . BL.toChunks
                    lazify bl = BL.fromChunks [bl]
                    lGzipped = lazify gzipped'
                    pList = map strictify . BL.lines . decompress $
                      lGzipped
                    ps = readPatches pList in
                    (prefix `BC.append` nonBlob, ps)
          where
          findSubStr = B.breakSubstring (BC.pack "darcs-patches: ") msg
          corrupt :: B.ByteString -> a
          corrupt s = error $
            "FATAL: Corruption? Cannot parse darcs-patches: " ++ BC.unpack s
          readPatches = foldl readP (seal2 NilRL, seal2 NilRL)
          readP :: (Sealed2 (RL (PrimOf p)), Sealed2 (RL (PrimOf p)))
            -> B.ByteString
            -> (Sealed2 (RL (PrimOf p)), Sealed2 (RL (PrimOf p)))
          readP (ss@(Sealed2 ss'), es@(Sealed2 es')) str =
            let (prefix, rest) = B.splitAt 2 str in
             case (BC.unpack prefix, rest) of
               ("S ", s) -> case readPatch s of
                              Nothing -> corrupt s
                              Just (Sealed patch) -> (seal2 $ patch :<: ss', es)
               ("E ", e) -> case readPatch e of
                              Nothing -> corrupt e
                              Just (Sealed patch) -> (ss, seal2 $ patch :<: es')
               _ -> corrupt str

        process :: State p -> Object -> TreeIO (State p)
        process s (Progress p) = do
          liftIO $ printer ("progress " ++ BC.unpack p)
          return s

        process (Toplevel n b) (Tag what author msg) = do
          if Just what == n
             then addtag author msg
             else liftIO $ printer $ "WARNING: Ignoring out-of-order tag " ++
                             head (lines $ BC.unpack msg)
          return (Toplevel n b)

        process (Toplevel n currentBranch) (Reset branch from) = do
             switchBranch n currentBranch from
             return $ Toplevel n branch

        process (Toplevel n b) (Blob (Just m) bits) = do
          TM.writeFile (markpath m) (BL.fromChunks [bits])
          return $ Toplevel n b

        process x (Gitlink link) = do
          liftIO $ printer $ "WARNING: Ignoring gitlink " ++ BC.unpack link
          return x

        process (Toplevel previous pbranch)
          (Commit branch mark author message from merges) = do
          liftIO $ doDebug $ "Handling commit beginning: " ++
            BC.unpack (BC.take 20 message)
          fromMark <- if pbranch /= branch
            then do
              liftIO $ doDebug $ unwords
                ["Switching branch from", show pbranch, "to", show branch]
              Just `fmap` switchBranch previous pbranch (MarkId `fmap` from)
            else return previous
          mergeIfNecessary fromMark merges
          (message', (Sealed2 prePatches, Sealed2 postPatches)) <-
            return $ tryParseDarcsPatches message
          let preCount = lengthRL prePatches
              postCount = lengthRL postPatches
          when (preCount > 0 || postCount > 0) $
            liftIO $ doDebug $ unwords ["Commit contains", show preCount
              , "start darcs-patches and", show postCount, "end darcs-patches."]
          apply prePatches
          startstate <- updateHashes
          info <- makeinfo author message' False
          return $ InCommit mark branch startstate prePatches postPatches info

        process s@(InCommit _ _ _ _ _ _) (Modify (ModifyMark m) path) = do
          liftIO $ doDebug $ "Handling modify of: " ++ BC.unpack path
          TM.copy (markpath m) (floatPath $ BC.unpack path)
          checkForNewFile s path

        process s@(InCommit _ _ _ _ _ _) (Modify (Inline bits) path) = do
          liftIO $ doDebug $ "Handling modify of: " ++ BC.unpack path
          TM.writeFile (floatPath $ BC.unpack path) (BL.fromChunks [bits])
          checkForNewFile s path

        process (InCommit _ _ _ _ _ _) (Modify (ModifyHash hash) path) =
          die $ unwords ["FATAL: Cannot currently handle Git hash:",
             BC.unpack hash, "for file", BC.unpack path,
             "Do not use the --no-data option of git fast-export."]

        process (InCommit mark branch _ ps endps info) (Delete path) = do
          liftIO $ doDebug $ "Handling delete of: " ++ BC.unpack path
          let filePath = BC.unpack path
              rmPatch = rmfile filePath
          TM.unlink $ floatPath filePath
          current <- updateHashes
          return $ InCommit mark branch current (rmPatch :<: ps) endps info

        process s@(InCommit _ _ _ _ _ _) (Copy from to) = do
          liftIO $ doDebug $ unwords
            ["Handling copy from of:", BC.unpack from, "to", BC.unpack to]
          let unpackFloat = floatPath.BC.unpack
          TM.copy (unpackFloat from) (unpackFloat to)
          -- We can't tell Darcs that a file has been copied, so it'll show as
          -- an addfile.
          diffCurrent s $ BC.unpack to

        process (InCommit mark branch start ps endps info)
          (Rename from to) = do
          liftIO $ doDebug $ unwords
            ["Handling rename from of:", BC.unpack from, "to", BC.unpack to]
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
                return $ foldl (flip $ (:<:) . adddir) NilRL missingPaths
          let movePatches = move uFrom uTo :<: preparePatchesRL
          TM.rename (floatPath uFrom) (floatPath uTo)
          current <- updateHashes
          return $ InCommit mark branch current (movePatches +<+ ps) endps info

        -- When we leave the commit, create a patch for the cumulated prims.
        process s@(InCommit mark branch _ _ _ _) x = do
          finalizeCommit s
          process (Toplevel mark branch) x

        process state obj = do
          liftIO $ print obj
          fail $ "Unexpected object in state " ++ show state

        finalizeCommit :: State p -> TreeIO ()
        finalizeCommit (Toplevel _ _) =
          error "Cannot finalize commit at toplevel."
        finalizeCommit (InCommit mark branch _ ps endps info) = do
          liftIO $ doDebug "Finalising commit."
          (prims :: FL p cX cY)  <- return $ fromPrims $
            sortCoalesceFL . reverseRL $ endps +<+ unsafeCoercePEnd ps
          let patch = infopatch info prims
          liftIO $ addToTentativeInventory (extractCache repo)
                                           GzipCompression (n2pia patch)
          case mark of
            Nothing -> return ()
            Just n -> case getMark marks n of
              Nothing -> liftIO $ modifyIORef marksref $
                \m -> addMark m n (patchHash $ n2pia patch)
              Just n' -> die $ "FATAL: Mark already exists: " ++ BC.unpack n'
          stashInventoryAndPristine mark (Just branch)

        notdot ('.':_) = False
        notdot _ = True

    -- TODO: this will die on incremental import...
    check patches (listMarks marks)
    (branches, _) <- hashedTreeIO (go initial B.empty) initPristine
      "_darcs/pristine.hashed"
    finalizeRepositoryChanges repo
    let pristineDir = "_darcs" </> "pristine.hashed"
    pristines <- filter notdot `fmap` getDirectoryContents pristineDir
    forM_ branches $ \b ->
      withCurrentDirectory
        (".." </> topLevelBranchDir (ParsedBranchName b)) $ do
        doDebug $
          "Linking pristines and copying patches/inventories for branch: "
          ++ BC.unpack b
        let origPristineDir = ".." </> repodir </> pristineDir
            origRepoDir = ".." </> repodir </> "_darcs"
        -- Hardlink all pristines.
        forM_ pristines $
          \name -> createLink (origPristineDir </> name) (pristineDir </> name)
        forM_ ["patches", "inventories"] $ \dir -> do
          contents <- filter notdot `fmap`
            getDirectoryContents (".." </> repodir </> "_darcs" </> dir)
          forM_ contents $
            \name -> copyFile (origRepoDir </> dir </> name)
                              ("_darcs" </> dir </> name)

        withRepository [] $ RepoJob $ \bRepo -> do
          doDebug $ "Finalizing and cleaning branch repo: " ++ BC.unpack b
          finalizeRepositoryChanges bRepo
          createPristineDirectoryTree bRepo "."
          cleanRepository bRepo
    -- Must clean main repo after branches, else we'll have missing pristines.
    doDebug "Finalizing and cleaning master repo."
    createPristineDirectoryTree repo "."
    cleanRepository repo
    readIORef marksref

parseObject :: Handle -> BC.ByteString -> IO ObjectStream
parseObject inHandle = next mbObject
  where mbObject = A.parse p_maybeObject
        lex :: A.Parser b -> A.Parser b
        lex p = p >>= \x -> A.skipSpace >> return x
        lexString s = A.string (BC.pack s) >> A.skipSpace
        line = lex $ A.takeWhile (/='\n')

        optional :: (Alternative f, Monad f) => f a -> f (Maybe a)
        optional p = Just `fmap` p <|> return Nothing

        p_maybeObject = Just `fmap` p_object
                        <|> (A.endOfInput >> return Nothing)

        p_object = p_blob
                   <|> p_reset
                   <|> p_commit
                   <|> p_tag
                   <|> p_modify
                   <|> p_rename
                   <|> p_copy
                   <|> p_delete
                   <|> (lexString "progress" >> Progress `fmap` line)

        p_author name = lexString name >> line

        p_reset = do lexString "reset"
                     branch <- parseBranch `fmap` line
                     committish <- optional $ lexString "from" >> p_committish
                     return $ Reset branch committish

        p_committish = MarkId `fmap` p_marked
                   <|> HashId `fmap` p_hash
                   <|> BranchName `fmap` line

        p_commit = do lexString "commit"
                      branch <- parseBranch `fmap` line
                      mark <- optional p_mark
                      _ <- optional $ p_author "author"
                      committer <- p_author "committer"
                      message <- p_data
                      from <- optional p_from
                      merges <- A.many p_merge
                      return $ Commit branch mark committer message from merges

        p_tag = do lexString "tag" >> line -- FIXME we ignore branch for now
                   lexString "from"
                   mark <- p_marked
                   author <- p_author "tagger"
                   message <- p_data
                   return $ Tag mark author message

        p_blob = do lexString "blob"
                    mark <- optional p_mark
                    Blob mark `fmap` p_data
                  <?> "p_blob"

        p_mark :: A.Parser Int
        p_mark = do lexString "mark"
                    p_marked
                  <?> "p_mark"

        p_modifyData :: A.Parser (Maybe ModifyData)
        p_modifyData = (Just . ModifyMark) `fmap` p_marked
                   <|> (lexString "inline" >> return Nothing)
                   <|> (Just . ModifyHash) `fmap` p_hash

        p_data = do lexString "data"
                    len <- A.decimal
                    A.char '\n'
                    lex $ A.take len
                  <?> "p_data"

        p_marked = lex $ A.char ':' >> A.decimal
        p_hash = lex $ A.takeWhile1 (A.inClass "0-9a-fA-F")
        p_from = lexString "from" >> p_marked
        p_merge = lexString "merge" >> p_marked
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
                      mode <- lex $ A.takeWhile (A.inClass "0-9")
                      mbData <- p_modifyData
                      path <- line
                      case mbData of
                        Nothing -> do
                          inlineData <- Inline `fmap` p_data
                          return $ Modify inlineData path
                        Just h@(ModifyHash hash) | mode == BC.pack "160000" ->
                          return $ Gitlink hash
                                                 | otherwise ->
                          return $ Modify h path
                        Just mark -> return $ Modify mark path

        next :: (B.ByteString -> A.Result (Maybe Object)) -> B.ByteString
          -> IO ObjectStream
        next parser rest =
          do chunk <- if B.null rest then liftIO $ B.hGet inHandle (64 * 1024)
                                     else return rest
             next_chunk parser chunk
        next_chunk parser chunk =
          case parser chunk of
             A.Done rest result -> return . maybe End (Elem rest) $ result
             A.Partial cont -> next cont B.empty
             A.Fail _ ctx err -> do
               let ch = "\n=== chunk ===\n" ++ BC.unpack chunk ++
                        "\n=== end chunk ===="
               fail $ "Error parsing stream. " ++ err ++ ch ++
                 "\nContext: " ++ show ctx
