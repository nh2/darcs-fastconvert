{-# LANGUAGE NoMonoLocalBinds, DeriveDataTypeable, GADTs, ScopedTypeVariables, ExplicitForAll,
             TypeFamilies #-}
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
import Data.List ( (\\) )
import qualified Data.Map as M
import Control.Applicative ( Alternative, (<|>) )
import Control.Arrow ( first, (&&&) )
import Control.Monad ( when, forM_, foldM, unless, void )
import Control.Monad.Trans ( liftIO, MonadIO )
import Control.Monad.State.Strict( modify )
import Data.Maybe ( isNothing, fromMaybe, fromJust )
import Numeric ( showHex )
import Prelude hiding ( readFile, lex, log, pi )
import System.Directory ( doesFileExist, createDirectory
                        , createDirectoryIfMissing, getDirectoryContents
                        , copyFile, canonicalizePath )
import System.IO ( Handle )
import System.FilePath ( (</>) )
import System.PosixCompat.Files ( createLink )
import System.Random ( randomRIO )

import Darcs.Commands ( commandCommand )
import qualified Darcs.Commands.Get as DCG
import Darcs.Diff( treeDiff )
import Darcs.Flags( Compression( .. )
                  , DarcsFlag( UseHashedInventory, UseFormat2, Quiet
                             , OnePattern ) )
import Darcs.Lock( withTempDir )
import Darcs.RepoPath ( toPath )
import Darcs.Repository ( Repository, withRepoLock, RepoJob(..)
                        , readRepo , withRepository, createRepository
                        , createPristineDirectoryTree
                        , finalizeRepositoryChanges , cleanRepository )
import Darcs.Repository.HashedRepo ( writePatchIfNecessary
                                   , readRepoFromInventoryList )
import Darcs.Repository.InternalTypes ( extractCache )
import Darcs.Repository.Prefs( FileType(..) )
import Darcs.Repository.State( readRecorded )
import Darcs.Patch ( RepoPatch, fromPrims, infopatch, adddeps, rmfile, rmdir
                   , adddir, move, invert )
import Darcs.Patch.Apply ( Apply(..), applyToTree )
import Darcs.Patch.Depends ( getTagsRight, newsetUnion, findUncommon
                           , merge2FL )
import Darcs.Patch.FileName ( fp2fn, normPath )
import Darcs.Patch.Info ( PatchInfo, patchinfo, piAuthor, piName )
import Darcs.Patch.MatchData ( patchMatch )
import Darcs.Patch.PatchInfoAnd ( PatchInfoAnd, n2pia, extractHash, info )
import Darcs.Patch.Prim ( sortCoalesceFL )
import Darcs.Patch.Prim.Class ( PrimOf, is_filepatch )
import Darcs.Patch.Prim.V1 ( Prim )
import Darcs.Patch.Read ( readPatch )
import Darcs.Patch.Set ( newset2FL, SealedPatchSet, Origin )
import Darcs.Patch.V2 ( RealPatch )
import Darcs.Utils ( nubsort, withCurrentDirectory, treeHasDir, treeHasFile )
import Darcs.Witnesses.Ordered ( FL(..), RL(..), (+<+), reverseFL, reverseRL
                               , (:\/:)(..), mapFL, filterRL, lengthRL )
import Darcs.Witnesses.Sealed ( Sealed(..), unFreeLeft, seal2,
                                Sealed2(..), unsafeUnseal2 )
import Darcs.Witnesses.Unsafe ( unsafeCoercePEnd )

import Storage.Hashed.Monad hiding ( createDirectory, exists )
import qualified Storage.Hashed.Monad as TM
import qualified Storage.Hashed.Tree as T
import Storage.Hashed.Tree ( Tree(..) )
import Storage.Hashed.Darcs
import Storage.Hashed.AnchoredPath( floatPath, parents, anchorPath )

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
      -> RL (PrimOf p) cA cB -> PatchInfo -> Maybe String -> State p

instance Show (State p) where
  show (Toplevel _ _) = "Toplevel"
  show (InCommit _ _ _ _ _ _ _) = "InCommit"

masterBranchName :: ParsedBranchName
masterBranchName = parseBranch . BC.pack $ "refs/heads/master"

fastImport :: Bool -> Handle -> (String -> IO ()) -> String -> RepoFormat
  -> IO Marks
fastImport debug inHandle printer repodir fmt =
  do when debug $ printer "Creating new repo dir."
     createDirectory repodir
     realRepoPath <- canonicalizePath repodir
     withCurrentDirectory realRepoPath $ do
       createRepository $ case fmt of
         Darcs2Format -> [UseFormat2]
         HashedFormat -> [UseHashedInventory]
       withRepoLock [] $ RepoJob $ \repo -> do
         let initState = Toplevel Nothing masterBranchName
         fastImport' debug realRepoPath inHandle printer repo emptyMarks
           initState

fastImportIncremental :: Bool -> Handle -> (String -> IO ()) -> String
  -> Marks -> IO Marks
fastImportIncremental debug inHandle printer repodir marks =
  withCurrentDirectory repodir $
    withRepoLock [] $ RepoJob $ \repo -> do
        let ancestor = lastMarkForBranch (pb2bn masterBranchName) marks
            initState = Toplevel ancestor masterBranchName
        fastImport' debug repodir inHandle printer repo marks initState

fastImport' :: forall p r u . (RepoPatch p, ApplyState p ~ Tree, ApplyState (PrimOf p) ~ Tree)
            => Bool -> FilePath -> Handle
            -> (String -> IO ()) -> Repository p r u r -> Marks -> State p -> IO Marks
fastImport' debug repodir inHandle printer repo marks initial = do
    let doDebug x = when debug $ printer $ "Import debug: " ++ x
        doTreeIODebug :: String -> TreeIO ()
        doTreeIODebug x = liftIO $ doDebug x
        -- (All inventories, current inventory)
        initInventories = (emptyMarkInventory, [])
    initPristine <- readRecorded repo
    patches <- newset2FL `fmap` readRepo repo
    -- We can easily check the master patches, which will ensure that the marks
    -- we've been given were used for a previous import of this repo.
    let check :: FL (PatchInfoAnd p) x y
          -> [(Int, (BC.ByteString, BC.ByteString, BC.ByteString))] -> IO ()
        check NilFL [] = return ()
        check (p:>:ps) ((_, (h, _, _)):ms) = do
          when (patchHash p /= h) $ die $ unwords
            [ "Marks do not correspond: expected:", show h, "got"
            , BC.unpack $ patchHash p, "for patch:", piName (info p) ]
          check ps ms
        check _ _ = die "Patch and mark count do not agree."

        masterBName = pb2bn masterBranchName
        isMaster (_, (_, bName, _)) = masterBName == bName
        masterMarks = filter isMaster $ listMarks marks
    check patches masterMarks
    marksref <- newIORef marks
    branchesref <- newIORef $ listBranches marks
    inventoriesref <- newIORef initInventories
    let pristineDir = "_darcs" </> "pristine.hashed"

        branchEdited b m = liftIO $ modifyIORef branchesref $
          \bs -> M.insert (pb2bn b) m bs

        go :: State p -> B.ByteString -> TreeIO [(ParsedBranchName, Int)]
        go state rest = do objectStream <- liftIO $ parseObject inHandle rest
                           case objectStream of
                             End -> handleEndOfStream state
                             Elem rest' obj -> do
                                state' <- process state obj
                                go state' rest'

        handleEndOfStream :: State p -> TreeIO [(ParsedBranchName, Int)]
        handleEndOfStream state = do
          doTreeIODebug "Handling end-of-stream"
          -- We won't necessarily be InCommit at the EOS.
          case state of
            s@(InCommit _ _ _ _ _ _ _) -> finalizeCommit s
            _ -> return ()
          currentBranches <- liftIO $ M.assocs `fmap` readIORef branchesref
          unless (null currentBranches) $
            -- The stream may not reset us to master, so do it manually.
            void $ restoreToBranch $ parseBranch $ BC.pack "refs/heads/master"
          let branches = map (first ParsedBranchName) .
                filter ((/= BC.pack "head-master") . fst) $ currentBranches
          mapM_ initBranch branches
          doTreeIODebug "Writing tentative pristine."
          (pristine, tree') <- getTentativePristineContents
          liftIO $ writePristineToPath "_darcs/tentative_pristine" pristine
          inv <- liftIO $
            (getLastBranchMark masterBranchName >>= getInventoryForMark . Just)
              `catch` \_ -> return []
          liftIO $
            writeInventoryToPath "_darcs/tentative_hashed_inventory" inv
          -- dump the right tree, without _darcs
          modify $ \s -> s { tree = tree' }
          return branches

        initBranch :: (ParsedBranchName, Int) -> TreeIO ()
        initBranch (b@(ParsedBranchName bName), m) = do
          doTreeIODebug $ "Setting up branch dir for: " ++ BC.unpack bName
          pristine <-
            (B.concat . BL.toChunks) `fmap` readFile (markPristinePath m)
          inv <- liftIO $ getInventoryForMark (Just m)
          liftIO $ withCurrentDirectory ".." $ do
            let bDir = topLevelBranchDir repodir b </> "_darcs"
                dirs = ["inventories", "patches", "pristine.hashed"]
            mapM_ (createDirectoryIfMissing True . (bDir </>)) dirs
            liftIO $
              writeInventoryToPath (bDir </> "tentative_hashed_inventory") inv
            writePristineToPath (bDir </> "tentative_pristine") pristine
            BL.writeFile (bDir </> "format") $ BL.pack $
              unlines ["hashed", "darcs-2"]

        readBranchPristinesAndInventory :: forall p1 r1 u1 . (RepoPatch p1, ApplyState p1 ~ Tree) =>
          Repository p1 r1 u1 r1 -> FilePath -> FilePath
          -> IO (Inventory, BC.ByteString, [String])
        readBranchPristinesAndInventory bRepo branchDir fullRepoPath = do
          ps <- newset2FL `fmap` readRepo bRepo
          -- Create a complete inventory, since we can't be sure that we have
          -- any preceeding tags, so we can't just read hashed_inventory. On
          -- initial import, all inventories will be without 'Starting with
          -- inventory:' and on incremental imports we will create the entire
          -- inventory, so we will never be missing inventories.
          let inventory = fl2inv ps
          -- We can read the first line of hashed_inventory to obtain the
          -- current pristine.
          pristine <- ((BC.drop (length "pristine:")) . head . BC.lines) `fmap`
            BC.readFile "_darcs/hashed_inventory"
          pristines <- filter notdot `fmap`
            getDirectoryContents
              (branchDir </> pristineDir)
          -- Hardlink all the pristines, since the branch-repo will
          -- probably contain pristines we don't have currently.
          forM_ pristines $ \name -> do
            let target = fullRepoPath </> pristineDir </> name
            targetExists <- doesFileExist target
            unless targetExists $ createLink
              (branchDir </> pristineDir </> name) target
          return (inventory, pristine, pristines)

        stashInventoryAndPristine :: Marked -> TreeIO ()
        stashInventoryAndPristine mbMark = do
          stashPristine mbMark
          liftIO $ stashInventory mbMark =<< getCurrentInventory

        stashInventory :: Marked -> Inventory -> IO ()
        stashInventory mbMark inv = maybe (return ()) updateInvs mbMark where
          updateInvs mark = modifyIORef inventoriesref $
            first (addMarkInventory mark inv)

        getCurrentInventory :: IO Inventory
        getCurrentInventory = snd `fmap` readIORef inventoriesref

        addPatchToCurrentInventory :: PatchInfo -> String -> IO ()
        addPatchToCurrentInventory pInfo hash =
          modifyIORef inventoriesref $ \(is, current) ->
            let newInv = (pInfo, hash) : current in
            newInv `seq` (is, newInv)

        getInventoryForMark :: Marked -> IO Inventory
        getInventoryForMark mbMark = do
          mark <- maybe (die "Cannot obtain inventory from Nothing mark.")
            return mbMark
          (invs, _) <- readIORef inventoriesref
          let mbInv = getMarkInventory mark invs
          case mbInv of
            Just inv -> return inv
            Nothing -> die $ "Couldn't retrieve inventory for mark: "
                             ++ show mark

        restoreInventoryFromMark mbMark = do
          inv <- getInventoryForMark mbMark
          modifyIORef inventoriesref $ \(invs, _) -> (invs, inv)

        restoreToMark mark = do
          canRestore <- canRestoreFromMark mark
          doTreeIODebug $ "Trying to read mark " ++ show mark
            ++ " pristine/inventory "
            ++ if canRestore then "quickly" else "slowly"
          let restorer = if canRestore
                           then fastRestoreFromMark
                           else slowRestoreFromMark
          restorer mark
          return mark

        fastRestoreFromMark mark = do
          restorePristineFromMark mark
          liftIO $ restoreInventoryFromMark (Just mark)

        slowRestoreFromMark mark = do
          doTreeIODebug $
            "Mark pristine was not found. Cloning and unpulling "
            ++ "by hashes, to find the correct pristine; this may "
            ++ "well be slow."
          case getMark marks mark of
            Nothing -> die $ "Cannot restore to mark: " ++ show mark
             ++ " since it is not in the current import, or the marks file"
            Just (hash, branch, _) -> do
              doTreeIODebug $ unwords [ "Mark has hash:", BC.unpack hash
                                      , "and branch:", BC.unpack branch]
              branchDir <- liftIO $
                canonicalizePath
                 (".." </> topLevelBranchDir repodir (ParsedBranchName branch))
                `catch` \_ -> die $ "Non-existent branch: " ++ BC.unpack branch
              fullRepoPath <- liftIO $ canonicalizePath "."
              (inv, pris) <- liftIO $ withTempDir "import-temporary" $
                \tempdir -> do
                  commandCommand DCG.get [Quiet,
                    OnePattern (patchMatch $ "hash " ++ BC.unpack hash)]
                    [branchDir, "repo"] `catch` \_ -> die . unwords $
                    [ "Could not clone branch ", BC.unpack branch, "; either"
                    , branchDir
                    , "is not a valid repo or the repo doesn't contain a patch"
                    , "with hash", BC.unpack hash]
                  tempRepoPath <- canonicalizePath $ toPath tempdir </> "repo"
                  withCurrentDirectory tempRepoPath $
                    withRepository [] $ RepoJob $ \bRepo -> do
                      (inventory, pristine, pristines) <-
                        readBranchPristinesAndInventory bRepo branchDir
                          fullRepoPath
                      tempPristines <- filter notdot `fmap`
                        getDirectoryContents
                          (tempRepoPath </> pristineDir)
                      -- Copy any pristines that were created when changes were
                      -- unpulled in the temp repo.
                      forM_ (tempPristines \\ pristines) $ \name -> copyFile
                        (tempRepoPath </> pristineDir </> name)
                        (fullRepoPath </> pristineDir </> name)
                      return (inventory, pristine)
              stashPristineBS (Just mark) pris
              liftIO $ stashInventory (Just mark) inv
              fastRestoreFromMark mark

        getLastBranchMark :: ParsedBranchName -> IO Int
        getLastBranchMark branch = do
          currentMarks <- readIORef marksref
          case lastMarkForBranch (pb2bn branch) currentMarks of
            Just m -> return m
            Nothing -> die $ "Couldn't find last mark for branch: "
                             ++ BC.unpack (pb2bn branch) ++ " marks: \n" ++
                             show currentMarks

        restoreToBranch :: ParsedBranchName -> TreeIO Int
        restoreToBranch branch = do
          doTreeIODebug "Trying to read branch pristine/inventory."
          m <- liftIO $ getLastBranchMark branch
          restoreToMark m

        fl2inv :: forall cX cY patch . FL (PatchInfoAnd patch) cX cY
          -> Inventory
        fl2inv ps = reverse $ mapFL getInfoAndHash ps where
          getInfoAndHash :: forall cA cB . PatchInfoAnd patch cA cB
            -> (PatchInfo, String)
          getInfoAndHash p = case extractHash p of
            Right h -> (info p, h)
            _       -> error "FATAL: could not extract hash of patch."

        switchBranch :: Marked -> Maybe Committish
          -> TreeIO Int
        switchBranch currentMark newHead = case newHead of
          Nothing -> return (fromJust currentMark) -- Base on current state
          Just newHead' -> case newHead' of
            HashId _ -> error "Cannot branch to commit id"
            MarkId mark -> restoreToMark mark
            BranchName bName -> restoreToBranch $ parseBranch bName

        makeinfo author message tag = do
          let (name, log) = case BC.unpack message of
                "" -> ("Unnamed patch", [])
                msg -> (head &&& tail) . lines $ msg
              (author'', date'') = span (/='>') $ BC.unpack author
              date' = dropWhile (`notElem` "0123456789") date''
              author' = author'' ++ ">"
              date = formatDateTime "%Y%m%d%H%M%S" $
                fromMaybe startOfTime (parseDateTime "%s %z" date')
          liftIO $ patchinfo date
            (if tag then "TAG " ++ name else name) author' log

        addtag :: BC.ByteString -> BC.ByteString -> TreeIO ()
        addtag author msg =
          do pInfo <- makeinfo author msg True
             inventory <- liftIO getCurrentInventory
             let gotany = not $ null inventory
             (Sealed patchSet) <- readRepoFromInventory inventory
             let deps = if gotany then getTagsRight patchSet else []
                 ident = NilFL :: FL (RealPatch Prim) cX cX
                 patch = adddeps (infopatch pInfo ident) deps
             hash <- liftIO $ snd `fmap` writePatchIfNecessary
               (extractCache repo) GzipCompression (n2pia patch)
             doTreeIODebug $
               "Adding tag beginning:" ++ BC.unpack (BC.take 20 msg)
             liftIO $ addPatchToCurrentInventory pInfo hash

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
          current <- treeProvider
          liftIO $ foldM (flip applyToTree) current invertedPrims

        diffCurrent (InCommit mark branch start ps endps pInfo mergeID) fn = do
          current <- updateTreeWithPrims (BC.unpack fn) endps updateHashes
          Sealed diff <- unFreeLeft `fmap`
            liftIO (treeDiff (const TextFile) start current)
          let newps = reverseFL diff +<+ ps
          return $ InCommit mark branch current newps endps pInfo mergeID
        diffCurrent _ _ =
          error "diffCurrent is never valid outside of a commit."

        readRepoFromInventory :: Inventory -> TreeIO (SealedPatchSet p Origin)
        readRepoFromInventory inv = liftIO
          -- Reverse the inventory order since we prepend new patches to the
          -- inventory list but inventorise are stored oldest->newest on disk
          -- (and the readinv code expects them in that order).
          (readRepoFromInventoryList (extractCache repo)
            (Nothing, reverse inv)
          `catch`
          \_ -> die $ "Failed to read repo from inventory: " ++ show inv)

        mergeIfNecessary :: Marked -> Merges -> AuthorInfo ->
          TreeIO (Maybe String)
        mergeIfNecessary _ [] _ = return Nothing
        mergeIfNecessary fromMark merges author = do
          randStr <- liftIO $
            flip showHex "" `fmap` randomRIO (0,2^(128 ::Integer) :: Integer)
          let getMarkPatches m = do
                restoreToMark m
                -- Tag the source, so we know the pre-merge context of each set
                -- of patches.
                addtag author
                  (BC.pack $ "darcs-fastconvert merge pre-source: " ++ randStr)
                inventory <- liftIO getCurrentInventory
                readRepoFromInventory inventory
          liftIO $ mapM_ (doDebug . ("Merging branch: " ++) . show) merges
          (Sealed them) <- newsetUnion `fmap` mapM getMarkPatches merges
          restoreToMark $ fromJust fromMark
          addtag author
            (BC.pack $ "darcs-fastconvert merge pre-target: " ++ randStr)
          inventory <- liftIO getCurrentInventory
          (Sealed us) <- readRepoFromInventory inventory
          us' :\/: them' <- return $ findUncommon us them
          (Sealed merged) <- return $ merge2FL us' them'
          infosAndHashes <- liftIO . sequence $ mapFL hasher merged
          liftIO $ forM_ infosAndHashes $ uncurry addPatchToCurrentInventory
          apply merged
          return $ Just randStr

        hasher :: forall cX cY. PatchInfoAnd p cX cY -> IO (PatchInfo, String)
        hasher = writePatchIfNecessary (extractCache repo) GzipCompression

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
                              Just (Sealed patch) ->
                                (seal2 $ patch :<: ss', es)
               ("E ", e) -> case readPatch e of
                              Nothing -> corrupt e
                              Just (Sealed patch) ->
                                (ss, seal2 $ patch :<: es')
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

        process (Toplevel n _) (Reset branch from) = do
             fromMark <- switchBranch n from
             branchEdited branch fromMark
             return $ Toplevel n branch

        process (Toplevel n b) (Blob (Just m) bits) = do
          TM.writeFile (markpath m) (BL.fromChunks [bits])
          return $ Toplevel n b

        process x (Gitlink link) = do
          liftIO $ printer $ "WARNING: Ignoring gitlink " ++ BC.unpack link
          return x

        process (Toplevel previous pbranch)
          (Commit branch mark author message from merges) = do
          doTreeIODebug $ "Handling commit beginning: " ++
            BC.unpack (BC.take 20 message)
          maybe (return ()) (branchEdited branch) mark
          fromMark <- if (pbranch /= branch) || (from /= previous)
            then do
              doTreeIODebug $ unwords
                [ "Switching branch from", show pbranch, "to", show branch
                , "previous:", show previous, "from:", show from]
              Just `fmap` switchBranch previous (MarkId `fmap` from)
            else return previous
          mergeID <- mergeIfNecessary fromMark merges author
          (message', (Sealed2 prePatches, Sealed2 postPatches)) <-
            return $ tryParseDarcsPatches message
          let preCount = lengthRL prePatches
              postCount = lengthRL postPatches
          when (preCount > 0 || postCount > 0) $
            doTreeIODebug $ unwords ["Commit contains", show preCount,
              "start darcs-patches and", show postCount, "end darcs-patches."]
          apply prePatches
          startstate <- updateHashes
          pInfo <- makeinfo author message' False
          return $
           InCommit mark branch startstate prePatches postPatches pInfo mergeID

        process s@(InCommit _ _ _ _ _ _ _) (Modify (ModifyMark m) path) = do
          doTreeIODebug $ "Handling modify of: " ++ BC.unpack path
          TM.copy (markpath m) (floatPath $ BC.unpack path)
          diffCurrent s path

        process s@(InCommit _ _ _ _ _ _ _) (Modify (Inline bits) path) = do
          doTreeIODebug $ "Handling modify of: " ++ BC.unpack path
          TM.writeFile (floatPath $ BC.unpack path) (BL.fromChunks [bits])
          diffCurrent s path

        process (InCommit _ _ _ _ _ _ _) (Modify (ModifyHash hash) path) =
          die $ unwords ["Cannot currently handle Git hash:",
             BC.unpack hash, "for file", BC.unpack path,
             "Do not use the --no-data option of git fast-export."]

        process (InCommit mark branch _ ps endps pInfo mergeID)
         (Delete path) = do
          doTreeIODebug $ "Handling delete of: " ++ BC.unpack path
          let filePath = BC.unpack path
              rmPatch = rmfile filePath
          TM.unlink $ floatPath filePath
          current <- updateHashes
          return $
            InCommit mark branch current (rmPatch :<: ps) endps pInfo mergeID

        process s@(InCommit _ _ _ _ _ _ _) (Copy from to) = do
          doTreeIODebug $ unwords
            ["Handling copy from of:", BC.unpack from, "to", BC.unpack to]
          let unpackFloat = floatPath.BC.unpack
          TM.copy (unpackFloat from) (unpackFloat to)
          -- We can't tell Darcs that a file has been copied, so it'll show as
          -- an addfile.
          diffCurrent s to

        process (InCommit mark branch start ps endps pInfo mergeID)
          (Rename from to) = do
          doTreeIODebug $ unwords
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
                    missing = filter (isNothing . T.findTree start) parentPaths
                    missingPaths = nubsort (map (anchorPath "") missing)
                return $ foldl (flip $ (:<:) . adddir) NilRL missingPaths
          let movePatches = move uFrom uTo :<: preparePatchesRL
          TM.rename (floatPath uFrom) (floatPath uTo)
          current <- updateHashes
          let ps' = movePatches +<+ ps
          return $
            InCommit mark branch current ps' endps pInfo mergeID

        -- When we leave the commit, create a patch for the cumulated prims.
        process s@(InCommit mark branch _ _ _ _ _) x = do
          finalizeCommit s
          process (Toplevel mark branch) x

        process state obj = do
          liftIO $ print obj
          fail $ "Unexpected object in state " ++ show state

        finalizeCommit :: State p -> TreeIO ()
        finalizeCommit (Toplevel _ _) =
          error "Cannot finalize commit at toplevel."
        finalizeCommit (InCommit mark branch _ ps endps pInfo mergeID) = do
          doTreeIODebug "Finalising commit."
          (prims :: FL p cX cY)  <- return $ fromPrims $
            sortCoalesceFL . reverseRL $ endps +<+ unsafeCoercePEnd ps
          let patch = infopatch pInfo prims
          hash <- liftIO $ snd `fmap` writePatchIfNecessary (extractCache repo)
            GzipCompression (n2pia patch)
          liftIO $ addPatchToCurrentInventory pInfo hash
          case mark of
            Nothing -> return ()
            Just n -> case getMark marks n of
              Nothing -> liftIO $ modifyIORef marksref $
                \m -> addMark m n (patchHash $ n2pia patch, pb2bn branch, BC.pack "-")
              Just (n', _, _) -> die $ "Mark already exists: " ++ BC.unpack n'
          let authorString = piAuthor pInfo ++ " " ++ patchDate (n2pia patch)
              doTag randStr = addtag (BC.pack authorString)
                               (BC.pack $ "darcs-fastconvert merge post: "
                                 ++ randStr)
          maybe (return ()) doTag mergeID
          stashInventoryAndPristine mark

    (branches, _) <- hashedTreeIO (go initial B.empty) initPristine
      "_darcs/pristine.hashed"
    finalizeRepositoryChanges repo
    forM_ branches $ \(b,_) ->
      withCurrentDirectory (".." </> topLevelBranchDir repodir b) $ do
        doDebug $
          "Linking pristines and copying patches/inventories for branch: "
          ++ BC.unpack (pb2bn b)
        -- Hardlink all pristines.
        pristines <- filter notdot `fmap`
          getDirectoryContents (".." </> repodir </> pristineDir)
        forM_ pristines $
          \name -> do
            let target = pristineDir </> name
            targetExists <- doesFileExist target
            unless targetExists $
              createLink (".." </> repodir </> pristineDir </> name) target
        forM_ ["patches", "inventories"] $ \dir -> do
          contents <- filter notdot `fmap`
            getDirectoryContents (".." </> repodir </> "_darcs" </> dir)
          forM_ contents $
            \name -> copyFile (".." </> repodir </> "_darcs" </> dir </> name)
                              ("_darcs" </> dir </> name)

        withRepository [] $ RepoJob $ \bRepo -> do
          doDebug $ "Finalizing and cleaning branch repo: "
                    ++ BC.unpack (pb2bn b)
          finalizeRepositoryChanges bRepo
          createPristineDirectoryTree bRepo "."
          cleanRepository bRepo
    -- Must clean main repo after branches, else we'll have missing pristines.
    doDebug "Finalizing and cleaning master repo."
    createPristineDirectoryTree repo "."
    cleanRepository repo
    readIORef marksref

notdot :: String -> Bool
notdot ('.':_) = False
notdot _ = True

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
