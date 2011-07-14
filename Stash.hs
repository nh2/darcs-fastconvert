module Stash ( restoreFromMark, restoreFromBranch, stashInventoryAndPristine
             , parseBranch, ParsedBranchName(..), updateHashes, stashPristine
             , filteredUpdateHashes, markpath, restorePristineFromMark
             , getTentativePristineContents, branchInventoryPath
             , branchPristinePath, topLevelBranchDir, canRestoreFromMark
             , canRestoreFromBranch, stashPristineBS, stashInventoryBS
             , pb2bn ) where

import Control.Monad.Trans ( liftIO )
import Control.Monad.State.Strict( gets, modify )
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Maybe ( fromMaybe )
import Prelude hiding ( readFile, filter )
import System.FilePath ( (</>) )

import Storage.Hashed.Darcs
import Storage.Hashed.Hash( decodeBase16, encodeBase16, sha256, Hash(..) )
import Storage.Hashed.Monad hiding ( createDirectory, exists )
import qualified Storage.Hashed.Tree as T
import qualified Storage.Hashed.Monad as TM
import Storage.Hashed.Tree( Tree, treeHash, readBlob, TreeItem(..), findTree
                          , findTree )
import Storage.Hashed.AnchoredPath( appendPath, floatPath, AnchoredPath(..)
                                  , Name(..), AnchoredPath )

data ParsedBranchName = ParsedBranchName B.ByteString deriving (Show, Eq)

-- Branch names are one of:
-- refs/heads/branchName
-- refs/remotes/remoteName
-- refs/tags/tagName
-- we want: branch-branchName
--          remote-remoteName
--          tag-tagName
parseBranch :: BC.ByteString -> ParsedBranchName
parseBranch b = ParsedBranchName $ BC.concat
  [bType, BC.pack "-", BC.drop 2 bName] where
    (bType, bName) = BC.span (/= 's') . BC.drop 5 $ b

pb2bn :: ParsedBranchName -> B.ByteString
pb2bn (ParsedBranchName name) = name


topLevelBranchDir :: String -> ParsedBranchName -> FilePath
topLevelBranchDir repodir (ParsedBranchName b) =
  concat [repodir, "-", BC.unpack b]

updateHashes :: TreeIO (Tree IO)
updateHashes = filteredUpdateHashes Nothing

filteredUpdateHashes :: Maybe (AnchoredPath -> TreeItem IO -> Bool)
  -> TreeIO (Tree IO)
filteredUpdateHashes mbFilter = do
  let nodarcs (AnchoredPath (Name x:_)) _ = x /= BC.pack "_darcs"
      hashblobs (File blob@(T.Blob con NoHash)) =
        do hash <- sha256 `fmap` readBlob blob
           return $ File (T.Blob con hash)
      hashblobs x = return x
      filter = fromMaybe nodarcs mbFilter
  tree' <- liftIO . T.partiallyUpdateTree hashblobs filter =<<
    gets tree
  modify $ \s -> s { tree = tree' }
  return $ T.filter nodarcs tree'

-- sort marks into buckets, since there can be a *lot* of them
markpath :: Int -> AnchoredPath
markpath n = floatPath "_darcs/marks"
                `appendPath` (Name $ BC.pack $ show (n `div` 1000))
                `appendPath` (Name $ BC.pack $ show (n `mod` 1000))

markPristinePath :: Int -> AnchoredPath
markPristinePath n = markpath n `appendPath`
          (Name $ BC.pack "pristine")

markInventoryPath :: Int -> AnchoredPath
markInventoryPath n = markpath n `appendPath`
  (Name $ BC.pack "inventory")

branchPath :: ParsedBranchName -> AnchoredPath
branchPath (ParsedBranchName b) = floatPath "_darcs/branches/"
  `appendPath` Name b

branchInventoryPath :: ParsedBranchName -> AnchoredPath
branchInventoryPath b = branchPath b `appendPath`
  Name (BC.pack "tentative_hashed_inventory")

branchPristinePath :: ParsedBranchName -> AnchoredPath
branchPristinePath b = branchPath b `appendPath`
  Name (BC.pack "tentative_pristine")

branchMarkPath :: ParsedBranchName -> AnchoredPath
branchMarkPath b = branchPath b `appendPath`
  Name (BC.pack "last_mark")

getTentativePristineContents :: TreeIO (BL.ByteString, Tree IO)
getTentativePristineContents = do
  tree' <- (liftIO . darcsAddMissingHashes) =<< updateHashes
  let root = encodeBase16 $ treeHash tree'
  return (BL.fromChunks [BC.concat [BC.pack "pristine:", root]], tree')

maybeReturn :: (a -> TreeIO ()) -> Maybe a -> TreeIO ()
maybeReturn = maybe (return ())

stashPristine ::  Maybe Int -> Maybe ParsedBranchName -> TreeIO ()
stashPristine mbMark mbBranch = do
  (pristine, tree') <- getTentativePristineContents
  -- Manually dump the tree.
  liftIO $ writeDarcsHashed tree' "_darcs/pristine.hashed"
  flip maybeReturn mbMark $ \mark -> do
    TM.writeFile (markPristinePath mark) pristine
    maybeReturn (\b -> TM.writeFile (branchMarkPath b) $ BL.pack $ show mark)
      mbBranch
  maybeReturn (\b -> TM.writeFile (branchPristinePath b) pristine) mbBranch

stashInventory :: Maybe Int -> Maybe ParsedBranchName -> TreeIO ()
stashInventory mbMark mbBranch = do
  inventory <- liftIO $ BL.readFile "_darcs/tentative_hashed_inventory"
  maybeReturn (\m -> TM.writeFile (markInventoryPath m) inventory) mbMark
  maybeReturn (\b -> TM.writeFile (branchInventoryPath b) inventory) mbBranch

stashInventoryAndPristine :: Maybe Int -> Maybe ParsedBranchName -> TreeIO ()
stashInventoryAndPristine mbMark mbBranch = do
  stashPristine mbMark mbBranch
  stashInventory mbMark mbBranch

restorePristineFromMark :: String -> Int -> TreeIO ()
restorePristineFromMark pref m = restorePristine pref $ markPristinePath m

restoreInventoryFromMark :: String -> Int -> TreeIO ()
restoreInventoryFromMark pref m = restoreInventory pref $ markInventoryPath m

restoreFromMark :: String -> Int -> TreeIO Int
restoreFromMark pref m = do
  -- TODO: If the restore point has already been exported, we won't have the
  -- mark in our TreeMonad, so we will have to do a 'get --to-patch' matching
  -- the patch corresponding to the mark as per the marks file. If we don't
  -- have the mark in the marks file, then fail.
  restorePristineFromMark pref m
  restoreInventoryFromMark pref m
  return m

restoreFromBranch :: String -> ParsedBranchName -> TreeIO Int
restoreFromBranch pref b = do
  -- TODO: if the branch we want to reset to has not been touched in the
  -- current import stream then this will fail since we will be missing the
  -- pristine/inventory files in the TreeMonad. We can instead just pull/get
  -- from the branch directory itself. May need the config for that.
  restoreInventory pref $ branchInventoryPath b
  restorePristine pref $ branchPristinePath b
  branchMark <- TM.readFile $ branchMarkPath b
  let Just (m, _) = BL.readInt branchMark
  return m

restoreInventory :: String -> AnchoredPath -> TreeIO ()
restoreInventory pref invPath = do
  inventory <- readFile invPath
  liftIO $ BL.writeFile
    ("_darcs" </> pref ++ "tentative_hashed_inventory") inventory

restorePristine :: String -> AnchoredPath -> TreeIO ()
restorePristine pref prisPath = do
  pristine <- TM.readFile prisPath
  liftIO $ BL.writeFile
    ("_darcs" </> pref ++ "tentative_pristine") pristine
  let prefixLen = fromIntegral $ length ("pristine:" :: String)
      pristineDir = "_darcs/pristine.hashed"
      strictify = B.concat . BL.toChunks
      hash = decodeBase16 . strictify . BL.drop prefixLen $ pristine
  currentTree <- gets tree
  prisTree <- liftIO $
    readDarcsHashedNosize pristineDir hash >>= T.expand
  let darcsDirPath = floatPath "_darcs"
      darcsDir = SubTree `fmap` findTree currentTree darcsDirPath
      combinedTree = T.modifyTree prisTree darcsDirPath darcsDir
  -- We want to keep our marks/stashed inventories/pristines, but
  -- the working dir of the pristine.
  modify $ \s -> s { tree = combinedTree }
