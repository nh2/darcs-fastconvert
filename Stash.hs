module Stash ( restoreFromMark, stashInventoryAndPristine
             , parseBranch, ParsedBranchName(..), updateHashes, stashPristine
             , filteredUpdateHashes, markpath, restorePristineFromMark
             , getTentativePristineContents, topLevelBranchDir
             , canRestoreFromMark , stashPristineBS, stashInventoryBS, pb2bn
             , markInventoryPath, markPristinePath ) where

import Control.Monad.Trans ( liftIO )
import Control.Monad.State.Strict( gets, modify )
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Maybe ( fromMaybe )
import Prelude hiding ( readFile, filter )
import System.FilePath ( (</>) )

import System.PosixCompat.Files ( fileSize, getFileStatus )
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
topLevelBranchDir repodir (ParsedBranchName b) = case BC.unpack b of
  "head-master" -> repodir
  branchName -> concat [repodir, "-", branchName]

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

getTentativePristineContents :: TreeIO (BL.ByteString, Tree IO)
getTentativePristineContents = do
  tree' <- (liftIO . darcsAddMissingHashes) =<< updateHashes
  let root = encodeBase16 $ treeHash tree'
  return (BL.fromChunks [BC.concat [BC.pack "pristine:", root]], tree')

maybeReturn :: (a -> TreeIO ()) -> Maybe a -> TreeIO ()
maybeReturn = maybe (return ())

stashPristine ::  Maybe Int -> TreeIO ()
stashPristine mbMark = do
  (pristine, tree') <- getTentativePristineContents
  -- Manually dump the tree.
  liftIO $ writeDarcsHashed tree' "_darcs/pristine.hashed"
  stashPristineBS mbMark pristine

stashPristineBS :: Maybe Int -> BL.ByteString -> TreeIO ()
stashPristineBS mbMark pristine = flip maybeReturn mbMark $
    \mark -> TM.writeFile (markPristinePath mark) pristine

stashInventory :: Maybe Int -> TreeIO ()
stashInventory mbMark = do
  inventory <- liftIO $ BL.readFile "_darcs/tentative_hashed_inventory"
  --fileSize <- liftIO $ fileSize `fmap` getFileStatus "_darcs/tentative_hashed_inventory"
  --liftIO $ putStrLn $ "progress inventory size: " ++ show ((fromRational (toRational(fileSize) / toRational(1024 * 1024))) :: Double)
  stashInventoryBS mbMark inventory

stashInventoryBS :: Maybe Int -> BL.ByteString -> TreeIO ()
stashInventoryBS mbMark inventory = do
  maybeReturn (\m -> TM.writeFile (markInventoryPath m) inventory) mbMark

stashInventoryAndPristine :: Maybe Int -> TreeIO ()
stashInventoryAndPristine mbMark = do
  stashPristine mbMark
  stashInventory mbMark

restorePristineFromMark :: String -> Int -> TreeIO ()
restorePristineFromMark pref m = restorePristine pref $ markPristinePath m

restoreInventoryFromMark :: String -> Int -> TreeIO ()
restoreInventoryFromMark pref m = restoreInventory pref $ markInventoryPath m

restoreFromMark :: String -> Int -> TreeIO Int
restoreFromMark pref m = do
  liftIO $ putStrLn $ "Restoring from mark: " ++ show m ++ " with pref: " ++ pref
  restorePristineFromMark pref m
  restoreInventoryFromMark pref m
  return m

canRestoreFromMark :: Int -> TreeIO Bool
canRestoreFromMark m = do
  prisExists <- TM.fileExists $ markPristinePath m
  invExists <- TM.fileExists $ markInventoryPath m
  return $ prisExists && invExists

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
