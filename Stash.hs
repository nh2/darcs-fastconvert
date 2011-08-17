module Stash ( parseBranch, ParsedBranchName(..), updateHashes, stashPristine
             , filteredUpdateHashes, markpath, restorePristineFromMark
             , getTentativePristineContents, topLevelBranchDir
             , canRestoreFromMark , stashPristineBS, pb2bn , markInventoryPath
             , markPristinePath, Inventory, getMarkInventory
             , addMarkInventory, emptyMarkInventory, writePristineToPath
             , writeInventoryToPath ) where

import Control.Monad ( forM_ )
import Control.Monad.Trans ( liftIO )
import Control.Monad.State.Strict( gets, modify )
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.IntMap as IM
import Data.Maybe ( fromMaybe )
import Prelude hiding ( readFile, filter, pi )

import Storage.Hashed.Darcs
import Storage.Hashed.Hash( decodeBase16, encodeBase16, sha256, Hash(..) )
import Storage.Hashed.Monad hiding ( createDirectory, exists )
import qualified Storage.Hashed.Tree as T
import qualified Storage.Hashed.Monad as TM
import Storage.Hashed.Tree( Tree, treeHash, readBlob, TreeItem(..), findTree
                          , findTree )
import Storage.Hashed.AnchoredPath( appendPath, floatPath, AnchoredPath(..)
                                  , Name(..), AnchoredPath )

import Darcs.Patch.Info ( PatchInfo(..), showPatchInfo )
import Darcs.Lock ( appendDocBinFile, appendBinFile )

-- An inventory is a info/hash pair.
type Inventory = [(PatchInfo, String)]

type MarkInventories = IM.IntMap Inventory

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

emptyMarkInventory :: MarkInventories
emptyMarkInventory = IM.empty

getMarkInventory :: IM.Key -> MarkInventories -> Maybe Inventory
getMarkInventory = IM.lookup

addMarkInventory :: IM.Key -> Inventory -> MarkInventories -> MarkInventories
addMarkInventory = IM.insert

writeInventoryToPath :: FilePath -> Inventory -> IO ()
writeInventoryToPath invFile inventory = do
  -- We want to write out the inventory in oldest -> newest order but we store
  -- the lists with newest entries first, so we can share tails.
  forM_ (reverse $ inventory) $ \(pi, hash) -> liftIO $ do
    appendDocBinFile invFile $ showPatchInfo pi
    appendBinFile invFile $ "\nhash: " ++ hash ++ "\n"

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

getTentativePristineContents :: TreeIO (BC.ByteString, Tree IO)
getTentativePristineContents = do
  tree' <- (liftIO . darcsAddMissingHashes) =<< updateHashes
  let root = encodeBase16 $ treeHash tree'
  return (root, tree')

maybeReturn :: (a -> TreeIO ()) -> Maybe a -> TreeIO ()
maybeReturn = maybe (return ())

stashPristine ::  Maybe Int -> TreeIO ()
stashPristine mbMark = do
  (pristine, tree') <- getTentativePristineContents
  -- Manually dump the tree.
  liftIO $ writeDarcsHashed tree' "_darcs/pristine.hashed"
  stashPristineBS mbMark pristine

stashPristineBS :: Maybe Int -> BC.ByteString -> TreeIO ()
stashPristineBS mbMark pristine = flip maybeReturn mbMark $
    \mark -> TM.writeFile (markPristinePath mark) $ BL.fromChunks [pristine]

canRestoreFromMark :: Int -> TreeIO Bool
canRestoreFromMark m = TM.fileExists $ markPristinePath m

writePristineToPath :: FilePath -> BC.ByteString -> IO ()
writePristineToPath filePath =
  (BC.writeFile filePath) . BC.append (BC.pack "pristine:")

restorePristineFromMark :: Int -> TreeIO ()
restorePristineFromMark = restorePristine . markPristinePath

restorePristine :: AnchoredPath -> TreeIO ()
restorePristine prisPath = do
  pristine <- TM.readFile prisPath
  let pristineDir = "_darcs/pristine.hashed"
      strictify = B.concat . BL.toChunks
      hash = decodeBase16 . strictify $ pristine
  currentTree <- gets tree
  prisTree <- liftIO $ readDarcsHashedNosize pristineDir hash >>= T.expand
  let darcsDirPath = floatPath "_darcs"
      darcsDir = SubTree `fmap` findTree currentTree darcsDirPath
      combinedTree = T.modifyTree prisTree darcsDirPath darcsDir
  -- We want to keep our marks/stashed inventories/pristines, but
  -- the working dir of the pristine.
  modify $ \s -> s { tree = combinedTree }
