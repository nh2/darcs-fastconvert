module Utils where

import Control.Monad.Trans ( MonadIO, liftIO )
import qualified Data.ByteString.Char8 as BSC
import System.Exit
import System.IO ( hPutStrLn, stderr )
import System.FilePath ( takeFileName )

import Darcs.Patch ( RepoPatch )
import Darcs.Patch.PatchInfoAnd ( PatchInfoAnd, info )
import Darcs.Patch.Set ( Origin )
import Darcs.Patch.Info ( makeFilename )
import Darcs.Witnesses.Eq ( (=\/=), isIsEq )
import Darcs.Witnesses.Ordered ( FL(..) )

die :: (MonadIO m) => String -> m a
die str = liftIO $ do
  hPutStrLn stderr ("FATAL: " ++ str)
  exitWith (ExitFailure 1)

patchHash :: (PatchInfoAnd p) a b -> BSC.ByteString
patchHash p = BSC.pack $ makeFilename (info p)

fp2bn :: FilePath -> String
fp2bn = takeFileName

-- |equalHead checks that the first patch of two distinct FLs is equal,
-- giving a common branch-base that we can reset to in the output stream.
equalHead :: (RepoPatch p) => FL (PatchInfoAnd p) Origin cX
  -> FL (PatchInfoAnd p) Origin cY -> Bool
equalHead (x :>: _) (y :>: _) = isIsEq $ x =\/= y
equalHead _     _ = False
