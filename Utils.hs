module Utils where

import Control.Monad.Trans ( MonadIO, liftIO )
import qualified Data.ByteString.Char8 as BSC
import System.Exit
import System.IO ( hPutStrLn, stderr )

import Darcs.Patch.PatchInfoAnd ( PatchInfoAnd, info )
import Darcs.Patch.Info ( makeFilename )

die :: (MonadIO m) => String -> m a
die str = liftIO (hPutStrLn stderr str >> exitWith (ExitFailure 1))

patchHash :: (PatchInfoAnd p) a b -> BSC.ByteString
patchHash p = BSC.pack $ makeFilename (info p)
