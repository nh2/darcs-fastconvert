module Utils where
import System.IO ( hPutStrLn, stderr )
import Control.Monad.Trans ( MonadIO, liftIO )
import System.Exit
import Darcs.Patch.PatchInfoAnd ( PatchInfoAnd, info )
import Darcs.Patch.Info ( makeFilename )
import qualified Data.ByteString.Char8 as BSC

die :: (MonadIO m) => String -> m a
die str = liftIO (hPutStrLn stderr str >> exitWith (ExitFailure 1))

patchHash :: (PatchInfoAnd p) a b -> BSC.ByteString
patchHash p = BSC.pack $ makeFilename (info p)
