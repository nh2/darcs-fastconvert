module Utils where
import System.IO ( hPutStrLn, stderr )
import Control.Monad.Trans ( liftIO )
import System.Exit
import Darcs.Patch.PatchInfoAnd ( info )
import Darcs.Patch.Info ( makeFilename )
import qualified Data.ByteString.Char8 as BSC

die str = liftIO (hPutStrLn stderr str >> exitWith (ExitFailure 1))
patchHash p = BSC.pack $ makeFilename (info p)
