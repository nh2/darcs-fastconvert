{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses, DeriveDataTypeable #-}
module Main ( main ) where

import Control.Monad (when)
import Data.List ( isPrefixOf, isSuffixOf, sort )
import qualified Data.ByteString.Char8 as B
import System.Console.CmdLib
import System.FilePath( takeDirectory, takeBaseName, isAbsolute )
import System.IO( hSetBinaryMode, stdin, stdout, stderr )
import Test.Framework.Providers.API
import Test.Framework
import Shellish hiding ( liftIO, run )
import qualified Shellish

data Running = Running deriving Show
data Result = Success | Skipped | Failed String

instance Show Result where
  show Success = "Success"
  show Skipped = "Skipped"
  show (Failed f) = unlines (map ("| " ++) $ lines f)

instance TestResultlike Running Result where
  testSucceeded Success = True
  testSucceeded Skipped = True
  testSucceeded _ = False

data ShellTest = ShellTest { testfile :: FilePath
                           , testdir  :: Maybe FilePath 
                           , _darcspath :: FilePath
                           }

runtest' :: ShellTest -> FilePath -> ShIO Result
runtest' (ShellTest _ _ dp) srcdir =
  do wd <- pwd
     setenv "HOME" wd
     setenv "EMAIL" "tester"
     getenv "PATH" >>= setenv "PATH" . ((takeDirectory dp ++ ":") ++)
     setenv "DARCS" dp
-- Warning:  A do-notation statement discarded a result of type String.
     _ <- Shellish.run "bash" [ "test" ]
     return Success
   `catch_sh` \e -> case e of
      RunFailed _ 200 _ -> return Skipped
      RunFailed _ _   _ -> Failed <$> B.unpack <$> lastOutput

runtest :: ShellTest -> ShIO Result
runtest t =
 withTmp $ \dir -> do
  cp ("tests" </> testfile t) (dir </> "test")
  srcdir <- pwd
  silently $ sub $ cd dir >> runtest' t srcdir
 where
  withTmp =
   case testdir t of
     Just dir -> \job -> do
       let d = (dir </> takeBaseName (testfile t))
       mkdir_p d
       job d
     Nothing  -> withTmpDir

instance Testlike Running Result ShellTest where
  testTypeName _ = "Shell"
  runTest _ test = runImprovingIO $ do yieldImprovement Running
                                       liftIO (shellish $ runtest test)

shellTest :: FilePath -> Maybe FilePath -> String -> Test
shellTest dp tdir file = Test file $ ShellTest file tdir dp

findShell :: FilePath -> Maybe FilePath -> ShIO [Test]
findShell dp tdir =
  do files <- sort <$> grep (".sh" `isSuffixOf`) <$> ls "tests"
     return [ shellTest dp tdir file | file <- files ]

-- ----------------------------------------------------------------------
-- harness
-- ----------------------------------------------------------------------

data Config = Config { darcs :: String
                     , tests :: [String]
                     , testDir :: Maybe FilePath
                     , plain :: Bool
                     , threads :: Int }
            deriving (Data, Typeable, Eq)

instance Attributes Config where
  attributes _ = group "Options"
    [ tests %> Help "Pattern to limit the tests to run." %+ short 't'
    , testDir %> Help "Directory to run tests in" %+ Default (Nothing :: Maybe FilePath)
    , plain %> Help "Use plain-text output."
    , threads %> Default (1 :: Int) %+ short 'j' ]

data DarcsFCTest = DarcsFCTest deriving Typeable
instance Command DarcsFCTest (Record Config) where
  run _ conf _ = do
    let args = [ "-j", show $ threads conf ] 
             ++ concat [ ["-t", x ] | x <- tests conf ]
             ++ [ "--plain" | True <- [plain conf] ]
    case testDir conf of
       Nothing -> return ()
       Just d  -> do e <- shellish (test_e d)
                     when e $ fail ("Directory " ++ d ++ " already exists. Cowardly exiting")
    tests <- shellish $ findShell (darcs conf) (testDir conf) 
    defaultMainWithArgs tests args

main :: IO ()
main = do hSetBinaryMode stdout True
          hSetBinaryMode stderr True
          hSetBinaryMode stdin True
          getArgs >>= execute DarcsFCTest
