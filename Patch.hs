{-# LANGUAGE GADTs, ScopedTypeVariables, FlexibleContexts, TypeFamilies  #-}
import Control.Exception as E
import Data.Attoparsec.Combinator( many' )
import Storage.Hashed.Tree ( Tree )
import Darcs.Patch.Apply ( ApplyState )
applyChanges :: forall p r u . (RepoPatch p, ApplyState (PrimOf p) ~ Tree, ApplyState p ~ Tree)
             => Bool -> PatchInfo -> [Change] -> Repository p r u r -> IO ()
      apply p `E.catch` \(_ :: SomeException) -> do
parseGitEmail h = go (A.parse $ many' p_gitPatch) BC.empty h where
    diffs <- many' p_diff
    many' p_unifiedHunk