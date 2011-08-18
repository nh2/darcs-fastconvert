{-# LANGUAGE GADTs, OverloadedStrings, Rank2Types, ScopedTypeVariables, TypeFamilies  #-}
module Export( fastExport ) where

import Marks
import Utils
import Stash

import Codec.Compression.GZip ( compress )
import Control.Applicative ( (<*>) )
import Control.Monad ( when, forM, forM_, unless )
import Control.Monad.Trans ( liftIO, lift )
import Control.Monad.State.Class ( gets, modify )
import Control.Monad.Reader.Class ( asks )
import Control.Monad.RWS.Strict ( RWST(..), evalRWST )
import Control.Exception( finally )
import Data.List ( sort, intercalate, isPrefixOf, sortBy, (\\), nub )
import Data.Maybe ( catMaybes, fromJust, isJust )
import Data.Ord ( comparing )
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.ByteString.Lazy.UTF8 as BLU
import Data.ByteString.Base64 ( encode )
import Data.IORef ( newIORef, modifyIORef, readIORef )
import Numeric ( showHex )
import Prelude hiding ( readFile, pi )
import Printer ( renderString )
import System.Directory ( canonicalizePath )
import System.Random ( randomRIO )

import Darcs.Repository ( Repository, RepoJob(..), readRepo, withRepository )
import Darcs.Repository.Cache ( HashedDir( HashedPristineDir ) )
import Darcs.Repository.HashedRepo ( readHashedPristineRoot )
import Darcs.Repository.HashedIO ( cleanHashdir )
import Darcs.Repository.Internal ( identifyRepositoryFor )
import Darcs.Repository.InternalTypes ( extractCache )
import Darcs.Patch ( effect, listTouchedFiles, apply, RepoPatch, showPatch
                   , getdeps )
import Darcs.Patch.Commute ( commuteFL )
import Darcs.Patch.Apply ( ApplyState )
import Darcs.Patch.Effect ( Effect )
import Darcs.Patch.Info ( isTag, PatchInfo, piAuthor, piName, piLog, piDate
                        , makePatchname )
import Darcs.Patch.PatchInfoAnd ( PatchInfoAnd, info, hopefully )
import Darcs.Patch.Prim.Class ( PrimOf, primIsTokReplace, PrimClassify(..) )
import Darcs.Patch.Set ( newset2FL )
import Darcs.Witnesses.Ordered ( FL(..), RL(..), nullFL, mapFL, spanFL
                               , (:>)(..), foldlFL, reverseRL, lengthFL )
import Darcs.Witnesses.Sealed ( seal2, Sealed2(..), seal , flipSeal
                              , Sealed(..), FlippedSeal(..) )
import Darcs.Witnesses.Unsafe ( unsafeCoercePStart )
import Darcs.Utils ( withCurrentDirectory )

import Storage.Hashed.Darcs
import Storage.Hashed.Monad hiding ( createDirectory, exists )
import Storage.Hashed.Tree( findTree, emptyTree, listImmediate, findTree, Tree )
import Storage.Hashed.AnchoredPath( anchorPath, appendPath, floatPath
                                  , AnchoredPath(..) )

import SHA1 ( sha1PS )

-- 'from' mark, name, current context, original patch FL, and the remaining
-- to-be-exported patches.
data Branch p = Branch Int String String (Sealed2 (FL (PatchInfoAnd p)))
  (Sealed2 (FL (PatchInfoAnd p)))

data ExportState = ExportState { lastExportedMark :: Int
                               , nextMark :: Int
                               }

data ExportReader =
  ExportReader { ctx2mark :: String -> ExportM (Maybe Int)
               , mark2bName :: Int -> ExportM (Maybe String)
               , marker :: forall p x y . (PatchInfoAnd p) x y -> Int
                            -> String -> String -> ExportM ()
               , printer :: BLU.ByteString -> ExportM ()
               , existingMarks :: Marks
               }

type ExportM a = RWST ExportReader () ExportState TreeIO a

incrementMark :: (RepoPatch p) => PatchInfoAnd p cX cY -> ExportM Int
incrementMark p = do
  currentNext <- gets nextMark
  let newNext = next currentNext p
  when (currentNext /= newNext) $
    modify (\s -> s {lastExportedMark = currentNext, nextMark = newNext})
  return currentNext

mergeTagString :: String
mergeTagString = "TAG darcs-fastconvert merge "

isAnyMergeTag :: PatchInfoAnd p x y -> Bool
isAnyMergeTag p = or $ [isJust . isMergeBeginTag
                       , isMergeSourceTag ""
                       , isMergeEndTag ""] <*> [p]

isMergeBeginTag :: PatchInfoAnd p x y -> Maybe String
isMergeBeginTag p | not $ isTag (info p) = Nothing
isMergeBeginTag p = extractMergeID $ patchName p where
  extractMergeID m = if mergeTag `isPrefixOf` m
                       then Just $ drop (length mergeTag) m
                       else Nothing
  mergeTag = mergeTagString ++ "pre-target: "

isMergeSourceTag :: String -> PatchInfoAnd p x y -> Bool
isMergeSourceTag mergeID p = isMergeTag p $ "pre-source: " ++ mergeID

isMergeEndTag :: String -> PatchInfoAnd p x y -> Bool
isMergeEndTag mergeID p = isMergeTag p $ "post: " ++ mergeID

isMergeTag :: PatchInfoAnd p x y -> String -> Bool
isMergeTag p _ | not $ isTag (info p) = False
isMergeTag p mergeID = (mergeTagString ++ mergeID) `isPrefixOf` patchName p

isProperTag :: (Effect p) => (PatchInfoAnd p) x y -> Bool
isProperTag p = isTag (info p) && nullFL (effect p)

next :: (Effect p) => Int -> (PatchInfoAnd p) x y -> Int
next n p = if isProperTag p then n else n + 1

tagName :: (PatchInfoAnd p) x y -> String
tagName = map cleanup . drop 4 . patchName
  where cleanup x | x `elem` bad = '_'
                  | otherwise = x
        -- FIXME many more chars are probably illegal
        bad = " ."

patchName :: (PatchInfoAnd p) x y -> String
patchName = piName . info

patchAuthor :: (PatchInfoAnd p) x y -> String
patchAuthor p = case span (/='<') author of
  (_, "") -> case span (/='@') author of
                 -- john@home -> john <john@home>
                 (n, "") -> n ++ " <unknown>"
                 (name, _) -> name ++ " <" ++ author ++ ">"
  (n, rest) -> case span (/='>') $ tail rest of
    (email, _) -> n ++ "<" ++ email ++ ">"
 where author = piAuthor (info p)

patchMessage :: (PatchInfoAnd p) x y -> BLU.ByteString
patchMessage p = BL.concat [ BLU.fromString (piName $ info p)
                           , case unlines . piLog $ info p of
                                "" -> BL.empty
                                plog -> BLU.fromString ('\n' : plog)]

dumpBits :: [BLU.ByteString] -> ExportM ()
dumpBits bs = do
  doPrint <- asks printer
  doPrint $ BL.intercalate "\n" bs

dumpFiles :: [AnchoredPath] -> ExportM ()
dumpFiles files = forM_ files $ \file -> do
  isfile <- lift $ fileExists file
  isdir <- lift $ directoryExists file
  when isfile $ do
    bits <- lift $ readFile file
    dumpBits [ BLU.fromString $ "M 100644 inline " ++ anchorPath "" file
             , BLU.fromString $ "data " ++ show (BL.length bits)
             , bits ]
  when isdir $ do tt <- lift $ gets tree -- ick
                  let subs = [ file `appendPath` n | (n, _) <-
                                  listImmediate $ fromJust $ findTree tt file ]
                  dumpFiles subs
  when (not isfile && not isdir) $ do
    doPrint <- asks printer
    doPrint . BLU.fromString $ "D " ++ anchorPath "" file

generateInfoIgnores :: forall p x y . (RepoPatch p) => PatchInfoAnd p x y
  -> BL.ByteString
generateInfoIgnores p =
  packIgnores . takeStartEnd . mapFL renderReplace $ effect p
  where renderReplace :: (PrimOf p) cX cY -> Maybe String
        renderReplace pr | primIsTokReplace pr =
          Just . renderString . showPatch $ pr
        renderReplace _ = Nothing
        -- We can only handle replaces at the start and end, since we cannot
        -- represent the intermediate states, which we would need to recover
        -- the prims that were originally applied.
        takeStartEnd ps = catMaybes $ starts ++ ends where
          (startJusts, ps') = span isJust ps
          (endJusts, _) = span isJust $ reverse ps'
          prefixer pref = map (fmap (pref ++))
          starts = prefixer "S " startJusts
          ends = prefixer "E " $ reverse endJusts
        packIgnores :: [String] -> BL.ByteString
        packIgnores [] = BL.empty
        packIgnores is = BLC.fromChunks [prefix `BC.append` base64ps] where
          prefix = BC.pack "\ndarcs-patches: "
          gzipped = compress . BLC.pack $ intercalate "\n" is
          base64ps = encode . BC.concat . BLC.toChunks $ gzipped

dumpPatch :: (RepoPatch p) => String -> (PatchInfoAnd p) cX cY -> String
  -> ExportM Int
dumpPatch ctx p bName = do
  from <- gets lastExportedMark
  mark <- incrementMark p
  stashPristineAtMark mark
  dumpBits [ BLC.pack $ "progress " ++ show mark ++ ": " ++ patchName p
              , BLC.pack $ "commit refs/heads/" ++ bName ]
  let message = patchMessage p `BL.append` generateInfoIgnores p
  asks marker >>= \m -> m p mark bName ctx
  dumpBits
     [ BLU.fromString $ "committer " ++ patchAuthor p ++ " " ++ patchDate p
     , BLU.fromString $ "data " ++ show (BL.length message + 1)
     , message ]
  when (mark > 1) $ dumpBits [ BLU.fromString $ "from :" ++ show from]
  dumpFiles . map floatPath $ listTouchedFiles p
  return mark

dumpTag :: (PatchInfoAnd p) cX cY -> ExportM ()
dumpTag p | isAnyMergeTag p = return ()
dumpTag p = do
  from <- gets lastExportedMark
  dumpBits
    [ BLU.fromString $ "progress TAG " ++ tagName p
    , BLU.fromString $ "tag refs/tags/" ++ tagName p -- FIXME is this valid?
    , BLU.fromString $ "from :" ++ show from
    , BLU.fromString $ "tagger " ++ patchAuthor p ++ " " ++ patchDate p
    , BLU.fromString $ "data " ++ show (BL.length (patchMessage p) - 4 + 1)
    , BL.drop 4 $ patchMessage p ]

reset :: Maybe Int -> String -> ExportM ()
reset mbMark branch = do
    doPrint <- asks printer
    doPrint . BLC.pack $ "reset refs/heads/" ++ branch
    maybe (return ()) (\m -> doPrint . BLC.pack $ "from :" ++ show m) mbMark
    doPrint BLC.empty

hashPatch :: String -> PatchInfoAnd p cX cY -> String
hashPatch ctx p = let pInfo = info p in
  if isTag pInfo
    then ctx
    else hashInfo ctx pInfo

hashInfo :: String -> PatchInfo -> String
hashInfo ctx = sha1PS . BC.pack . (ctx ++) . makePatchname

--  if current > lastMark existingMarks
--    then if inOrderTag tags p
--           then dumpTag printer p from
--           else dumpPatch ctx' printer ctx2mark doMark p from current bName
--    else unless (inOrderTag tags p ||
--          (fstTriple `fmap` getMark existingMarks current == Just (patchHash p))) $
--           die . unwords $ ["Marks do not correspond: expected"
--                           , show (getMark existingMarks current), ", got "
--                           , BC.unpack (patchHash p)]

restoreBranch :: Int -> ExportM ()
restoreBranch bFrom = do
  lift $ restorePristineFromMark bFrom
  -- Ensure any exported patches are based on the *branch's* previous
  -- commit, not the globally previous commit.
  modify (\s -> s {lastExportedMark = bFrom})

dumpBranch :: forall p . (RepoPatch p, ApplyState p ~ Tree)
           => Branch p -> [Branch p] -> ExportM ()
dumpBranch (Branch bFrom bName _ _ (Sealed2 NilFL)) bs = do
    -- Reset, so branch heads are up-to-date at the end of the export.
    reset (Just bFrom) bName
    unless (null bs) $ dumpBranch (head bs) (tail bs)

dumpBranch (Branch bFrom bName bCtx (Sealed2 bOrigPs) (Sealed2(bp:>:bps))) bs =
  do
    let incBranch m = Branch m bName bCtx (seal2 bOrigPs) (seal2 bps)
    ctxToMark <- asks ctx2mark
    from <- gets lastExportedMark
    when (from /= bFrom) $ restoreBranch bFrom
    lift $ apply bp
    if isProperTag bp
      then case isMergeBeginTag bp of
        Just mergeID -> do
          let mbMerged = findMergeEndTag mergeID bps
          case mbMerged of
            -- If we can't find the matching merge tag, warn, and export the
            -- patches as if there was no merge.
            Nothing -> do
              progressWarn [ "could not find merge end tag with mergeID:"
                           , mergeID, "merge will be omitted." ]
              dumpBranch (incBranch bFrom) bs
            Just (Sealed mPs, Sealed2 endTag, FlippedSeal bps') ->
              case partitionMergesAndResolutions mergeID mPs of
                -- If we can't find any source tags, we can't recreate the
                -- pre-merge context, so warn, and export the remaining patches
                -- without the merge.
                Nothing -> do
                  progressWarn [ "could not find merge source tag(s) with"
                               , "mergeID:", mergeID, "merge will be omitted."]
                  dumpBranch (incBranch bFrom) bs
                Just (merges, Sealed2 resolutions) -> do
                  currentFrom <- gets lastExportedMark
                  let mergeTagInfo = info bp
                  -- We can safely use fromJust since we know the tag is
                  -- present as we have already hit it.
                  (Sealed origPsUpToMerge) <- return . fromJust $
                        tryTakeUntilFL ((== mergeTagInfo) . info) bOrigPs
                  mergeMarks <-
                    dumpMergeSources bName (seal2 origPsUpToMerge) merges
                  -- Reset our current state since it could have been changed
                  -- if some merge sources hadn't been exported.
                  lift $ restorePristineFromMark bFrom
                  modify (\s -> s {lastExportedMark = currentFrom})
                  -- mPs includes all merged patches, and any resolutions
                  lift $ apply mPs
                  let branchCtx = fl2Ctx bCtx mPs
                      touchedFiles = nub . concat $ mapFL listTouchedFiles mPs
                      dumpMergeP = dumpMergePatch branchCtx resolutions endTag
                                     mergeMarks touchedFiles bName
                  mbCtxMark <- ctxToMark branchCtx
                  -- Only dump the merge patch if it hasn't been already.
                  -- Otherwise, stash the pristine and update the last mark.
                  newFrom <- maybe dumpMergeP stashUpdateLastMark mbCtxMark
                  let newBranch = Branch newFrom bName branchCtx
                        (seal2 bOrigPs) (seal2 bps')
                  dumpBranch newBranch bs
        Nothing -> do
          unless (isAnyMergeTag bp) $ dumpTag bp
          dumpBranch (incBranch bFrom) bs
      else do
        let nextCtx = hashPatch bCtx bp
            patchDumper = dumpPatch nextCtx bp bName
        mbCtxMark <- ctxToMark nextCtx
        newFrom <- maybe patchDumper stashUpdateLastMark mbCtxMark
        dumpBranch (Branch newFrom bName nextCtx (seal2 bOrigPs) (seal2 bps) ) bs
  where
    progressWarn ws = asks printer >>=
      \p -> p . BLC.pack . unwords . ("progress WARNING:" :) $ ws

    -- |stashUpdateLastMark stashes the current pristine for the given mark,
    -- and updates the state, setting the last exported mark as the given mark.
    stashUpdateLastMark m = do
      stashPristineAtMark m
      modify (\s -> s { lastExportedMark = m })
      return m

    -- TODO: this sucks, we know that the resulting Just tuple is of this form:
    -- (FL (PIAP p) a b, PIAP p b c, FL (PIAP p) c d) but I don't think I can
    -- prove it...
    findMergeEndTag :: String -> FL (PatchInfoAnd p) x z
      -> Maybe (Sealed(FL (PatchInfoAnd p) x),
                Sealed2(PatchInfoAnd p),
                FlippedSeal(FL (PatchInfoAnd p)) z)
    findMergeEndTag _ NilFL = Nothing
    findMergeEndTag mergeID ps =
      case spanFL (not . isMergeEndTag mergeID) ps of
        _ :> NilFL             -> Nothing
        merged :> (t :>: rest) -> Just (seal merged, seal2 t, flipSeal rest)

    -- The input FL should be of the form:
    -- {P1_1,P1_2,T1,P2_1,P2_2,T2,[...],R1,R2} where each Ti is a merge-source
    -- tag, determining the source context before the merge. Each Ri is a
    -- resolution patch, resolving any conflicts of the merge(s). We will split
    -- the FL into: ([({P1_1,P1_2},T1),({P2_1,P2_2},T2)], {R1, R2}) i.e. a list
    -- of merges (with the final tag separated) and the resolution patches.
    partitionMergesAndResolutions :: String -> FL (PatchInfoAnd p) x y ->
      Maybe ( [( Sealed2(FL (PatchInfoAnd p))
               , Sealed2(PatchInfoAnd p))]
            , Sealed2(FL(PatchInfoAnd p)))
    partitionMergesAndResolutions _ NilFL = Nothing
    partitionMergesAndResolutions mergeID ps =
      case spanFL (not . isMergeSourceTag mergeID) ps of
        _ :> NilFL -> Nothing
        sourcePs :> (sourceTag :>: ps') ->
          case partitionMergesAndResolutions mergeID ps' of
            Nothing -> Just ([(seal2 sourcePs, seal2 sourceTag)], seal2 ps')
            Just (sources, rs) ->
              Just ((seal2 sourcePs, seal2 sourceTag) : sources, rs)

    -- |dumpMergeSources ensures that each of a list of merge sources (a FL and
    -- end-tag pair) have been dumped, returning a list of the marks
    -- corresponding to the branch tips.
    dumpMergeSources :: (RepoPatch p) => String -> Sealed2(FL (PatchInfoAnd p))
      -> [(Sealed2(FL (PatchInfoAnd p)), Sealed2(PatchInfoAnd p))]
      -> ExportM [Int]
    dumpMergeSources _ _ [] = return []
    dumpMergeSources bName targetPs ((Sealed2 ps, Sealed2 tag) : ss) = do
      let dependencyPIs = reverse . getdeps . hopefully $ tag
          newPIs = mapFL info ps
          ctxPIs = dependencyPIs \\ newPIs
      checkOrDumpPatches bName ctxPIs targetPs
      let targetPsList = flToList targetPs
          mbOriginalPatches = commuteOutNonDeps targetPsList newPIs ps
      branchOriginalPatches <- case mbOriginalPatches of
               Just x -> return x
               Nothing -> die $ "Couldn't commute merged patches back into "
                                 ++ "their original form."
      sourceMark <-
        checkOrDumpPatches bName dependencyPIs branchOriginalPatches
      others <- dumpMergeSources bName targetPs ss
      return $ sourceMark : others

    flToList :: Sealed2(FL (PatchInfoAnd p)) -> [Sealed2 (PatchInfoAnd p)]
    flToList (Sealed2 NilFL) = []
    flToList (Sealed2 (p :>: ps)) = seal2 p : flToList (seal2 ps)

    -- |commuteListPastFL takes a list of individual patches and a FL of
    -- patches and attempts to commute each individual patch past the FL.
    commuteListPastFL :: [Sealed2(PatchInfoAnd p)] -> FL (PatchInfoAnd p) cC cD
      -> Maybe(Sealed2(FL (PatchInfoAnd p)))
    commuteListPastFL [] ps = Just $ seal2 ps
    commuteListPastFL (Sealed2 x : xs) ps =
      case commuteFL (x :> unsafeCoercePStart ps) of
        Just (ps' :> _) -> commuteListPastFL xs ps'
        Nothing -> Nothing

    -- |commuteOutNonDeps takes a FL of patches representing the merge target,
    -- a list of infos corresponding to the context of the merged patches and
    -- a FL of merged patches. Any patches from the merge target that are not
    -- in the context of the merged patches need to be commuted out
    -- (particularly in the case of conflicts, so we can output the merged-in
    -- patches in their original form).
    commuteOutNonDeps :: [Sealed2(PatchInfoAnd p)] -> [PatchInfo]
      -> FL (PatchInfoAnd p) cC cD -> Maybe(Sealed2(FL (PatchInfoAnd p)))
    commuteOutNonDeps [] [] mergedPatches = Just $ seal2 mergedPatches
    commuteOutNonDeps ps [] mergedPatches = commuteListPastFL ps mergedPatches
    -- There were PatchInfos that we didn't find, to commute out, so fail.
    commuteOutNonDeps [] _ _ = Nothing
    commuteOutNonDeps (Sealed2 p : ss) (pi : pis) mergedPatches =
      if info p == pi
        then commuteOutNonDeps ss pis mergedPatches
        else case commuteFL (p :> unsafeCoercePStart mergedPatches) of
          Just (mergedPatches' :> _) -> commuteOutNonDeps ss pis mergedPatches'
          Nothing -> Nothing

    -- |checkOrDumpPatches ensures that a given list of PatchInfos are dumped,
    -- either now, or previously.
    checkOrDumpPatches :: String -> [PatchInfo] -> Sealed2(FL (PatchInfoAnd p))
      -> ExportM Int
    checkOrDumpPatches branchName pis targetPs = do
      contextAlreadyExported <- findMostRecentMarkFromCtx pis
      case contextAlreadyExported of
        Right m -> return m
        Left (latestCtx, latestMark, remainingPIs) -> do
          let mbCtxPs = extractPatchesByPatchInfos remainingPIs targetPs
          case mbCtxPs of
            Nothing -> die "Couldn't commute out context patches for merge."
            Just (Sealed2 ctxPs) -> do
              let fooBranchPs = seal2 ctxPs
                  fooBranch = Branch latestMark branchName latestCtx
                    fooBranchPs fooBranchPs
              dumpBranch fooBranch []
              -- Return the last exported mark
              gets lastExportedMark

    -- extractPatchesByPatchInfos attempts to retrieve a list of patches
    -- described by their PatchInfos from a given FL of patches, returning
    -- Nothing if the patches cannot be extracted.
    extractPatchesByPatchInfos :: [PatchInfo] -> Sealed2(FL (PatchInfoAnd p))
      -> Maybe(Sealed2(FL (PatchInfoAnd p)))
    extractPatchesByPatchInfos [] _    = Just $ seal2 NilFL
    extractPatchesByPatchInfos _ (Sealed2 NilFL) = Nothing
    extractPatchesByPatchInfos pis (Sealed2 ps) =
      let finalInfo = last pis in
          -- Obtain the FL 'as far as' the last patch we require.
      tryTakeUntilFL ((== finalInfo) . info) ps
        >>= \(Sealed ps') -> extractPatchesByPatchInfos' NilRL pis ps'

    extractPatchesByPatchInfos' :: RL (PatchInfoAnd p) cX cY -> [PatchInfo]
      -> FL (PatchInfoAnd p) cY cZ -> Maybe(Sealed2(FL (PatchInfoAnd p)))
    extractPatchesByPatchInfos' ack [] _  = Just . seal2 . reverseRL $ ack
    extractPatchesByPatchInfos' _ _ NilFL = Nothing
    extractPatchesByPatchInfos' ack iss@(i:is) (p :>: ps) =
      if i == info p
        then extractPatchesByPatchInfos' (p :<: ack) is ps
        else case commuteFL (p :> ps) of
               Just (ps' :> _) -> extractPatchesByPatchInfos' ack iss ps'
               Nothing -> Nothing

    -- |findMostRecentMarkFromCtx returns the most recent mark from a list of
    -- PatchInfos. The PatchInfos are folded left-to-right into a context,
    -- which at each step is checked for being exported already. If a context
    -- has not been exported, we return the most recent mark, the current
    -- context and the remaining PatchInfos.
    findMostRecentMarkFromCtx = findMostRecentMarkFromCtx' ""
    findMostRecentMarkFromCtx' ctx [] = do
      ctxToMark <- asks ctx2mark
      (Right . fromJust) `fmap` ctxToMark ctx
    findMostRecentMarkFromCtx' ctx infos@(pi:pis) = do
      ctxToMark <- asks ctx2mark
      let newCtx = hashInfo ctx pi
      mbCtxMark <- ctxToMark newCtx
      case mbCtxMark of
        Just _ -> findMostRecentMarkFromCtx' newCtx pis
        Nothing -> do
          ctxMark <- fromJust `fmap` ctxToMark ctx
          return $ Left (ctx, ctxMark, infos)

-- |tryTakeUntilFL will attempt to take items from an FL, up-to and
-- including the item that the predicate matches. If no item is found,
-- Nothing is returned.
tryTakeUntilFL :: forall p cX cY. (forall x y. p x y -> Bool) -> FL p cX cY
  -> Maybe(Sealed(FL p cX))
tryTakeUntilFL = tryTakeUntilFL' NilRL
tryTakeUntilFL' :: forall p cX cY cZ. RL p cX cY -> (forall x y. p x y -> Bool)
  -> FL p cY cZ -> Maybe(Sealed(FL p cX))
tryTakeUntilFL' _ _ NilFL = Nothing
tryTakeUntilFL' acc f (x :>: _) | f x = Just . seal . reverseRL $ x :<: acc
tryTakeUntilFL' acc f (x :>: xs) = tryTakeUntilFL' (x :<: acc) f xs

dumpMergePatch :: (RepoPatch p) => String -> FL (PatchInfoAnd p) cX cY
  -> PatchInfoAnd p cA cB -> [Int] -> [FilePath] -> String -> ExportM Int
dumpMergePatch ctx resolutions mergeTag merges touchedFiles bName = do
  markToBName <- asks mark2bName
  let resolutionCount = lengthFL resolutions
      headMessage = case resolutions of
        NilFL -> ""
        (p :>: _) -> piName $ info p
  message <- if resolutionCount == 1 && "Merge" `isPrefixOf` headMessage
               then return $ BLC.pack headMessage
               else do
                 bNames <- (intercalate "," . map fromJust)
                   `fmap` mapM markToBName merges
                 let resMessages = unlines $
                       bNames : mapFL (piName . info) resolutions
                     msg = "Merge in branches: " ++ resMessages
                 return $ BLC.pack msg
  from <- gets lastExportedMark
  mark <- gets nextMark
  stashPristineAtMark mark
  modify (\s -> s {lastExportedMark = mark, nextMark = mark + 1})
  dumpBits [ BLC.pack ("progress " ++ show mark ++ ": ")
              `BLC.append` (head . BLC.lines $ message)
           , BLC.pack $ "commit refs/heads/" ++ bName ]
  asks marker >>= \m -> m mergeTag mark bName ctx
  let committer =
       "committer " ++ patchAuthor mergeTag ++ " " ++ patchDate mergeTag
  dumpBits
     [ BLU.fromString committer
     , BLU.fromString $ "data " ++ show (BL.length message + 1)
     , message ]
  dumpBits [ BLU.fromString $ "from :" ++ show from]
  forM_ merges $
    \m -> dumpBits [ BLU.fromString $ "merge :" ++ show m]
  dumpFiles $ map floatPath touchedFiles
  asks printer >>= \p -> p BLC.empty
  return mark

fl2Ctx :: String -> FL (PatchInfoAnd p) x y -> String
fl2Ctx = foldlFL hashPatch

stashPristineAtMark :: Int -> ExportM ()
stashPristineAtMark mark = lift $ stashPristine (Just mark)

go :: (RepoPatch p, ApplyState p ~ Tree) => [Branch p] -> ExportM ()
go [] = return ()
go branches = do
  stashPristineAtMark 0
  dumpBranch (head branches) (tail branches)


fastExport :: (BLU.ByteString -> ExportM ()) -> String -> [FilePath] -> Marks
  -> IO Marks
fastExport doPrint repodir bs marks = do
  branchPaths <- sort `fmap` mapM canonicalizePath bs
  withCurrentDirectory repodir $ withRepository [] $ RepoJob $ \repo ->
    fastExport' repo doPrint branchPaths marks

fastExport' :: forall p r u . (RepoPatch p, ApplyState p ~ Tree) => Repository p r u r
  -> (BLU.ByteString -> ExportM ()) -> [FilePath] -> Marks -> IO Marks
fastExport' repo doPrint bs marks = do
  patchset <- readRepo repo
  marksref <- newIORef marks
  let patches = newset2FL patchset
      emptyContext = ""
      doMark :: (forall q x y . (PatchInfoAnd q) x y -> Int -> String -> String
        -> ExportM ())
      doMark p n b c = do doPrint $ BLU.fromString $ "mark :" ++ show n
                          let bn = pb2bn . parseBranch . BC.pack $
                                     "refs/heads/" ++ b
                          liftIO $ modifyIORef marksref $
                            \m -> addMark m n (patchHash p, bn, BC.pack c)

      ctxTomark :: String -> ExportM (Maybe Int)
      ctxTomark ctx = do
        ms <- liftIO $ readIORef marksref
        return $ findMarkForCtx ctx ms

      markTobName :: Int -> ExportM (Maybe String)
      markTobName m = do
        ms <- liftIO $ readIORef marksref
        return $ (BC.unpack . (\(_,b,_) -> b)) `fmap` getMark ms m

      readBranchRepo :: FilePath -> IO (Branch p)
      readBranchRepo bPath = do
        bRepo <- identifyRepositoryFor repo bPath
        bPatches <- newset2FL `fmap` readRepo bRepo
        unless (equalHead patches bPatches) $
          die $ "ERROR: cannot export branch that has unequal initial patch: "
            ++ bPath
        let bName = fp2bn bPath
            sealedPs = seal2 bPatches
        return $ Branch 0 bName emptyContext sealedPs sealedPs

  them <- forM bs readBranchRepo
  let sealedPs = seal2 patches
      masterBranch = Branch 0 "master" emptyContext sealedPs sealedPs
      branches = masterBranch : them
      prisDir = "_darcs/pristine.hashed"
      initState = ExportState 0 (1 + lastMark marks)
      initReader =
        ExportReader ctxTomark markTobName doMark doPrint marks
      dumper = fst `fmap` evalRWST (go branches) initReader initState
  hashedTreeIO dumper emptyTree prisDir
  readIORef marksref
 `finally` do
  current <- readHashedPristineRoot repo
  cleanHashdir (extractCache repo) HashedPristineDir $ catMaybes [current]
