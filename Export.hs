{-# LANGUAGE GADTs, OverloadedStrings, Rank2Types, ScopedTypeVariables #-}
module Export( fastExport ) where

import Marks
import Utils
import Stash

import Codec.Compression.GZip ( compress )
import Control.Applicative ( (<*>) )
import Control.Monad ( when, forM, forM_, unless )
import Control.Monad.Trans ( liftIO )
import Control.Monad.State.Strict( gets )
import Control.Exception( finally )
import Data.List ( sort, intercalate, isPrefixOf, sortBy, (\\), nub )
import Data.Maybe ( catMaybes, fromJust, isJust )
import Data.Ord ( comparing )
import Data.DateTime ( formatDateTime, fromClockTime )
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.ByteString.Lazy.UTF8 as BLU
import Data.ByteString.Base64 ( encode )
import Data.IORef ( newIORef, modifyIORef, readIORef )
import Numeric ( showHex )
import Prelude hiding ( readFile )
import Printer ( renderString )
import System.Directory ( canonicalizePath )
import System.Random ( randomRIO )
import System.Time ( toClockTime )

import Darcs.Repository ( Repository, RepoJob(..), readRepo, withRepository )
import Darcs.Repository.Cache ( HashedDir( HashedPristineDir ) )
import Darcs.Repository.HashedRepo ( readHashedPristineRoot )
import Darcs.Repository.HashedIO ( cleanHashdir )
import Darcs.Repository.Internal ( identifyRepositoryFor )
import Darcs.Repository.InternalTypes ( extractCache )
import Darcs.Patch ( effect, listTouchedFiles, apply, RepoPatch, showPatch
                   , getdeps )
import Darcs.Patch.Commute ( commuteFL )
import Darcs.Patch.Effect ( Effect )
import Darcs.Patch.Info ( isTag, PatchInfo, piAuthor, piName, piLog, piDate
                        , makePatchname )
import Darcs.Patch.PatchInfoAnd ( PatchInfoAnd, info, hopefully )
import Darcs.Patch.Prim.Class ( PrimOf, primIsTokReplace, PrimClassify(..)
                              , joinPatches )
import Darcs.Patch.Set ( PatchSet(..), Tagged(..), newset2FL )
import Darcs.Witnesses.Ordered ( FL(..), RL(..), nullFL, mapFL, spanFL
                               , (:>)(..), foldlFL, reverseRL )
import Darcs.Witnesses.Sealed ( seal2, Sealed2(..), seal , flipSeal
                              , Sealed(..), FlippedSeal(..) )
import Darcs.Utils ( withCurrentDirectory )

import Storage.Hashed.Darcs
import Storage.Hashed.Monad hiding ( createDirectory, exists )
import Storage.Hashed.Tree( findTree, emptyTree, listImmediate, findTree )
import Storage.Hashed.AnchoredPath( anchorPath, appendPath, floatPath
                                  , AnchoredPath(..) )

import SHA1 ( sha1PS )

import Debug.Trace

-- Name, 'from' mark current context, original patch FL, and the remaining
-- to-be-exported patches.
data Branch p = Branch Int String String (Sealed2 (FL (PatchInfoAnd p)))
  (Sealed2 (FL (PatchInfoAnd p)))

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
isMergeSourceTag id p = isMergeTag p $ "pre-source: " ++ id

isMergeEndTag :: String -> PatchInfoAnd p x y -> Bool
isMergeEndTag id p = isMergeTag p $ "post: " ++ id

isMergeTag :: PatchInfoAnd p x y -> String -> Bool
isMergeTag p _ | not $ isTag (info p) = False
isMergeTag p id = (mergeTagString ++ id) `isPrefixOf` patchName p

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

patchDate :: (PatchInfoAnd p) x y -> String
patchDate = formatDateTime "%s +0000" . fromClockTime . toClockTime .
  piDate . info

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

dumpBits :: (BLU.ByteString -> TreeIO ()) -> [BLU.ByteString] -> TreeIO ()
dumpBits printer = printer . BL.intercalate "\n"

dumpFiles :: (BLU.ByteString -> TreeIO ()) -> [AnchoredPath] -> TreeIO ()
dumpFiles printer files = forM_ files $ \file -> do
  isfile <- fileExists file
  isdir <- directoryExists file
  when isfile $ do
    bits <- readFile file
    dumpBits printer [ BLU.fromString $
                        "M 100644 inline " ++ anchorPath "" file
                     , BLU.fromString $ "data " ++ show (BL.length bits)
                     , bits ]
  when isdir $ do tt <- gets tree -- ick
                  let subs = [ file `appendPath` n | (n, _) <-
                                  listImmediate $ fromJust $ findTree tt file ]
                  dumpFiles printer subs
  when (not isfile && not isdir) $
    printer $ BLU.fromString $ "D " ++ anchorPath "" file

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

dumpPatch :: (RepoPatch p) => String -> (BLU.ByteString -> TreeIO ())
  -> ((PatchInfoAnd p) x y -> Int -> String -> String -> TreeIO ())
  -> (PatchInfoAnd p) x y -> Int -> Int -> String -> TreeIO ()
dumpPatch ctx printer doMark p from current bName =
  do dumpBits printer [ BLC.pack $ "progress " ++ show current ++ ": "
                          ++ patchName p
                      , BLC.pack $ "commit refs/heads/" ++ bName ]
     let message = patchMessage p `BL.append` generateInfoIgnores p
     doMark p current bName ctx
     stashPristine (Just current) Nothing
     dumpBits printer
        [ BLU.fromString $ "committer " ++ patchAuthor p ++ " " ++ patchDate p
        , BLU.fromString $ "data " ++ show (BL.length message + 1)
        , message ]
     when (current > 1) $
       dumpBits printer [ BLU.fromString $ "from :" ++ show from]
     dumpFiles printer $ map floatPath $ listTouchedFiles p

dumpTag :: (BLU.ByteString -> TreeIO ()) -> (PatchInfoAnd p) x y -> Int
  -> TreeIO ()
dumpTag _ p _ | isAnyMergeTag p = return ()
dumpTag printer p from = dumpBits printer
    [ BLU.fromString $ "progress TAG " ++ tagName p
    , BLU.fromString $ "tag refs/tags/" ++ tagName p -- FIXME is this valid?
    , BLU.fromString $ "from :" ++ show from
    , BLU.fromString $ "tagger " ++ patchAuthor p ++ " " ++ patchDate p
    , BLU.fromString $ "data " ++ show (BL.length (patchMessage p) - 4 + 1)
    , BL.drop 4 $ patchMessage p ]

reset :: (BLU.ByteString -> TreeIO ()) -> Maybe Int -> String -> TreeIO ()
reset printer mbMark branch = do
    printer . BLC.pack $ "reset refs/heads/" ++ branch
    maybe (return ()) (\m -> printer . BLC.pack $ "from :" ++ show m) mbMark

hashPatch :: String -> PatchInfoAnd p cX cY -> String
hashPatch ctx = hashInfo ctx . info

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


dumpBranch :: forall p . (RepoPatch p) => (BLU.ByteString -> TreeIO ())
  -> Marks
  -> (String -> IO (Maybe Int))
  -> (Int -> IO (Maybe String))
  -> (Int -> IO (Maybe String))
  -> (forall cX cY . PatchInfoAnd p cX cY -> Int -> String -> String -> TreeIO ())
  -> Int -> Int -> Branch p -> [Branch p] -> TreeIO (Int, Int)
dumpBranch printer existingMarks ctx2mark mark2bName mark2ctx doMark from
  current (Branch _ _ _ _ (Sealed2 NilFL)) bs = if null bs
    then return (from, current)
    else dumpBranch printer existingMarks ctx2mark mark2bName mark2ctx doMark
           from current (head bs) (tail bs)
dumpBranch printer existingMarks ctx2mark mark2bName mark2ctx doMark from
  current (Branch bFrom bName bCtx bOrigPs (Sealed2(p:>:ps))) bs = do
    let nextCtx = hashPatch bCtx p
    cMark <- liftIO $ ctx2mark nextCtx
    case cMark of
      -- If we've seen the next context before, we want to base our patches
      -- on that context.
      Just mark ->
        let incBranch = Branch mark bName nextCtx bOrigPs (seal2 ps) in
        dumpBranches printer existingMarks ctx2mark mark2bName mark2ctx doMark
          from current (incBranch : bs)
      Nothing -> do
        when (from /= bFrom) $ restorePristineFromMark "exportTemp" bFrom
        apply p
        case isMergeBeginTag p of
          Just mergeID -> do
            let mbMerged = findMergeEndTag mergeID ps
            case mbMerged of
              -- If we can't find the matching merge tag, warn, and
              -- export the patches as if there was no merge.
              Nothing -> do
                prog [ "WARNING: could not find merge end tag with mergeID:"
                     , mergeID, "merge will be omitted." ]
                dumpBranches printer existingMarks ctx2mark mark2bName mark2ctx
                  doMark bFrom current
                  (Branch bFrom bName nextCtx bOrigPs (seal2 ps) : bs)
              Just (Sealed mPs, Sealed2 endTag, FlippedSeal ps') -> do
                let mbMergesResolutions =
                      partitionMergesAndResolutions mergeID mPs
                case mbMergesResolutions of
                  -- If we can't find any source tags, we can't recreate the
                  -- pre-merge context, so warn, and export the remaining
                  -- patches without the merge.
                  Nothing -> do
                    prog [ "WARNING: could not find merge source tag(s) with"
                         , "mergeID:", mergeID, "merge will be omitted." ]
                    dumpBranches printer existingMarks ctx2mark mark2bName
                      mark2ctx doMark bFrom current
                      (Branch bFrom bName nextCtx bOrigPs (seal2 ps) : bs)
                  Just (merges, Sealed2 resolutions) -> do
                    (current', mergeMarks) <-
                      dumpMergeSources bOrigPs merges current from
                    -- Reset our current state since it could have been changed
                    -- if some merge sources hadn't been exported.
                    restorePristineFromMark "exportTemp" bFrom
                    -- mPs includes all merged patches, and any resolutions
                    apply mPs
                    let nextMark = current' + 1
                        nextCtx = hashPatch (fl2Ctx nextCtx mPs) endTag
                    dumpMergePatch nextCtx printer doMark mark2bName
                      resolutions endTag bFrom current' mergeMarks bName
                    dumpBranches printer existingMarks ctx2mark mark2bName
                      mark2ctx doMark current' nextMark
                      (Branch current bName nextCtx bOrigPs (seal2 ps') : bs)
          Nothing -> do
            if isProperTag p
              then dumpTag printer p bFrom
              else dumpPatch nextCtx printer doMark p bFrom current bName
            let nextMark = next current p
                fromMark = if nextMark == current then bFrom else current
            dumpBranches printer existingMarks ctx2mark mark2bName mark2ctx
              doMark fromMark nextMark
              (Branch fromMark bName nextCtx bOrigPs (seal2 ps) : bs)
  where
    prog = printer . BLC.pack . unwords . ("progress" :)
    -- TODO: this sucks, we know that the resulting Just tuple is of this form:
    -- (FL (PIAP p) a b, PIAP p b c, FL (PIAP p) c d) but I don't think I can
    -- prove it...
    findMergeEndTag :: String -> FL (PatchInfoAnd p) x z
      -> Maybe (Sealed(FL (PatchInfoAnd p) x),
                Sealed2(PatchInfoAnd p),
                FlippedSeal(FL (PatchInfoAnd p)) z)
    findMergeEndTag _ NilFL = Nothing
    findMergeEndTag mergeID ps = case spanFL (not . isMergeEndTag mergeID) ps of
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

    dumpMergeSources :: (RepoPatch p) => Sealed2(FL (PatchInfoAnd p))
      -> [(Sealed2(FL (PatchInfoAnd p)), Sealed2(PatchInfoAnd p))] -> Int
      -> Int -> TreeIO (Int, [Int])
    dumpMergeSources _ [] current from = return (current, [])
    dumpMergeSources targetPs ((sealedPs@(Sealed2 ps), Sealed2 tag) : bs)
      current from = do
      let dependencyPIs = reverse . getdeps . hopefully $ tag
          newPIs = mapFL info ps
          ctxPIs = dependencyPIs \\ newPIs
      tempBranchName <- liftIO $
        flip showHex "" `fmap` randomRIO (0,2^(128 ::Integer) :: Integer)
      (contextMark, current') <-
        checkOrDumpPatches tempBranchName ctxPIs targetPs current from
      -- TODO: here, do we need to commute other patches from the contexts out,
      -- so we don't output conflicting changes that have no effect?
      (sourceMark, current'') <-
        checkOrDumpPatches tempBranchName dependencyPIs sealedPs current'
          contextMark
      (current''', otherMarks) <-
        dumpMergeSources targetPs bs current'' sourceMark
      return (current''', sourceMark : otherMarks)

    checkOrDumpPatches branchName pis targetPs current from = do
      contextAlreadyExported <- findMostRecentStateFromCtx pis
      case contextAlreadyExported of
        Right exportMark -> return (exportMark, current)
        -- The entire merged-branch hasn't already been exported, we export the
        -- patches on a randomly-named branch. Had the branch been available
        -- for export, it would have been exported due to our sorting in
        -- dumpBranches.
        Left (latestCtx, latestMark, remainingPIs) -> do
          let mbCtxPs = getPatchesFromPatchInfos remainingPIs targetPs
          case mbCtxPs of
            Nothing -> die "Couldn't commute out context patches for merge."
            Just (Sealed2 ctxPs) -> do
              let fooBranchPs = seal2 ctxPs
                  fooBranch = Branch latestMark latestCtx branchName
                    fooBranchPs fooBranchPs
              dumpBranch printer existingMarks ctx2mark mark2bName mark2ctx
                doMark from current fooBranch []

    getPatchesFromPatchInfos :: [PatchInfo] -> Sealed2(FL (PatchInfoAnd p))
      -> Maybe(Sealed2(FL (PatchInfoAnd p)))
    getPatchesFromPatchInfos [] _    = Just $ seal2 NilFL
    getPatchesFromPatchInfos _ (Sealed2 NilFL) = Nothing
    getPatchesFromPatchInfos pis (Sealed2 ps) =
      let finalInfo = last pis in
          -- Obtain the FL 'as far as' the last patch we require.
      tryTakeUntilFL ((== finalInfo) . info) ps
        >>= \(Sealed ps') -> getPatchesFromPatchInfos' NilRL pis ps'

    getPatchesFromPatchInfos' :: RL (PatchInfoAnd p) cX cY -> [PatchInfo]
      -> FL (PatchInfoAnd p) cY cZ -> Maybe(Sealed2(FL (PatchInfoAnd p)))
    getPatchesFromPatchInfos' ack [] _  = Just . seal2 . reverseRL $ ack
    getPatchesFromPatchInfos' _ _ NilFL = Nothing
    getPatchesFromPatchInfos' ack iss@(i:is) (p :>: ps) =
      if i == info p
        then getPatchesFromPatchInfos' (p :<: ack) is ps
        else case commuteFL (p :> ps) of
               Just (ps' :> _) -> getPatchesFromPatchInfos' ack iss ps'
               Nothing -> Nothing

    findMostRecentStateFromCtx = findMostRecentStateFromCtx' ""
    findMostRecentStateFromCtx' ctx [] =
      liftIO $ (Right . fromJust) `fmap` ctx2mark ctx
    findMostRecentStateFromCtx' ctx infos@(pi:pis) = do
      let newCtx = hashInfo ctx pi
      mbCtxMark <- liftIO $ ctx2mark newCtx
      case mbCtxMark of
        Just m -> findMostRecentStateFromCtx' newCtx pis
        Nothing -> do
          ctxMark <- liftIO $
            fromJust `fmap` ctx2mark ctx
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

dumpMergePatch :: (RepoPatch p) => String -> (BLU.ByteString -> TreeIO ())
  -> (forall cX cY . PatchInfoAnd p cX cY -> Int -> String -> String -> TreeIO ())
  -> (Int -> IO (Maybe String)) -> FL (PatchInfoAnd p) cA cB
  -> PatchInfoAnd p cC cD -> Int -> Int -> [Int] -> String -> TreeIO ()
dumpMergePatch ctx printer doMark mark2bName resolutions mergeTag from current
  merges bName = do
     bNames <- liftIO $
       (intercalate "," . map fromJust) `fmap` mapM mark2bName merges
     let message = BLC.pack $ "Merge in branches: " ++ bNames
     dumpBits printer [ BLC.pack ("progress " ++ show current ++ ": ")
                         `BLC.append` message
                      , BLC.pack $ "commit refs/heads/" ++ bName ]
     doMark mergeTag current bName ctx
     stashPristine (Just current) Nothing
     let committer =
          "committer " ++ patchAuthor mergeTag ++ " " ++ patchDate mergeTag
     dumpBits printer
        [ BLU.fromString committer
        , BLU.fromString $ "data " ++ show (BL.length message + 1)
        , message ]
     dumpBits printer [ BLU.fromString $ "from :" ++ show from]
     forM_ merges $
       \m -> dumpBits printer [ BLU.fromString $ "merge :" ++ show m]
     let touchedFiles = nub . concat $ mapFL listTouchedFiles resolutions
     dumpFiles printer $ map floatPath touchedFiles

fl2Ctx :: String -> FL (PatchInfoAnd p) x y -> String
fl2Ctx = foldlFL hashPatch

dumpBranches :: forall p . (RepoPatch p) => (BLU.ByteString -> TreeIO ())
  -> Marks -> (String -> IO (Maybe Int))
  -> (Int -> IO (Maybe String))
  -> (Int -> IO (Maybe String))
  -> (forall cX cY . PatchInfoAnd p cX cY -> Int -> String -> String -> TreeIO ())
  -> Int -> Int -> [Branch p] -> TreeIO (Int, Int)
dumpBranches _ _ _ _ _ _ from current []
  = return (from, current)
dumpBranches printer existingMarks ctx2mark mark2bName mark2ctx doMark from
  current bs = dumpBranch printer existingMarks ctx2mark mark2bName mark2ctx
    doMark from current (head sortedBranches) (tail sortedBranches)
  where
    sortedBranches = sortBy branchOrderer bs
    -- We want to export non-merges first, since the merges could be merge the
    -- patches from the non-merges.
    branchOrderer :: Branch p -> Branch p -> Ordering
    branchOrderer = comparing mergeHeadDepth
    mergeHeadDepth (Branch _ _ _ _ ps) = mergeHeadDepth' 0 ps
    mergeHeadDepth' :: Int -> Sealed2(FL(PatchInfoAnd p)) -> Int
    mergeHeadDepth' acc (Sealed2 NilFL) = acc
    mergeHeadDepth' acc (Sealed2(p :>: ps)) =
      if isJust $ isMergeBeginTag p
        then mergeHeadDepth' (acc + 1) (seal2 ps)
        else acc

fastExport :: (BLU.ByteString -> TreeIO ()) -> String -> [FilePath] -> Marks
  -> IO Marks
fastExport printer repodir bs marks = do
  branchPaths <- sort `fmap` mapM canonicalizePath bs
  withCurrentDirectory repodir $ withRepository [] $ RepoJob $ \repo ->
    fastExport' repo printer branchPaths marks

fastExport' :: forall p r u . (RepoPatch p) => Repository p r u r
  -> (BLU.ByteString -> TreeIO ()) -> [FilePath] -> Marks -> IO Marks
fastExport' repo printer bs marks = do
  patchset <- readRepo repo
  marksref <- newIORef marks
  let patches = newset2FL patchset
      emptyContext = ""
      doMark :: (PatchInfoAnd p) x y -> Int -> String -> String -> TreeIO ()
      doMark p n b c = do printer $ BLU.fromString $ "mark :" ++ show n
                          let bn = pb2bn . parseBranch . BC.pack $
                                     "refs/heads/" ++ b
                          liftIO $ modifyIORef marksref $
                            \m -> addMark m n (patchHash p, bn, BC.pack c)

      ctx2mark :: String -> IO (Maybe Int)
      ctx2mark ctx = do
        ms <- readIORef marksref
        return $ findMarkForCtx ctx ms

      mark2bName :: Int -> IO (Maybe String)
      mark2bName = markExtract (BC.unpack . (\(_,b,_) -> b))

      mark2ctx :: Int -> IO (Maybe String)
      mark2ctx = markExtract (BC.unpack . (\(_,_,c) -> c))

      markExtract :: ((BC.ByteString, BC.ByteString, BC.ByteString) -> a)
        -> Int -> IO (Maybe a)
      markExtract f m  = do
        ms <- readIORef marksref
        return $ f `fmap` getMark ms m

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
      bs = masterBranch : them
      dumper =
        dumpBranches printer marks ctx2mark mark2bName mark2ctx doMark 0 1 bs
  hashedTreeIO dumper emptyTree "_darcs/pristine.hashed"
  readIORef marksref
 `finally` do
  current <- readHashedPristineRoot repo
  cleanHashdir (extractCache repo) HashedPristineDir $ catMaybes [current]
