{-# LANGUAGE BangPatterns #-}

import Data.ByteString.Lazy.Char8 (ByteString)
import qualified Data.ByteString.Lazy.Char8 as BS
import Data.Ord
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Data.IntSet (IntSet)
import qualified Data.IntSet as Set
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Vector.Mutable (IOVector)
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Algorithms.Intro as VA
import System.Environment

main :: IO ()
main = do
  [f] <- getArgs
  (l1 : l2 : ls) <- BS.lines <$> BS.readFile f
  let [v , e , r , c , x] = map readInt (BS.split ' ' l1)
  let videoSizes = readIntLine l2
  (endpoints , ls') <- readEndpoints e ls
  requests <- readRequests v videoSizes e r ls'
  results <- calculateCaches c x videoSizes endpoints requests
  printResults results

readInt :: ByteString -> Int
readInt s = case BS.readInt s of
  Just (i , _) -> i
  Nothing      -> error "Unparsable Int"

readIntLine :: ByteString -> Vector Int
readIntLine = V.unfoldr step
  where step !s = case BS.readInt s of
          Just (!i , !t) -> Just (i , if BS.null t then BS.empty else BS.tail t)
          Nothing        -> Nothing

readEndpoints :: Int -> [ByteString] -> IO (Vector (Vector (Int , Int)) , [ByteString])
readEndpoints e ls = do
  v <- VM.new e
  ls' <- readEndpoint ls v 0
  v' <- V.unsafeFreeze v
  return (v' , ls')

  where

    readCaches :: [ByteString] -> IOVector (Int , Int) -> Int -> Int -> Int -> IO [ByteString]
    readCaches (l : ls) w latency numCaches i
      | i < numCaches = do
        let [cacheId , latency'] = map readInt (BS.split ' ' l)
        VM.unsafeWrite w i (cacheId , latency - latency')
        readCaches ls w latency numCaches (i + 1)

      | otherwise = return (l : ls)

    readCaches [] _ _ _ _ = error "Ran out of input!"

    readEndpoint :: [ByteString] -> IOVector (Vector (Int , Int)) -> Int -> IO [ByteString]
    readEndpoint (l : ls) v i
      | i < e = do
        let [latency , numCaches] = map readInt (BS.split ' ' l)
        w <- VM.new numCaches
        ls' <- readCaches ls w latency numCaches 0
        VA.sortBy (comparing (Down . snd)) w
        w' <- V.unsafeFreeze w
        VM.unsafeWrite v i w'
        readEndpoint ls' v (i + 1)

      | otherwise = return (l : ls)

    readEndpoint [] _ _ = error "Ran out of input!"

readRequests :: Int -> Vector Int -> Int -> Int -> [ByteString] -> IO (Vector (Int , Int , Int))
readRequests noVideos videoSizes e r ls = do
  v <- VM.replicate (noVideos * e)  (0 , 0 , 0)
  readRequest ls v 0
  v' <- V.unsafeFreeze v
  let v'' = V.filter (\ (_ , _ , c) -> c /= 0) v'
  v''' <- V.unsafeThaw v''
  V.forM_ (V.enumFromN 0 (noVideos * e)) $ \i ->
    VM.unsafeModify v''' (\ (a , b , c) -> (a , b , c * (videoSizes V.! a))) i
  VA.sortBy (comparing (Down . (\ (_ , _ , c) -> c))) v'''
  V.unsafeFreeze v'''

  where

    readRequest :: [ByteString] -> IOVector (Int , Int , Int) -> Int -> IO ()
    readRequest (l : ls) v i
      | i < r = do
        let [videoID , endpointID , requestSize] = map readInt (BS.split ' ' l)
        VM.unsafeModify v (\ (_ , _ , c) -> (videoID , endpointID , c + requestSize)) (videoID * e + endpointID)
        readRequest ls v (i + 1)

      | otherwise = return ()

    readRequest [] _ i
      | i >= r = return ()
      | otherwise = error "Ran out of input!"

calculateCaches :: Int -> Int -> Vector Int -> Vector (Vector (Int , Int)) -> Vector (Int , Int , Int) -> IO (Map Int IntSet)
calculateCaches c x videoSizes endpoints requests = do
  caches <- VM.replicate c x
  V.foldM (\ m (videoID , endpointID , _) -> do
            let cacheList = V.map fst $ endpoints V.! endpointID
            if inAnySets videoID cacheList m
              then return m
              else do
                cacheID <- findCache caches cacheList videoID
                case cacheID of
                  Just cacheID ->
                    case Map.lookup cacheID m of
                      Just resultSet ->
                        if Set.member videoID resultSet
                          then return m
                          else do
                            VM.unsafeModify caches (flip (-) (videoSizes V.! videoID)) cacheID
                            return (Map.insert cacheID (Set.insert videoID resultSet) m)
                      Nothing -> do
                        VM.unsafeModify caches (flip (-) (videoSizes V.! videoID)) cacheID
                        return (Map.insert cacheID (Set.singleton videoID) m)
                  Nothing -> return m)
          Map.empty
          requests

  where

    inAnySets :: (Eq k) => Int -> Vector k -> Map k IntSet -> Bool
    inAnySets a ks = not . Map.null . Map.filterWithKey (\ k v -> k `V.elem` ks && a `Set.member` v)

    findCache :: IOVector Int -> Vector Int -> Int -> IO (Maybe Int)
    findCache caches cacheList videoID = findCache' 0

      where

        findCache' :: Int -> IO (Maybe Int)
        findCache' i
          | i < V.length cacheList = do
              let cacheID = cacheList V.! i
              cacheSize <- VM.unsafeRead caches cacheID
              if cacheSize >= videoSizes V.! videoID
                then return (Just cacheID)
                else findCache' (i + 1)

          | otherwise = return Nothing

printResults :: Map Int IntSet -> IO ()
printResults m = do
  print $ Map.size m
  putStr $ unlines $ Map.foldrWithKey (\ k x acc -> unwords (show k : map show (Set.toList x)) : acc ) [] m
