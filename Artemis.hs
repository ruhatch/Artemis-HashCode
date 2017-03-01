{-# LANGUAGE BangPatterns #-}

import Control.Monad (forM_)
import Data.ByteString.Lazy.Char8 (ByteString)
import qualified Data.ByteString.Lazy.Char8 as BS
import Data.Ord
import Data.Map (Map)
import qualified Data.Map as Map
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
  let [v , e , _ , c , x] = map readInt (BS.split ' ' l1)
  let videoSizes = readIntLine l2
  (endpoints , ls') <- readEndpoints e ls
  requests <- readRequests v videoSizes e ls'
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

    readEndpoint :: [ByteString] -> IOVector (Vector (Int , Int)) -> Int -> IO [ByteString]
    readEndpoint (l : rest) v i
      | i < e = do
        let [latency , numCaches] = map readInt (BS.split ' ' l)
        let (cacheLines , rest') = splitAt numCaches rest
        w <- VM.new numCaches
        forM_ (zip [0..] cacheLines) (\ (j , l') -> do
          let [cacheId , latency'] = map readInt (BS.split ' ' l')
          VM.unsafeWrite w j (cacheId , latency - latency'))
        VA.sortBy (comparing (Down . snd)) w
        w' <- V.unsafeFreeze w
        VM.unsafeWrite v i w'
        readEndpoint rest' v (i + 1)

      | otherwise = return (l : rest)

    readEndpoint [] _ _ = error "Ran out of input!"

readRequests :: Int -> Vector Int -> Int -> [ByteString] -> IO (Vector (Int , Int , Int))
readRequests noVideos videoSizes e ls = do
  v <- VM.replicate (noVideos * e)  (0 , 0 , 0)
  forM_ ls (\ l -> do
    let [videoID , endpointID , requestSize] = map readInt (BS.split ' ' l)
    VM.unsafeModify v (\ (_ , _ , c) -> (videoID , endpointID , c + requestSize)) (videoID * e + endpointID))
  v' <- V.unsafeFreeze v
  let v'' = V.filter (\ (_ , _ , c) -> c /= 0) v'
  v''' <- V.unsafeThaw v''
  V.forM_ (V.enumFromN 0 (noVideos * e)) $ \i ->
    VM.unsafeModify v''' (\ (a , b , c) -> (a , b , c * (videoSizes V.! a))) i
  VA.sortBy (comparing (Down . (\ (_ , _ , c) -> c))) v'''
  V.unsafeFreeze v'''

calculateCaches :: Int -> Int -> Vector Int -> Vector (Vector (Int , Int)) -> Vector (Int , Int , Int) -> IO (Map Int IntSet)
calculateCaches c x videoSizes endpoints requests = do
  caches <- VM.replicate c x
  videos <- VM.replicate (V.length videoSizes) Set.empty
  V.foldM (\ m (videoID , endpointID , _) -> do
            let cacheList = V.map fst $ endpoints V.! endpointID
            videoCaches <- VM.unsafeRead videos videoID
            if any (`Set.member` videoCaches) cacheList
              then return m
              else do
                maybeCacheID <- findCache caches cacheList videoID
                case maybeCacheID of
                  Just cacheID ->
                    case Map.lookup cacheID m of
                      Just resultSet -> do
                        VM.unsafeModify caches (flip (-) (videoSizes V.! videoID)) cacheID
                        VM.unsafeModify videos (Set.insert cacheID) videoID
                        return (Map.insert cacheID (Set.insert videoID resultSet) m)
                      Nothing -> do
                        VM.unsafeModify caches (flip (-) (videoSizes V.! videoID)) cacheID
                        VM.unsafeModify videos (Set.insert cacheID) videoID
                        return (Map.insert cacheID (Set.singleton videoID) m)
                  Nothing -> return m)
          Map.empty
          requests

  where

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
