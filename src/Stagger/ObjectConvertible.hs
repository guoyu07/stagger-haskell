-- | ObjectConvertible is for transforming data to json-like objects and 
-- | reversely.
module Stagger.ObjectConvertible where

import Data.Attoparsec.ByteString (Parser)
import Data.MessagePack (Object(..), get, from)
import Data.Hashable (Hashable)
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import Data.Word
import qualified Data.ByteString as B
import qualified Data.Text as T
import qualified Data.HashSet as HS

import Control.Applicative

import Blaze.ByteString.Builder (Builder)

class ObjectConvertible x where
  fromObj :: Object -> Maybe x
  toObj :: x -> Object

instance ObjectConvertible Object where
  fromObj = Just

  toObj = id

instance ObjectConvertible T.Text where
  fromObj (ObjectRAW r) = Just (decodeUtf8 r)
  fromObj _ = Nothing

  toObj = ObjectRAW . encodeUtf8

instance ObjectConvertible B.ByteString where
  fromObj (ObjectRAW r) = Just r
  fromObj _ = Nothing

  toObj = ObjectRAW

instance ObjectConvertible Int where
  fromObj (ObjectInteger i) = Just i
  fromObj _ = Nothing

  toObj = ObjectInteger

-- FIXME: NOT SAFE
instance ObjectConvertible Integer where
  fromObj obj = fmap toInteger (fromObj obj :: Maybe Int)

  toObj = (toObj :: Int -> Object) . fromInteger

-- FIXME: NOT SAFE
instance ObjectConvertible Word64 where
  fromObj obj = fmap fromIntegral (fromObj obj :: Maybe Int)

  toObj = (toObj :: Int -> Object) . fromIntegral

-- FIXME: NOT SAFE
instance ObjectConvertible Word16 where
  fromObj obj = fmap fromIntegral (fromObj obj :: Maybe Int)

  toObj = (toObj :: Int -> Object) . fromIntegral

instance ObjectConvertible Double where
  fromObj (ObjectDouble d) = Just d
  fromObj _ = Nothing

  toObj = ObjectDouble

instance ObjectConvertible Bool where
  fromObj (ObjectBool b) = Just b
  fromObj _ = Nothing

  toObj = ObjectBool

instance ObjectConvertible a => ObjectConvertible [a] where
  toObj = ObjectArray . map toObj

  fromObj (ObjectArray l) = mapM fromObj l
  fromObj _ = Nothing

instance ObjectConvertible () where
  toObj () = ObjectNil

  fromObj ObjectNil = Just ()
  fromObj _ = Nothing

instance ObjectConvertible a => ObjectConvertible (Maybe a) where
  toObj Nothing = ObjectNil
  toObj (Just x) = toObj x

  fromObj ObjectNil = Just Nothing
  fromObj obj = Just <$> fromObj obj

instance (Hashable k, Eq k, ObjectConvertible k) => ObjectConvertible (HS.HashSet k) where
  toObj = toObj . HS.toList
  fromObj = fmap HS.fromList . fromObj

instance (ObjectConvertible a, ObjectConvertible b) => ObjectConvertible (Either a b) where
  toObj (Left x) =
    toObj x
  toObj (Right x) =
    toObj x

  fromObj x =
    (Left <$> fromObj x) <|>
    (Right <$> fromObj x)

getMap :: Object -> Maybe [(T.Text, Object)]
getMap (ObjectMap elems) =
  mapM (\(k, v) -> do
    k' <- fromObj k
    return (k', v)) elems
getMap _ = Nothing

-- used for string literals to disambiguate the type
objectText :: T.Text -> Object
objectText = toObj

key :: (ObjectConvertible a, Eq k) => k -> [(k, Object)] -> Maybe a
key k m = lookup k m >>= fromObj

objGet :: ObjectConvertible x => Parser x
objGet = get >>= (maybe (fail "failed to unpack") return . fromObj)

objFrom :: ObjectConvertible x => x -> Builder
objFrom = from . toObj
