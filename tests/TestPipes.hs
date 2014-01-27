module Main where

import Network.Transport.Tests
import Network.Transport.Pipes
import Control.Applicative ((<$>))

main :: IO ()
main = testTransport $ either (Left . show) (Right) <$> createTransport
