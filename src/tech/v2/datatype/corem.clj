(ns tech.v2.datatype.corem
  (:require [clojure.core.matrix.protocols :as mp]))


(defn corem-ecount
  ^long [item]
  (mp/element-count item))


(defn corem-shape
  [item]
  (mp/get-shape item))
