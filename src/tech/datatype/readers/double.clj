(ns tech.datatype.readers.double
  (:require [tech.datatype.reader :as reader]))


(def float64-readers (reader/make-marshalling-reader-fn :float64))
