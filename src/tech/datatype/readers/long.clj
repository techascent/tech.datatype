(ns tech.datatype.readers.long
  (:require [tech.datatype.reader :as reader]))


(def int64-readers (reader/make-marshalling-reader-fn :int64))
