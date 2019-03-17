(ns tech.datatype.readers.int
  (:require [tech.datatype.reader :as reader]))


(def int32-readers (reader/make-marshalling-reader-fn :int32))
