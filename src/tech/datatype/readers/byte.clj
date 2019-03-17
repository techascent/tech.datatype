(ns tech.datatype.readers.byte
  (:require [tech.datatype.reader :as reader]))


(def int8-readers (reader/make-marshalling-reader-fn :int8))
