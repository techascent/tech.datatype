(ns tech.datatype.readers.float
  (:require [tech.datatype.reader :as reader]))


(def float32-readers (reader/make-marshalling-reader-fn :float32))
