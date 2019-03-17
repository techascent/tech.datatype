(ns tech.datatype.readers.short
  (:require [tech.datatype.reader :as reader]))


(def int16-readers (reader/make-marshalling-reader-fn :int16))
