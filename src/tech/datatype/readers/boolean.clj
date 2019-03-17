(ns tech.datatype.readers.boolean
  (:require [tech.datatype.nio-buffer :as nio-buffer]))


(def boolean-readers (nio-buffer/make-marshalling-reader-fn :boolean))
