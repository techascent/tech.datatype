(ns tech.v2.datatype.byte-conversions-test
  (:require [clojure.test :refer [deftest is]]
            [tech.v2.datatype :as dtype])
  (:import [tech.v2.datatype ByteConversions]))



(defn short-test
  []
  (let [test-val Short/MAX_VALUE
        le-ary (byte-array 2)
        be-ary (byte-array 2)]
    (ByteConversions/shortToByteWriterLE test-val (dtype/->writer le-ary))
    le-ary))
