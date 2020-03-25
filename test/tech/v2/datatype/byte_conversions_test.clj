(ns tech.v2.datatype.byte-conversions-test
  (:require [clojure.test :refer [deftest is]]
            [tech.v2.datatype :as dtype])
  (:import [tech.v2.datatype ByteConversions]))



(deftest short-test
  (let [test-val Short/MAX_VALUE
        le-ary (byte-array 2)
        be-ary (byte-array 2)]
    (ByteConversions/shortToWriterLE test-val (dtype/->writer le-ary))
    (ByteConversions/shortToWriterBE test-val (dtype/->writer be-ary))
    (is (== test-val (ByteConversions/shortFromReaderLE (dtype/->reader le-ary))))
    (is (== test-val (ByteConversions/shortFromReaderBE (dtype/->reader be-ary))))
    (is (not= (vec le-ary) (vec be-ary)))))


(deftest int-test
  (let [test-val Integer/MAX_VALUE
        le-ary (byte-array 4)
        be-ary (byte-array 4)]
    (ByteConversions/intToWriterLE test-val (dtype/->writer le-ary))
    (ByteConversions/intToWriterBE test-val (dtype/->writer be-ary))
    (is (== test-val (ByteConversions/intFromReaderLE (dtype/->reader le-ary))))
    (is (== test-val (ByteConversions/intFromReaderBE (dtype/->reader be-ary))))
    (is (not= (vec le-ary) (vec be-ary)))))


(deftest float-test
  (let [test-val (float Integer/MAX_VALUE)
        le-ary (byte-array 4)
        be-ary (byte-array 4)]
    (ByteConversions/floatToWriterLE test-val (dtype/->writer le-ary))
    (ByteConversions/floatToWriterBE test-val (dtype/->writer be-ary))
    (is (== test-val (ByteConversions/floatFromReaderLE (dtype/->reader le-ary))))
    (is (== test-val (ByteConversions/floatFromReaderBE (dtype/->reader be-ary))))
    (is (not= (vec le-ary) (vec be-ary)))))


(deftest long-test
  (let [test-val Long/MAX_VALUE
        le-ary (byte-array 8)
        be-ary (byte-array 8)]
    (ByteConversions/longToWriterLE test-val (dtype/->writer le-ary))
    (ByteConversions/longToWriterBE test-val (dtype/->writer be-ary))
    (is (== test-val (ByteConversions/longFromReaderLE (dtype/->reader le-ary))))
    (is (== test-val (ByteConversions/longFromReaderBE (dtype/->reader be-ary))))
    (is (not= (vec le-ary) (vec be-ary)))))


(deftest double-test
  (let [test-val (double Long/MAX_VALUE)
        le-ary (byte-array 8)
        be-ary (byte-array 8)]
    (ByteConversions/doubleToWriterLE test-val (dtype/->writer le-ary))
    (ByteConversions/doubleToWriterBE test-val (dtype/->writer be-ary))
    (is (== test-val (ByteConversions/doubleFromReaderLE (dtype/->reader le-ary))))
    (is (== test-val (ByteConversions/doubleFromReaderBE (dtype/->reader be-ary))))
    (is (not= (vec le-ary) (vec be-ary)))))
