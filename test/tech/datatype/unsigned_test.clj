(ns tech.datatype.unsigned-test
  (:require [tech.datatype.java-unsigned :as unsigned]
            [tech.datatype.core :as dtype]
            [clojure.test :refer :all])
  (:import [java.nio ByteBuffer]))



(deftest unsigned-basics
  (let [test-data [255 254 0 1 2 3]
        src-buffer (short-array test-data)
        n-elems (dtype/ecount src-buffer)
        dst-buffer (unsigned/make-typed-buffer :uint8 n-elems)
        byte-buffer (byte-array n-elems)]
    (dtype/copy! src-buffer dst-buffer)
    (is (= test-data
           (dtype/->vector dst-buffer)))
    (is (thrown? Throwable (dtype/copy! dst-buffer byte-buffer)))
    (let [casted-bytes (dtype/copy! dst-buffer 0 byte-buffer 0 n-elems
                                    {:unchecked? true})]
      (is (= [-1 -2 0 1 2 3] (vec casted-bytes))))
    ;;The item-by-item interfaces always check
    (is (thrown? Throwable (dtype/set-value! dst-buffer 2 -1)))
    (is (thrown? Throwable (dtype/set-value! dst-buffer 2 256)))
    (let [new-buf (float-array n-elems)]
      (dtype/copy! dst-buffer new-buf)
      (is (= test-data (mapv long new-buf))))
    (let [^ByteBuffer storage-buf (:buffer dst-buffer)
          _ (.position storage-buf 1)
          test-ary (short-array (dtype/ecount dst-buffer))]
      (dtype/copy! dst-buffer test-ary)

      (is (= [254 0 1 2 3]
             (vec test-ary)))
      (is (= 254
             (dtype/get-value dst-buffer 0)))
      (dtype/set-value! dst-buffer 0 255)
      (dtype/copy! dst-buffer test-ary)
      (is (= [255 0 1 2 3]
             (vec test-ary))))))
