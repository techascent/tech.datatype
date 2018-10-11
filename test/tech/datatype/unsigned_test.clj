(ns tech.datatype.unsigned-test
  (:require [tech.datatype.java-unsigned :as unsigned]
            [tech.datatype.core :as dtype]
            [clojure.test :refer :all]))



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
    (is (thrown? Throwable (dtype/set-value! dst-buffer 2 256)))))
