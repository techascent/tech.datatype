(ns tech.libs.buffered-image-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.tensor :as dtt]
            [tech.v2.tensor.typecast :as tens-typecast]
            [tech.libs.buffered-image :as bufimg]
            [clojure.test :refer :all]))


(deftest base-img-tensor-test
  (let [test-img (bufimg/new-image 4 4 :int-rgb)
        test-writer (tens-typecast/datatype->tensor-writer
                     :uint8
                     (bufimg/as-ubyte-tensor test-img))
        _ (.write3d test-writer 2 2 2 255)]
    (is (= [0 0 0 0 0 0 0 0 0 0 16711680 0 0 0 0 0]
           (dtype/->vector test-img)))))
