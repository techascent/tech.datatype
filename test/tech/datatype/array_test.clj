(ns tech.datatype.array-test
  (:require [tech.datatype.array :as ary]
            [tech.datatype.nio-buffer :as nio-buf]
            [tech.datatype.typed-buffer :as typed-buf]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.protocols :as dtype-proto]
            [clojure.test :refer :all]))


(deftest array-basics
  (let [src-ary (short-array (range 10))
        byte-data (byte-array 10)
        float-data (float-array 10)]
    (dtype-base/copy! src-ary byte-data)
    (is (= (vec (range 10))
           (dtype-proto/->vector byte-data)))
    (is (= (mapv float (range 10))
           (-> (dtype-base/copy! src-ary float-data)
               (dtype-proto/->vector))))
    (let [sub-ary (dtype-proto/sub-buffer src-ary 3 3)]
      (dtype-base/set-value! sub-ary 1 20)
      (is (= [3 20 5] (dtype-proto/->vector sub-ary)))
      (is (= [0 1 2 3 20 5 6 7 8 9] (dtype-proto/->vector src-ary)))
      (dtype-base/set-constant! sub-ary 0 0 3)
      (is (= [0 0 0] (dtype-proto/->vector sub-ary)))
      (is (= [0 1 2 0 0 0 6 7 8 9] (dtype-proto/->vector src-ary))))
    (let [new-shorts (short-array (range 4))
          new-indexes (int-array [0 2 4 6])]
      (dtype-base/write-indexes! src-ary
                                 (dtype-proto/->buffer-backing-store new-indexes)
                                 (dtype-proto/->buffer-backing-store new-shorts))
      (is (= [0 1 1 0 2 0 3 7 8 9] (dtype-proto/->vector src-ary))))))
