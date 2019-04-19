(ns tech.v2.datatype.array-test
  (:require [tech.v2.datatype.array :as ary]
            [tech.v2.datatype.nio-buffer :as nio-buf]
            [tech.v2.datatype.typed-buffer :as typed-buf]
            [tech.v2.datatype :as dtype]
            [clojure.test :refer :all]))


(deftest array-basics
  (let [src-ary (short-array (range 10))
        byte-data (byte-array 10)
        float-data (float-array 10)]
    (dtype/copy! src-ary byte-data)
    (is (= (vec (range 10))
           (dtype/->vector byte-data)))
    (is (= (mapv float (range 10))
           (-> (dtype/copy! src-ary float-data)
               (dtype/->vector))))
    (let [sub-ary (dtype/sub-buffer src-ary 3 3)]
      (dtype/set-value! sub-ary 1 20)
      (is (= [3 20 5] (dtype/->vector sub-ary)))
      (is (= [0 1 2 3 20 5 6 7 8 9] (dtype/->vector src-ary)))
      (dtype/set-constant! sub-ary 0 0 3)
      (is (= [0 0 0] (dtype/->vector sub-ary)))
      (is (= [0 1 2 0 0 0 6 7 8 9] (dtype/->vector src-ary))))
    (let [new-shorts (short-array (range 4))
          new-indexes (int-array [0 2 4 6])]
      (dtype/write-indexes! src-ary
                                 (dtype/->buffer-backing-store new-indexes)
                                 (dtype/->buffer-backing-store new-shorts))
      (is (= [0 1 1 0 2 0 3 7 8 9] (dtype/->vector src-ary))))))
