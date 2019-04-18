(ns tech.datatype.sparse.sparse-buffer-test
  (:require [tech.datatype.sparse.protocols :as sparse-proto]
            [tech.datatype :as dtype]
            [tech.datatype.sparse.sparse-buffer]
            [clojure.test :refer :all]))

(defn ->pairs
  [item-seq]
  (->> item-seq
       (mapv (fn [{:keys [data-index global-index]}]
               [data-index global-index]))))


(deftest base-sparse-sanity
  (let [test-sparse (dtype/make-container :sparse :float32 [1 0 1 0 1 0 1])
        next-sparse (dtype/make-container :sparse :float32 [0 2 0 4 0 6])]
    (is (= [[0 0] [1 2] [2 4] [3 6]] (->pairs (sparse-proto/index-seq test-sparse))))
    (is (= [[0 0] [1 2] [2 4]] (->pairs (-> (dtype/sub-buffer test-sparse 0 6)
                                            (sparse-proto/index-seq)))))
    (is (= [[0 1] [1 3] [2 5]] (->pairs (sparse-proto/index-seq next-sparse))))
    (is (= (mapv float [1 0 1 0 1 0 1]) (dtype/->vector test-sparse)))
    (is (= (mapv float [0 2 0 4 0 6]) (dtype/->vector next-sparse)))
    (let [test-sparse (dtype/copy! next-sparse test-sparse)]
      (is (= (mapv float [0 2 0 4 0 6 1])
             (dtype/->vector test-sparse))))
    (is (= (mapv float [4 0 6])

          (-> (dtype/sub-buffer test-sparse 3 3)
               (dtype/sub-buffer 0 3)
               dtype/->vector)))
    (is (= [[0 0] [1 2]]
           (-> (dtype/sub-buffer test-sparse 3 3)
               (dtype/sub-buffer 0 3)
               sparse-proto/index-seq
               ->pairs)))
    (let [new-buffer (dtype/copy! (float-array (range 5 8))
                                  (dtype/sub-buffer test-sparse 3 3))]
      (is (= (mapv float [5 6 7])
             (dtype/->vector new-buffer)))
      (is (= (mapv float [0 2 0 5 6 7 1])
             (dtype/->vector test-sparse)))))
  (let [test-sparse (dtype/make-container :sparse :float32 [1 0 2 0 3 0 4 0])
        new-buffer (dtype/sub-buffer test-sparse 1 3)]
    (is (= (mapv float [0 2 0])
           (dtype/->vector new-buffer))))
  (let [test-sparse (dtype/make-container :sparse :float32 [1 0 1 0 1 0 1 0])
        new-buffer (dtype/set-constant! (dtype/sub-buffer test-sparse 1 3) 0 0 3)]
    (is (= (mapv float [0 0 0])
           (dtype/->vector new-buffer)))
    (is (= (mapv float [1 0 0 0 1 0 1 0])
           (dtype/->vector test-sparse))))
  (let [test-sparse (dtype/make-container :sparse :float32 [1 0 1 0 1 0 1 0])
        new-buffer (dtype/copy! (float-array (range 5 8))
                                (dtype/sub-buffer test-sparse 1 3))]
    (is (= (mapv float [5 6 7])
           (dtype/->vector new-buffer)))
    (is (= (mapv float [1 5 6 7 1 0 1 0])
           (dtype/->vector test-sparse)))))



(deftest unsigned-data
  (let [test-data [255 254 0 1 2 3]
        src-buffer (short-array test-data)
        n-elems (dtype/ecount src-buffer)
        dst-buffer (dtype/make-container :sparse :uint8 n-elems)
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
    (let [sub-buf (dtype/sub-buffer dst-buffer 1 (- n-elems 1))
          test-ary (short-array (dtype/ecount sub-buf))]
      (dtype/copy! sub-buf test-ary)
      (is (= [254 0 1 2 3]
             (vec test-ary)))
      (is (= 254
             (dtype/get-value sub-buf 0)))
      (dtype/set-value! sub-buf 1 255)
      (dtype/copy! sub-buf test-ary)
      (is (= [254 255 1 2 3]
             (vec test-ary)))
      (is (= [254 255 1 2 3]
             (-> (dtype/clone sub-buf)
                 (dtype/->vector))))
      (is (= :uint32
             (-> (dtype/clone dst-buffer :datatype :uint32)
                 (dtype/get-datatype)))))))
