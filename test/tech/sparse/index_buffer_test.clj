(ns tech.sparse.index-buffer-test
  (:require [tech.sparse.index-buffer :as index-buffer]
            [tech.sparse.protocols :as sparse-proto]
            [tech.datatype :as dtype]
            [tech.datatype.functional :as functional]
            [clojure.test :refer :all]))

(defn index-seq->tuple-pairs
  [idx-seq]
  (->> idx-seq
       (map (fn [{:keys [data-index global-index]}]
              [data-index global-index]))))


(deftest index-buffer-offset-stride-test
  (let [new-buf (index-buffer/make-index-buffer 12 (range 12))]
    (is (= [[0 0] [1 1] [2 2] [3 3] [4 4] [5 5] [6 6] [7 7] [8 8] [9 9] [10 10] [11 11]]
           (->> (sparse-proto/index-seq new-buf)
                index-seq->tuple-pairs)))

    (is (= [[0 0] [2 1] [4 2] [6 3] [8 4] [10 5]]
           (->> (sparse-proto/index-seq (sparse-proto/set-stride new-buf 2))
                index-seq->tuple-pairs)))

    (is (= 4 (dtype/ecount (sparse-proto/set-stride new-buf 3))))

    (is (= [[0 0] [3 1] [6 2] [9 3]]
           (->> (sparse-proto/index-seq (sparse-proto/set-stride new-buf 3))
                index-seq->tuple-pairs)))

    (let [sub-buf (dtype/sub-buffer new-buf 2 6)]
      (is (= 6 (dtype/ecount sub-buf)))
      (is (= [[0 0] [1 1] [2 2] [3 3] [4 4] [5 5]]
             (->> (sparse-proto/index-seq sub-buf)
                  index-seq->tuple-pairs)))
      (is (= [[0 0] [2 1] [4 2]]
             (->> (sparse-proto/index-seq (sparse-proto/set-stride sub-buf 2))
                  index-seq->tuple-pairs)))
      (is (= [[0 0] [3 1]]
             (->> (sparse-proto/index-seq (sparse-proto/set-stride sub-buf 3))
                  index-seq->tuple-pairs))))

    (let [sub-buf (sparse-proto/set-stride new-buf 3)]
      (is (= [[0 0] [3 1] [6 2] [9 3]]
             (->> (sparse-proto/index-seq sub-buf)
                  index-seq->tuple-pairs)))
      (is (= [[0 0] [6 1]]
             (->> (sparse-proto/index-seq (sparse-proto/set-stride sub-buf 2))
                  index-seq->tuple-pairs)))
      (is (= [[0 0]]
             (->> (sparse-proto/index-seq (sparse-proto/set-stride
                                           (dtype/sub-buffer sub-buf 1 3)
                                           3))
                  index-seq->tuple-pairs))))

    (let [data-buf (dtype/make-container :list :int32 (range 10))
          sub-buf (-> (dtype/sub-buffer new-buf 2 5)
                      (sparse-proto/remove-sequential-indexes! data-buf))]
      (is (= [] (->> (sparse-proto/index-seq sub-buf)
                     index-seq->tuple-pairs)))
      ;;2-6 are removed.
      (is (= [0 1 7 8 9] (dtype/->vector data-buf)))
      (is (= [[0 0] [1 1] [2 7] [3 8] [4 9] [5 10] [6 11]]
             (->> (sparse-proto/index-seq new-buf)
                  index-seq->tuple-pairs))))
    (let [new-buf (index-buffer/make-index-buffer 10 (range 10))
          data-buf (dtype/make-container :list :int32 (range 10))
          sub-buf (-> (sparse-proto/set-stride new-buf 2)
                      (sparse-proto/remove-sequential-indexes! data-buf))]
      (is (= [] (sparse-proto/index-seq sub-buf)))
      (is (= [1 3 5 7 9] (dtype/->vector data-buf)))
      (is (= [[0 1] [1 3] [2 5] [3 7] [4 9]]
             (->> (sparse-proto/index-seq new-buf)
                  index-seq->tuple-pairs)))
      (let [data-idx (second (sparse-proto/find-index sub-buf 3))
            _ (dtype/insert! data-buf data-idx 15)
            _ (sparse-proto/insert-index! sub-buf data-idx 3)]
        (is (= [[3 3]] (->> (sparse-proto/index-seq sub-buf)
                            index-seq->tuple-pairs)))
        (is (= [1 3 5 15 7 9] (dtype/->vector data-buf)))
        (is (= [[0 1] [1 3] [2 5] [3 6] [4 7] [5 9]]
               (->> (sparse-proto/index-seq new-buf)
                    index-seq->tuple-pairs)))
        (let [remove-idx (sparse-proto/remove-index! sub-buf 3)
              _ (dtype/remove-range! data-buf remove-idx 1)]
          (is (= [] (sparse-proto/index-seq sub-buf)))
          (is (= [1 3 5 7 9] (dtype/->vector data-buf)))
          (is (= [[0 1] [1 3] [2 5] [3 7] [4 9]]
                 (->> (sparse-proto/index-seq new-buf)
                      index-seq->tuple-pairs)))
          (is (nil? (sparse-proto/remove-index! sub-buf 3))))))))


(deftest sub-buffer
  (let [test-sparse (index-buffer/make-index-buffer 7 [0 2 4 6])]
    (is (= [[0 0] [1 2] [2 4] [3 6]] (vec
                                      (->> (sparse-proto/index-seq test-sparse)
                                           index-seq->tuple-pairs))))
    (is (= [[0 0] [1 2] [2 4]] (vec (-> (dtype/sub-buffer test-sparse 0 6)
                                        (sparse-proto/index-seq)
                                        index-seq->tuple-pairs))))))
