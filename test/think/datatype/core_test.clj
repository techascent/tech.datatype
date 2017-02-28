(ns think.datatype.core-test
  (:require [clojure.test :refer :all]
            [think.datatype.core :as dtype]
            [clojure.core.matrix :as m]
            [think.datatype.util :as util])
  (:import [think.datatype FloatArrayView]))


(deftest raw-copy-with-mutable-lazy-sequence
  ;;It is important that raw copy can work with a lazy sequence of double buffers where
  ;;the same buffer is being filled for ever member of the lazy sequence.  This is an easy
  ;;optimization to make that cuts down the memory usage when reading from datasets by
  ;;a fairly large amount.
  (let [input-seq (partition 10 (range 100))
        input-ary (double-array 10)
        ;;Note the input ary is being reused.  Now imagine the input ary is a large
        ;;image and we want a batch size of 50.  This is precisely the use case
        ;;where we don't want to allocate dynamically an entire batch worth of data
        ;;every time but copy one image at a time into the packed buffer for upload
        ;;to the gpu.
        double-array-seq (map (fn [data]
                                (dtype/copy-raw->item! data input-ary 0)
                                input-ary)
                              input-seq)
        output-doubles (double-array 100)]
    (dtype/copy-raw->item! double-array-seq output-doubles 0)
    (is (= (vec output-doubles) (mapv double (flatten input-seq))))))

(defn basic-copy
  [src-fn dest-fn]
  (let [ary (src-fn :float (range 10))
        buf (dest-fn :double 10)
        retval (dtype/make-array-of-type :double 10)]
    ;;copy starting at position 2 of ary into position 4 of buf 4 elements
    (dtype/copy! ary 2 buf 4 4)
    (dtype/copy! buf 0 retval 0 10)
    (is (m/equals [0 0 0 0 2 3 4 5 0 0]
                  (vec retval)))))

(defn make-offset-view
  [dtype elems-or-len]
  (let [temp-view (dtype/make-array-of-type dtype elems-or-len)
        retval (dtype/->view
                (dtype/make-array-of-type dtype (+ (dtype/ecount temp-view) 100))
                100
                (dtype/ecount temp-view))]
    (dtype/copy! temp-view 0 retval 0 (dtype/ecount temp-view))
    retval))


(def create-functions [dtype/make-array-of-type
                       dtype/make-buffer
                       dtype/make-view
                       make-offset-view])



(deftest generalized-copy-test
  (mapv (fn [[src-fn dest-fn]]
          (basic-copy src-fn dest-fn))
        (util/all-pairs create-functions)))


(defn indexed-copy-test
  [src-fn dest-fn]
  (let [n-elems 100
        src-data (src-fn :float (range n-elems))
        src-indexes (range n-elems)
        dest-indexes (reverse (range n-elems))
        dest-data (dest-fn :double n-elems)
        result (double-array n-elems)]
    (dtype/indexed-copy! src-data 0 src-indexes dest-data 0 dest-indexes)
    (dtype/copy! dest-data 0 result 0 n-elems)
    (is (m/equals (vec (reverse (range n-elems)))
                  (vec result)))))


(deftest generalized-indexed-copy-test
  (mapv (fn [[src-fn dest-fn]]
          (indexed-copy-test src-fn dest-fn))
        (util/all-pairs create-functions)))


(deftest array-view-offset->array-indexed-test
  (let [n-elems 100
        src-data (dtype/make-view :float (range (+ n-elems 100)))
        src-data (dtype/->view src-data 100 n-elems)
        src-indexes (range n-elems)
        dest-indexes (reverse (range n-elems))
        dest-data (dtype/make-array-of-type :float n-elems)
        result (double-array (reverse (drop 100 (range (+ n-elems 100)))))
        answer (double-array n-elems)]
    (dtype/indexed-copy! src-data 0 src-indexes dest-data 0 dest-indexes)
    (dtype/copy! dest-data 0 answer 0 n-elems)
    (is (m/equals result answer))))


(deftest vector-indexed-copy-test
  (let [n-elems 100
        vec-len 10
        n-vecs (/ n-elems vec-len)
        src-data (dtype/make-view :float (range n-elems))
        dst-data (dtype/make-view :double n-elems)
        src-indexes (range vec-len)
        dest-indexes (reverse src-indexes)
        result (double-array n-elems)
        answer (->> (range n-elems)
                    (partition vec-len)
                    reverse
                    (mapv vec))]
    (dtype/indexed-copy! src-data 0 src-indexes dst-data 0 dest-indexes vec-len)
    (dtype/copy! dst-data result)
    (is (m/equals (->> result
                       (partition vec-len)
                       (mapv vec)) answer))))


(deftest v-aget-rem-regression-test
  (let [n-elems 100
        view-len 10
        test-view (dtype/make-view :float (range n-elems))
        sub-views (map (fn [idx]
                         (dtype/->view test-view (* idx view-len) view-len))
                       (range (quot n-elems view-len)))
        result-seq (mapcat (fn [^FloatArrayView sub-view]
                             (map (fn [^long idx]
                                    (dtype/v-aget-rem sub-view idx))
                                  (range view-len)))
                           sub-views)]
    (is (m/equals (range n-elems)
                  result-seq))))
