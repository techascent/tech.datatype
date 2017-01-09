(ns think.datatype.core-test
  (:require [clojure.test :refer :all]
            [think.datatype.core :as dtype]
            [clojure.core.matrix :as m]))


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


(defn indexed-copy-test
  [src-fn dest-fn]
  (let [n-elems 100
        src-data (src-fn (range n-elems))
        src-indexes (range n-elems)
        dest-indexes (reverse (range n-elems))
        dest-data (dest-fn n-elems)
        result (double-array n-elems)]
    (dtype/indexed-copy! src-data 0 src-indexes dest-data 0 dest-indexes)
    (dtype/copy! dest-data 0 result 0 n-elems)
    (is (m/equals (vec (reverse (range n-elems)))
                  (vec result)))))



(deftest array->array-indexed-test
  (indexed-copy-test #(dtype/make-array-of-type :float %)
                     #(dtype/make-array-of-type :int %)))

(deftest array->buffer-indexed-test
  (indexed-copy-test #(dtype/make-array-of-type :float %)
                     #(dtype/make-buffer :int %)))

(deftest buffer->array-indexed-test
  (indexed-copy-test #(dtype/make-buffer :float %)
                     #(dtype/make-array-of-type :int %)))

(deftest buffer->buffer-indexed-test
  (indexed-copy-test #(dtype/make-buffer :float %)
                     #(dtype/make-array-of-type :int %)))

(deftest array->array-view-indexed-test
  (indexed-copy-test #(dtype/make-array-of-type :float %)
                     #(dtype/make-view :double %)))

(deftest array-view->array-indexed-test
  (indexed-copy-test #(dtype/make-view :float %)
                     #(dtype/make-array-of-type :double %)))

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
