(ns tech.datatype.core-test
  (:require [clojure.test :refer :all]
            [tech.datatype.core :as dtype]
            [tech.datatype.base :as base]
            [clojure.core.matrix :as m]
            [tech.datatype.util :as util]))


(deftest raw-copy-with-mutable-lazy-sequence
  ;;It is important that raw copy can work with a lazy sequence of double buffers where
  ;;the same buffer is being filled for ever member of the lazy sequence.  This is an easy
  ;;optimization to make that cuts down the memory usage when reading from datasets by
  ;;a fairly large amount.
  (let [input-seq (partition 10 (range 100))
        input-ary (double-array 10)
        ;;Note the input ary is being reused.  Now imagine the input ary is a large
        ;;and we want a batch size of 50.  This is precisely the use case
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
  (let [ary (src-fn :float32 (range 10))
        buf (dest-fn :float64 10)
        retval (dtype/make-array-of-type :float64 10)]
    ;;copy starting at position 2 of ary into position 4 of buf 4 elements
    (dtype/copy! ary 2 buf 4 4)
    (dtype/copy! buf 0 retval 0 10)
    (is (m/equals [0 0 0 0 2 3 4 5 0 0]
                  (vec retval)))))


(def create-functions [dtype/make-array-of-type])



(deftest generalized-copy-test
  (mapv (fn [[src-fn dest-fn]]
          (basic-copy src-fn dest-fn))
        (util/all-pairs create-functions)))


(deftest array-of-array-support
  (let [^"[[D" src-data (make-array (Class/forName "[D") 5)
        _ (doseq [idx (range 5)]
            (aset src-data idx (double-array (repeat 10 idx))))
        dst-data (float-array (* 5 10))]
    ;;This should not hit any slow paths.
    (with-bindings {#'base/*error-on-slow-path* true}
      (dtype/copy-raw->item! src-data dst-data 0))
    (is (= (vec (float-array (flatten (map #(repeat 10 %) (range 5)))))
           (vec dst-data)))))


(def demo-graph {:red    {:green 10, :blue   5, :orange 8},
                 :green  {:red 10,   :blue   3},
                 :blue   {:green 3,  :red    5, :purple 7},
                 :purple {:blue 7,   :orange 2},
                 :orange {:purple 2, :red 2}
                 :island {}
                 :mainland {:island 2}})

(def demo-edge-pairs (->> demo-graph
                          (mapcat (fn [[k v-map]]
                                    (map vector (repeat k) (keys v-map))))))
