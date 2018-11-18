(ns tech.datatype.jna-test
  (:require [clojure.test :refer :all]
            [tech.datatype.java-unsigned :as unsigned]
            [tech.datatype :as dtype]
            [tech.datatype.base :as base]
            [tech.datatype.jna :as dtype-jna]
            [clojure.core.matrix.macros :refer [c-for]]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(deftest jna-things-are-typed-pointers
  (let [test-buf (dtype-jna/make-typed-pointer :uint8 (range 255 245 -1))]
    (is (unsigned/typed-buffer? test-buf))
    (is (identical? test-buf (unsigned/as-typed-buffer test-buf)))
    (let [data-buf (int-array (dtype/ecount test-buf))]
      (dtype/copy! test-buf data-buf)
      (is (= (vec (range 255 245 -1))
             (dtype/->vector data-buf))))))


(deftest copy-time-test
  (testing "Run perf regression of times spent to copy data"
    (let [num-items (long 100000)
          src-data (float-array (range num-items))
          dst-data (float-array num-items)
          array-copy (fn []
                       (c-for [idx (int 0) (< idx num-items) (inc idx)]
                              (aset dst-data idx (aget src-data idx))))

          src-ptr (dtype-jna/make-typed-pointer :float32 src-data)
          dst-ptr (dtype-jna/make-typed-pointer :float32 num-items)

          ptr-ptr-copy (fn []
                         (dtype/copy! src-ptr 0 dst-ptr 0 num-items))

          array-ptr-copy (fn []
                           (dtype/copy! src-data 0 dst-ptr 0 num-items))
          fns {:array-hand-copy array-copy
               :ptr-ptr-copy ptr-ptr-copy
               :array-ptr-copy array-ptr-copy}
          run-timed-fns (fn []
                          (->> fns
                               (map (fn [[fn-name time-fn]]
                                      [fn-name (with-out-str
                                                 (time
                                                  (dotimes [iter 400]
                                                    (time-fn))))]))
                               (into {})))
          warmup (run-timed-fns)
          times (run-timed-fns)]
      (println times))))


(deftest set-constant!
  (let [test-buf (dtype-jna/make-typed-pointer :int64 5)]
    (dtype/set-constant! test-buf 0 1 (dtype/ecount test-buf))
    (is (= [1 1 1 1 1] (dtype/->vector test-buf)))
    (is (= [1 1 1 1 1] (-> (dtype/clone test-buf :datatype :uint8)
                           dtype/->vector)))
    (is (dtype-jna/typed-pointer? (dtype/from-prototype test-buf)))))


(deftest simple-init-ptr
  (let [test-ptr (dtype-jna/make-typed-pointer :int64 [2 2])]
    (is (= [2 2]
           (dtype/->vector test-ptr))))
  (let [test-ptr (dtype-jna/make-typed-pointer :float64 [2 2])]
    (is (= [2.0 2.0]
           (dtype/->vector test-ptr)))))
