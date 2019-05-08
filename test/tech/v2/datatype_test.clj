(ns tech.v2.datatype-test
  (:require [clojure.test :refer :all]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.base :as base]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.primitive]
            [tech.v2.datatype.list]
            [tech.parallel.for :as parallel-for]
            [tech.v2.datatype.functional :as dfn])
  (:import [java.nio FloatBuffer]))


(deftest raw-copy-with-mutable-lazy-sequence
  ;;It is important that raw copy can work with a lazy sequence of double buffers where
  ;;the same buffer is being filled for ever member of the lazy sequence.  This is an
  ;;easy optimization to make that cuts down the memory usage when reading from datasets
  ;;by a fairly large amount.
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
  [src-fn dest-fn src-dtype dst-dtype]
  (let [ary (dtype/make-container src-fn src-dtype (range 10))
        buf (dtype/make-container dest-fn dst-dtype 10)
        retval (dtype/make-jvm-container :float64 10)]
    ;;copy starting at position 2 of ary into position 4 of buf 4 elements
    (dtype/copy! ary 2 buf 4 4)
    (dtype/copy! buf 0 retval 0 10)
    (is (= [0 0 0 0 2 3 4 5 0 0]
           (mapv int (dtype/->vector retval)))
        (pr-str {:src-fn src-fn :dest-fn dest-fn
                 :src-dtype src-dtype :dst-dtype dst-dtype}))
    (is (= 10 (dtype/ecount buf)))))


(def create-functions [:typed-buffer :native-buffer :list :sparse])


(deftest generalized-copy-test
  (->> (for [src-container create-functions
             dst-container create-functions
             src-dtype casting/numeric-types
             dst-dtype casting/numeric-types]
         (basic-copy src-container dst-container src-dtype dst-dtype))
       dorun))


(deftest array-of-array-support
  (let [^"[[D" src-data (make-array (Class/forName "[D") 5)
        _ (doseq [idx (range 5)]
            (aset src-data idx (double-array (repeat 10 idx))))
        dst-data (float-array (* 5 10))]
    ;;This should not hit any slow paths.
    (dtype/copy-raw->item! src-data dst-data 0)
    (is (= (vec (float-array (flatten (map #(repeat 10 %) (range 5)))))
           (vec dst-data)))))


(deftest out-of-range-data-causes-exception
  (is (thrown? Throwable (dtype/copy! (int-array [1000 2000 3000 4000])
                                      (byte-array 4)))))


(deftest out-of-range-data-does-not-cause-exception-if-unchecked
  (let [byte-data
        (dtype/copy! (int-array [1000 2000 3000 4000]) 0
                     (byte-array 4) 0
                     4 {:unchecked? true})]
    (is (= [-24 -48 -72 -96]
           (vec byte-data)))))


(deftest offset-buffers-should-copy-correctly
  (let [fbuf (-> (dtype/make-buffer-of-type :float32 (range 10))
                 (dtype/sub-buffer 3))
        result-buf (dtype/make-jvm-container :float32 (dtype/ecount fbuf))]
    (dtype/copy! fbuf result-buf)
    (is (= (dtype/get-value fbuf 0)
           (float 3)))
    (is (= (vec (drop 3 (range 10)))
           (mapv long (dtype/->vector result-buf))))))


(deftest primitive-types-are-typed
  (doseq [[cls dtype] [[(byte 1) :int8]
                       [(short 1) :int16]
                       [(int 1) :int32]
                       [(long 1) :int64]
                       [(float 1) :float32]
                       [(double 1) :float64]
                       [(boolean false) :boolean]]]
    (is (= dtype (dtype/get-datatype cls)))))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(deftest copy-time-test
  (testing "Run perf regression of times spent to copy data"
    (let [num-items (long 100000)
          src-data (float-array (range num-items))
          dst-data (float-array num-items)
          array-copy (fn []
                       (parallel-for/parallel-for
                        idx num-items
                        (aset dst-data idx (aget src-data idx))))
          src-buf (FloatBuffer/wrap src-data)
          dst-buf (FloatBuffer/wrap dst-data)
          buffer-copy (fn []
                        (let [
                              ;; Curiously, uncommenting this gets a far faster result.
                              ;; But it isn't at all practical.
                              ;; src-buf (FloatBuffer/wrap src-data)
                              ;; dst-buf (FloatBuffer/wrap dst-data)
                              ]
                          (parallel-for/parallel-for
                           idx num-items
                           (.put dst-buf idx (.get src-buf idx)))))

          dtype-copy (fn []
                       (dtype/copy! src-buf 0 dst-buf 0 num-items))

          unchecked-dtype-copy (fn []
                                 (dtype/copy! src-buf 0 dst-buf 0 num-items
                                              {:unchecked? true}))

          make-array (fn []
                       (dtype/make-container :java-array :float32 dst-buf))
          marshal-buf (int-array num-items)
          ;;If you have to do a marshalling copy then exploiting parallelism will be
          ;;your best bet.  It costs a lot to marshal across datatypes, esp. int->float.
          marshal-copy (fn []
                         (dtype/copy! src-data 0
                                      marshal-buf 0
                                      num-items
                                      {:unchecked? true}))
          fns {:array-copy array-copy
               :buffer-copy buffer-copy
               :dtype-copy dtype-copy
               :unchecked-dtype-copy unchecked-dtype-copy
               :make-array make-array
               :marshal-copy marshal-copy}
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


(deftest nil-has-nil-shape
  (is (= nil (dtype/shape nil)))
  (is (= 0 (dtype/ecount nil)))
  (is (= 0 (dtype/shape->ecount (dtype/shape nil)))))


(deftest boolean-support
  (is (= [0 1 0]
         (-> (dtype/make-container :java-array  :int8 [false true false])
             (dtype/->vector))))


  (is (= [0 1 0]
         (-> (dtype/make-container :java-array :int8
                                   (boolean-array [false true false]))
             (dtype/->vector))))


  (is (= [0 1 0]
         (-> (dtype/copy! (boolean-array [false true false]) 0
                          (dtype/make-jvm-container :int8 3) 0
                          3
                          {:unchecked? true})
             (dtype/->vector))))


  (is (= [false true false]
         (-> (dtype/make-jvm-container :boolean [0 1 0])
             (dtype/->vector))))


  (is (= [false true false]
         (-> (dtype/make-jvm-container :boolean [0.0 0.01 0.0])
             (dtype/->vector))))

  (is (= true (dtype/cast 10 :boolean)))

  (is (= false (dtype/cast 0 :boolean)))

  (is (= 1 (dtype/cast true :int16)))

  (is (= 0.0 (dtype/cast false :float64))))


(deftest list-regression
  (is (= (Class/forName "[D")
         (type (dtype/->array-copy
                (dtype/make-container :list :float64 10)))))
  (is (= (Class/forName "[D")
         (type (dtype/->array-copy
                (-> (dtype/make-container :list :float64 10)
                    (dtype/sub-buffer 2 2)))))))


(deftest nested-array-things-have-appropriate-shape
  (is (= [5 5]
         (->> (repeatedly 5 #(double-array 5))
              (into-array)
              dtype/shape)))
  (is (= 25
         (->> (repeatedly 5 #(double-array 5))
              (into-array)
              dtype/ecount)))
  (is (= [5 5]
         (->> (repeatedly 5 #(range 5))
              (into-array)
              dtype/shape)))
  (is (= 25
         (->> (repeatedly 5 #(range 5))
              (into-array)
              dtype/ecount))))


(deftest base-math-sanity
  (is (= 0.0 (-> (dfn/- (range 10) (range 10))
                 (dfn/pow 2)
                 (dfn/+))))

  (is (= 0.0 (-> (dfn/- (into-array (range 10)) (range 10))
                 (dfn/pow 2)
                 (dfn/+)))))
