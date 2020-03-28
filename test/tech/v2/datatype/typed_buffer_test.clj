(ns tech.v2.datatype.typed-buffer-test
  (:require [tech.v2.datatype.typed-buffer :as typed-buffer]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.list]
            [clojure.test :refer :all])
  (:import [java.nio ByteBuffer]))


(deftest unsigned-basics
  (let [test-data [255 254 0 1 2 3]
        src-buffer (short-array test-data)
        n-elems (dtype/ecount src-buffer)
        dst-buffer (typed-buffer/make-typed-buffer :uint8 n-elems)
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
    (let [dst-buffer (dtype-proto/sub-buffer dst-buffer 1 (- n-elems 1))
          test-ary (short-array (dtype/ecount dst-buffer))]
      (dtype/copy! dst-buffer test-ary)

      (is (= [254 0 1 2 3]
             (vec test-ary)))
      (is (= 254
             (dtype/get-value dst-buffer 0)))
      (dtype/set-value! dst-buffer 0 255)
      (dtype/copy! dst-buffer test-ary)
      (is (= [255 0 1 2 3]
             (vec test-ary)))
      (is (= [255 0 1 2 3]
             (-> (dtype/clone dst-buffer)
                 (dtype/->vector))))
      (is (= :uint8
             (-> (dtype/clone dst-buffer)
                 (dtype/get-datatype)))))))

(deftest unsigned-basics-list
  (testing "Lists can be used in typed buffers"
    (let [test-data [255 254 0 1 2 3]
          src-buffer (dtype-proto/make-container :list :int64 test-data {})
          n-elems (dtype/ecount src-buffer)
          dst-buffer (-> (dtype-proto/make-container :list :int8 n-elems {})
                         (typed-buffer/set-datatype :uint8))
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

      (let [new-buf (dtype-proto/make-container :list :float32 n-elems {})]
        (dtype/copy! dst-buffer new-buf)
        (is (= test-data (mapv long new-buf))))
      (let [dst-buffer (dtype-proto/sub-buffer dst-buffer 1 (- n-elems 1))
            test-ary (short-array (dtype/ecount dst-buffer))]
        (dtype/copy! dst-buffer test-ary)

        (is (= [254 0 1 2 3]
               (vec test-ary)))
        (is (= 254
               (dtype/get-value dst-buffer 0)))
        (dtype/set-value! dst-buffer 0 255)
        (dtype/copy! dst-buffer test-ary)
        (is (= [255 0 1 2 3]
               (vec test-ary)))
        (is (= [255 0 1 2 3]
               (-> (dtype/clone dst-buffer)
                   (dtype/->vector))))
        (is (= :uint8
               (-> (dtype/clone dst-buffer)
                   (dtype/get-datatype))))))))


(deftest negative-numbers-always-wrong
  (doseq [datatype casting/unsigned-int-types]
    (is (thrown? Throwable
                 (dtype/cast -1 datatype)))))


(deftest get-value-on-things
  (is (= :one (dtype/get-value :one 0)))
  (is (= :alpha (dtype/get-value {3 :alpha} 3)))
  (is (= 3 (dtype/get-value [1 2 3] 2)))
  (is (thrown? Throwable (dtype/get-value :one 1))))


(deftest satsifies-interface-requirements
  (is (typed-buffer/typed-buffer? (typed-buffer/make-typed-buffer :uint8 5 {})))
  (is (typed-buffer/typed-buffer? (double-array 5)))
  (is (typed-buffer/typed-buffer? (ByteBuffer/allocate 5))))


(deftest ->array-respects-limit
  (let [nio-buf (doto (ByteBuffer/wrap (byte-array [1 2 3 4 5]))
                  (.limit 2))]
    (is (= 2 (dtype/ecount nio-buf)))
    (is (nil? (dtype/->array nio-buf)))
    (is (= (dtype/->vector nio-buf) [1 2]))))


(deftest make-array-all-variants
  (let [src-buf (typed-buffer/make-typed-buffer :uint8 (range 5) {})
        test-vec (dtype/->vector src-buf)
        make-array-test-fn (fn [target-dtype input]
                             (let [dest-ary (dtype/make-array-of-type
                                             target-dtype input)]
                               (is (= test-vec
                                      (mapv long (dtype/->vector dest-ary))))))]
    (make-array-test-fn :int8 src-buf)
    (make-array-test-fn :float32 src-buf)
    (make-array-test-fn :int64 (dtype/->vector src-buf))
    (make-array-test-fn :int32 (int-array (dtype/->vector src-buf)))))
