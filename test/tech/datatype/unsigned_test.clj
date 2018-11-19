(ns tech.datatype.unsigned-test
  (:require [tech.datatype.java-unsigned :as unsigned]
            [tech.datatype :as dtype]
            [clojure.test :refer :all])
  (:import [java.nio ByteBuffer]))


(deftest unsigned-basics
  (let [test-data [255 254 0 1 2 3]
        src-buffer (short-array test-data)
        n-elems (dtype/ecount src-buffer)
        dst-buffer (unsigned/make-typed-buffer :uint8 n-elems)
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
    (let [^ByteBuffer storage-buf (:buffer dst-buffer)
          _ (.position storage-buf 1)
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
      (is (= :uint32
             (-> (dtype/clone dst-buffer :datatype :uint32)
                 (dtype/get-datatype)))))))


(deftest negative-numbers-always-wrong
  (doseq [datatype unsigned/unsigned-datatypes]
    (is (thrown? Throwable
                 (dtype/cast -1 datatype)))))


(deftest get-value-on-things
  (is (= :one (dtype/get-value :one 0)))
  (is (= 3 (dtype/get-value {:alpha 3} :alpha)))
  (is (= 3 (dtype/get-value [1 2 3] 2)))
  (is (thrown? Throwable (dtype/get-value :one 1))))


(deftest satsifies-interface-requirements
  (is (unsigned/typed-buffer? (unsigned/make-typed-buffer :uint8 5 {})))
  (is (unsigned/typed-buffer? (double-array 5)))
  (is (unsigned/typed-buffer? (ByteBuffer/allocate 5))))
