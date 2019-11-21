(ns tech.v2.datatype.javacpp-test
  (:require [clojure.test :refer :all]
            [tech.v2.datatype.javacpp :as jcpp-dtype]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.jna :as dtype-jna]
            [tech.resource :as resource])
  ;;required to load some of the javacpp help functions; they are small functions
  ;;compiled into each bound library.
  (:import [org.bytedeco.javacpp opencv_core]))

;;Force loading of the class to make unit tests work
(println opencv_core/ACCESS_FAST)


(deftest ensure-fast-copy-paths
  (resource/stack-resource-context
    (let [int-data (int-array [1 2 3 4 5 6])
          result (jcpp-dtype/make-pointer-of-type :float32 6)
          short-data (short-array 6)]
      (dtype/copy! int-data 0 result 0 6)
      (dtype/copy! result 0 short-data 0 6)
      (is (= [1 2 3 4 5 6]
             (vec short-data))))))

(deftest typed-pointers
  (resource/stack-resource-context
    (let [src-data (range 255 235 -1)
          typed-ptr (jcpp-dtype/make-typed-pointer :uint8 src-data)
          result-data (short-array 20)
          byte-data (byte-array 20)
          signed-data (range -1 -21 -1)]
      (dtype/copy! typed-ptr result-data)
      (is (= (vec result-data)
             (vec src-data)))
      (dtype/copy! typed-ptr 0 byte-data 0 20 {:unchecked? true})
      (is (= (vec signed-data)
             (vec byte-data))))))


(deftest datatype-base-->array
  (resource/stack-resource-context
    (let [base-ptr (jcpp-dtype/make-pointer-of-type :float32 (range 10))
          typed-ptr (jcpp-dtype/make-typed-pointer :int32 10)]
      ;;These should be nil but should not cause exceptions.
      (is (and (not (dtype/->array base-ptr))
               (not (dtype/->array typed-ptr))))
      (dtype/copy! base-ptr typed-ptr)
      (is (= (vec (range 10))
             (dtype/->vector typed-ptr))))))


(deftest jcpp-ptr-interface-specification
  (resource/stack-resource-context
    (let [base-ptr (jcpp-dtype/make-pointer-of-type :float32 (range 10))]
      (is (instance? org.bytedeco.javacpp.Pointer (dtype/clone base-ptr)))
      (is (= (dtype/->vector base-ptr)
             (dtype/->vector (dtype/clone base-ptr :datatype :float32 )))))))
