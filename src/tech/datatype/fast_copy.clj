(ns tech.datatype.fast-copy
  (:require [tech.datatype.typecast :refer :all]
            [tech.datatype.protocols :as dtype-proto]
            [tech.jna :as jna]
            [tech.datatype.casting :as casting]
            [clojure.core.matrix.protocols :as mp])
  (:import  [com.sun.jna Pointer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(jna/def-jna-fn "c" memcpy
  "Copy bytes from one object to another"
  Pointer
  [dst ensure-ptr-like]
  [src ensure-ptr-like]
  [n-bytes int])


(defn copy!
  [dst src]
  (let [dst-ptr (as-ptr dst)
        dst-ary (as-array dst)
        src-ptr (as-ptr src)
        src-ary (as-array src)
        src-buf (as-nio-buffer src)
        dst-buf (as-nio-buffer dst)
        src-dtype (dtype-proto/get-datatype src)
        dst-dtype (dtype-proto/get-datatype dst)
        src-host-type (casting/datatype->host-datatype src-dtype)
        n-elems (long (mp/element-count dst))]
    (when-not (= src-host-type
                 (casting/datatype->host-datatype dst-dtype))
      (throw (ex-info "Fast copy called inappropriately; datatypes do not match"
                      {:src-datatype src-dtype
                       :dst-datatype dst-dtype})))
    (cond
      ;;Very fast path
      (and dst-ptr src-ary)
      (let [{:keys [array-data array-offset]} src-ary
            array-offset (int array-offset)]
        (case src-dtype
          :int8 (.write dst-ptr 0 ^bytes array-data array-offset n-elems)
          :int16 (.write dst-ptr 0 ^shorts array-data array-offset n-elems)
          :int32 (.write dst-ptr 0 ^ints array-data array-offset n-elems)
          :int64 (.write dst-ptr 0 ^longs array-data array-offset n-elems)
          :float32 (.write dst-ptr 0 ^floats array-data array-offset n-elems)
          :float64 (.write dst-ptr 0 ^doubles array-data array-offset n-elems)))
      (and dst-ary src-ptr)
      (let [{:keys [array-data array-offset]} dst-ary
            array-offset (int array-offset)]
        (case src-dtype
          :int8 (.read src-ptr 0 ^bytes array-data array-offset n-elems)
          :int16 (.read src-ptr 0 ^shorts array-data array-offset n-elems)
          :int32 (.read src-ptr 0 ^ints array-data array-offset n-elems)
          :int64 (.read src-ptr 0 ^longs array-data array-offset n-elems)
          :float32 (.read src-ptr 0 ^floats array-data array-offset n-elems)
          :float64 (.read src-ptr 0 ^doubles array-data array-offset n-elems)))
      (and src-buf dst-buf)
      (memcpy dst-buf src-buf (* n-elems (casting/numeric-byte-width src-dtype)))
      :else
      (throw (ex-info "Fast copy called inappropriately" {}))))
  dst)
