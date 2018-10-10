(ns tech.datatype.marshal-unchecked
  (:require [clojure.core.matrix.macros :refer [c-for]]
            [tech.datatype.base :as base]
            [clojure.set :as cset]
            [tech.datatype.marshal :as marshal]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro datatype->unchecked-cast-fn
  [src-dtype dtype val]
  (if (= src-dtype dtype)
    val
    (case dtype
      :int8 `(unchecked-byte ~val)
      :int16 `(unchecked-short ~val)
      :int32 `(unchecked-int ~val)
      :int64 `(unchecked-long ~val)
      :float32 `(unchecked-float ~val)
      :float64 `(unchecked-double ~val))))


(defmacro buffer-buffer-copy
  [src-dtype dst-dtype src src-offset dst dst-offset n-elems options]
  `(let [src# (marshal/datatype->buffer-cast-fn ~src-dtype ~src)
         dst# (marshal/datatype->buffer-cast-fn ~dst-dtype ~dst)
         src-offset# (long ~src-offset)
         dst-offset# (long ~dst-offset)
         n-elems# (long ~n-elems)]
     (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
            (.put dst# (+ idx# dst-offset#)
                  (datatype->unchecked-cast-fn ~src-dtype ~dst-dtype
                                               (.get src# (+ idx# src-offset#)))))))


(defmacro update-copy-table
  []
  `(vector
    ~@(for [src-container [:nio-buffer]
            dst-container [:nio-buffer]
            src-dtype base/datatypes
            dst-dtype base/datatypes]
        `(do (marshal/add-copy-operation ~src-container ~dst-container ~src-dtype ~dst-dtype true
                                         (fn [src# src-offset# dst# dst-offset# n-elems# options#]
                                           (buffer-buffer-copy ~src-dtype ~dst-dtype
                                                               src# src-offset# dst# dst-offset# n-elems# options#)))
             [~src-container ~dst-container ~src-dtype ~dst-dtype false]))))

(def core-copy-operations (update-copy-table))
