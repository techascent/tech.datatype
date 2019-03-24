(ns tech.sparse.set-ops
  "Set operation utilities for sparse buffers"
  (:require [tech.datatype :as dtype]
            [tech.datatype.casting :as casting]
            [tech.datatype.typecast :as typecast]
            [clojure.core.matrix.macros :refer [c-for]])
  (:import [tech.datatype IntReader IntMutable IntWriter]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defprotocol PBufferAdder
  (add-left [item])
  (ignore-left [item])
  (add-right [item])
  (result [item]))


(defmacro make-buffer-adder
  [datatype lhs-data-buf rhs-data-buf unchecked?]
  `(let [lhs-iter# (typecast/datatype->iter ~datatype ~lhs-data-buf ~unchecked?)
         rhs-iter# (typecast/datatype->iter ~datatype ~rhs-data-buf ~unchecked?)
         result# (dtype/make-container :list ~datatype 0)
         result-mut# (typecast/datatype->mutable ~datatype result# ~unchecked?)
         zero-val# (casting/datatype->sparse-value ~datatype)]
     (reify PBufferAdder
       (add-left [item#]
         (let [temp-val# (typecast/datatype->iter-next-fn ~datatype lhs-iter#)
               is-not-zero?# (not= temp-val# zero-val#)]
           (when is-not-zero?#
             (.append result-mut# temp-val#))
           is-not-zero?#))
       (ignore-left [item#]
         (typecast/datatype->iter-next-fn ~datatype lhs-iter#)
         true)
       (add-right [item#]
         (let [temp-val# (typecast/datatype->iter-next-fn ~datatype rhs-iter#)
               is-not-zero?# (not= temp-val# zero-val#)]
           (when is-not-zero?#
             (.append result-mut# temp-val#))
           is-not-zero?#))
       (result [item] result#))))


(defmacro buffer-adder-construct-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             [dtype `(fn [lhs-buf# rhs-buf# unchecked?#]
                       (make-buffer-adder ~dtype lhs-buf# rhs-buf# unchecked?#))])]
        (into {})))

(def buffer-adder-table (buffer-adder-construct-table))


(defn union-values
  "Union two buffers producing a new idx buf and a new value buffer.
  In the case of both buffers having the value, the rhs buffer overrides
  the value.  Note that is does not assume that lhs or rhs data buffers
  do *not* contain the zero value and thus it does an implicit filter
  step.
  returns
  {:indexes new-indexes
   :values new-values}"
  [lhs-idx-buf lhs-data-buf rhs-idx-buf rhs-data-buf unchecked?]
  (if (= 0 (dtype/ecount lhs-idx-buf))
    {:indexes (dtype/make-container :list :int32 0)
     :values (dtype/make-container :list (dtype/get-datatype lhs-idx-buf) 0)}
    (let [lhs-iter (typecast/datatype->iter :int32 lhs-idx-buf true)
          rhs-iter (typecast/datatype->iter :int32 rhs-idx-buf true)
          result-idx-buf (dtype/make-container :list :int32 0)
          result-mut (typecast/datatype->mutable :int32 result-idx-buf true)
          dtype (dtype/get-datatype lhs-data-buf)
          add-fn (get buffer-adder-table (casting/flatten-datatype dtype))
          _ (when-not add-fn
              (throw (ex-info (format "Failed to find add fn for datatype %s" dtype)
                              {})))
          adder (add-fn lhs-data-buf rhs-data-buf unchecked?)]
      (loop [left-has-more? (.hasNext lhs-iter)
             right-has-more? (.hasNext rhs-iter)]
        (if (or left-has-more? right-has-more?)
          (do
            (if (and left-has-more? right-has-more?)
              (let [left-idx (.current lhs-iter)
                    right-idx (.current rhs-iter)]
                (if (< left-idx right-idx)
                  (do
                    (.next lhs-iter)
                    (when (add-left adder)
                      (.append result-mut left-idx)))
                  (do
                    (.next rhs-iter)
                    (when (add-right adder)
                      (.append result-mut right-idx))
                    (when (= left-idx right-idx)
                      (.next lhs-iter)
                      (ignore-left adder)))))
              (if left-has-more?
                (while (.hasNext lhs-iter)
                  (when (add-left adder)
                    (.append result-mut (.nextInt lhs-iter))))
                (while (.hasNext rhs-iter)
                  (when (add-right adder)
                    (.append result-mut (.nextInt rhs-iter))))))
            (recur (.hasNext lhs-iter)
                   (.hasNext rhs-iter)))
          {:indexes result-idx-buf
           :values (result adder)})))))
