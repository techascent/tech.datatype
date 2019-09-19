(ns tech.v2.datatype.mutable.iterable-to-list
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]))


(defmacro make-iter->list-table
  []
  `(->> [~@(for [dtype casting/base-marshal-types]
             [dtype `(fn [iter# output# unchecked?#]
                       (let [iter# (typecast/datatype->iter ~dtype iter# unchecked?#)
                             output# (or output#
                                         (dtype-proto/make-container
                                          :list ~dtype 0))
                             mutable# (typecast/datatype->mutable ~dtype output#
                                                                  unchecked?#)]
                         (while (.hasNext iter#)
                           (.append mutable# (typecast/datatype->iter-next-fn
                                              ~dtype iter#)))
                         output#))])]
        (into {})))


(def iter->list-table (make-iter->list-table))


(defn iterable->list
  [src-iterable dst-list {:keys [datatype unchecked?]}]
  (let [datatype (or datatype (dtype-proto/get-datatype src-iterable))
        dst-list (or dst-list (dtype-proto/make-container :list datatype 0 {}))
        iter-fn (get iter->list-table (casting/safe-flatten datatype))]
    (iter-fn src-iterable dst-list unchecked?)))
