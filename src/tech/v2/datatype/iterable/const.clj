(ns tech.v2.datatype.iterable.const
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.nio-access :as access]))



(defmacro make-const-iter
  [datatype]
  (let [host-type (casting/safe-flatten datatype)]
    `(fn [item#]
       (let [item# (access/checked-full-write-cast
                    item# :unknown ~datatype
                    ~host-type)]
         (reify
           Iterable
           (iterator [item]
             (reify ~(typecast/datatype->iter-type host-type)
               (getDatatype [iter#] ~datatype)
               (hasNext [iter#] true)
               (~(typecast/datatype->iter-next-fn-name host-type) [iter#] item#)
               (current [iter#] item#)))
           dtype-proto/PDatatype
           (get-datatype [item] ~datatype))))))


(defmacro make-const-iter-table
  []
  `(->> [~@(for [dtype casting/base-marshal-types]
             [dtype `(make-const-iter ~dtype)])]
        (into {})))


(def const-iter-table (make-const-iter-table))


(defn make-const-iterable
  [item datatype]
  (if-let [iter-fn (get const-iter-table (casting/flatten-datatype datatype))]
    (iter-fn item)
    (throw (ex-info (format "Failed to find iter for datatype %s" datatype) {}))))
