(ns tech.v2.datatype.reduce-op
  (:require [tech.v2.datatype.reader :as reader]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.iterator :as iterator]
            [tech.v2.datatype.binary-op :as binary-op]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.nio-access :as nio-access]
            [tech.v2.datatype.argtypes :as argtypes])
  (:import [tech.v2.datatype
            BinaryOperators$ByteBinary  BinaryOperators$ShortBinary
            BinaryOperators$IntBinary  BinaryOperators$LongBinary
            BinaryOperators$FloatBinary  BinaryOperators$DoubleBinary
            BinaryOperators$BooleanBinary  BinaryOperators$ObjectBinary]
           [clojure.lang IFn]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn datatype->reduce-op-type
  [datatype]
  (binary-op/datatype->binary-op-type datatype))


(defmacro make-reduce-op
  "Make a reduce op of type datatype.  Arguments to the operation
  are exposed to the local scope as 'x' and 'y' respectively.  If finalize
  is provided it is implemented else default is to return accum.
  (make-reduce-op :float32 (Math/pow x y))"
  ([opname datatype update finalize]
   `(reify ~(binary-op/datatype->binary-op-type datatype)
      (getDatatype [item#] ~datatype)
      (op [item# ~'accum ~'next]
        ~update)
      ~@(when finalize
          [`(finalize [item# ~'accum ~'num-elems]
                      ~finalize)])

      dtype-proto/POperator
      (op-name [item] ~opname)))
  ([datatype update finalize]
   `(make-reduce-op :unnamed ~datatype ~update ~finalize))
  ([datatype update]
   `(make-reduce-up ~datatype ~update nil)))


(defmacro make-iterable-reduce-fn
  [datatype]
  `(fn [reduce-op# iterable# unchecked?#]
     (let [reduce-op# (binary-op/datatype->binary-op ~datatype reduce-op# unchecked?#)
           iter# (typecast/datatype->iter ~datatype iterable# unchecked?#)]
       (loop [accum# (casting/datatype->sparse-value ~datatype)
              next?# (.hasNext iter#)
              item-count# (int 0)]
         (if next?#
           (if (= item-count# 0)
             (recur (typecast/datatype->iter-next-fn ~datatype iter#)
                    (.hasNext iter#) 1)
             (recur (.op reduce-op# accum# (.next iter#))
                    (.hasNext iter#)
                    (inc item-count#)))
           (.finalize reduce-op# accum# item-count#))))))


(defmacro make-iterable-reduce-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-iterable-reduce-fn ~dtype)])]
        (into {})))


(def iterable-reduce-table (make-iterable-reduce-table))


(defmulti iterable-reduce-map
  (fn [options reduce-op values]
    (dtype-base/buffer-type values)))


(defn default-iterable-reduce-map
  [{:keys [datatype unchecked?] :as options} reduce-op values]
  (let [datatype (or datatype (dtype-base/get-datatype values))
        reduce-fn (get iterable-reduce-table (casting/safe-flatten datatype))]
    (reduce-fn reduce-op values unchecked?)))


(defmethod iterable-reduce-map :default
  [options reduce-op values]
  (default-iterable-reduce-map options reduce-op values))


(defmacro iterable-reduce
  ([datatype update-code finalize-code values]
   `(iterable-reduce-map
     {:datatype ~datatype}
     (make-reduce-op ~datatype ~update-code ~finalize-code)
     ~values))
  ([datatype update-code values]
   `(iterable-reduce ~datatype ~update-code ~'accum ~values))
  ([update-code values]
   `(iterable-reduce :object ~update-code ~'accum ~values)))


(defmacro make-dot-product-op
  [datatype]
  `(fn [lhs# rhs# bin-op# reduce-op# unchecked?#]
     (let [lhs# (typecast/datatype->iter ~datatype lhs# unchecked?#)
           rhs# (typecast/datatype->iter ~datatype rhs# unchecked?#)
           bin-op# (binary-op/datatype->binary-op ~datatype bin-op# true)
           reduce-op# (binary-op/datatype->binary-op ~datatype reduce-op# true)]
       (loop [n-elems# 0
              sum# (casting/datatype->sparse-value ~datatype)]
         (if (and (.hasNext lhs#)
                  (.hasNext rhs#))
           (recur (unchecked-inc n-elems#)
                  (if (= 0 n-elems#)
                    (.op bin-op#
                         (typecast/datatype->iter-next-fn ~datatype lhs#)
                         (typecast/datatype->iter-next-fn ~datatype rhs#))
                    (.op reduce-op#
                         sum#
                         (.op bin-op#
                              (typecast/datatype->iter-next-fn ~datatype lhs#)
                              (typecast/datatype->iter-next-fn ~datatype rhs#)))))
           (.finalize reduce-op# sum# n-elems#))))))


(defmacro make-dot-product-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-dot-product-op ~dtype)])]
        (into {})))


(def dot-product-table (make-dot-product-table))


(defn default-dot-product
  [{:keys [datatype unchecked?]} lhs rhs bin-op reduce-op]
  (let [datatype (or datatype (dtype-base/get-datatype lhs))
        dot-prod-fn (get dot-product-table (casting/safe-flatten datatype))]
    (dot-prod-fn lhs rhs bin-op reduce-op unchecked?)))


(defmulti dot-product
  (fn [options lhs rhs bin-op reduce-op]
    [(dtype-base/buffer-type lhs)
     (dtype-base/buffer-type rhs)]))


(defmethod dot-product :default
  [options lhs rhs bin-op reduce-op]
  (default-dot-product options lhs rhs bin-op reduce-op))
