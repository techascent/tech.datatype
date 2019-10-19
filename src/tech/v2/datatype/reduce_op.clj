(ns tech.v2.datatype.reduce-op
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.binary-op :as binary-op]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.protocols :as dtype-proto])
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
   `(make-reduce-op ~datatype ~update nil)))


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


(def iterable-reduce-table (casting/make-base-datatype-table
                            make-iterable-reduce-fn))


(defmulti iterable-reduce-map
  (fn [_options _reduce-op values]
    (dtype-base/buffer-type values)))


(defn default-iterable-reduce-map
  [{:keys [datatype unchecked?]} reduce-op values]
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


(defmacro make-commutative-reader-reduce-fn
  [datatype]
  `(fn [reduce-op# iterable# unchecked?#]
     (let [reduce-op# (binary-op/datatype->binary-op ~datatype reduce-op# unchecked?#)
           reader# (typecast/datatype->reader ~datatype iterable# unchecked?#)
           n-elems# (.lsize reader#)
           n-cpus# (.availableProcessors
                   (Runtime/getRuntime))
           n-elems-per-group# (quot n-elems# n-cpus#)
           n-groups# (+ n-cpus# 1)
           accum# (casting/datatype->sparse-value ~datatype)]
       (->> (range n-groups#)
            (pmap
             (fn [group-idx#]
               (let [group-idx# (long group-idx#)
                     start-idx# (* group-idx# n-elems-per-group#)
                     end-idx# (min n-elems#
                                   (+ start-idx# n-elems-per-group#))
                     n-loop-elems# (- end-idx# start-idx#)]
                      (loop [accum# accum#
                             idx# 0]
                        (if (< idx# n-loop-elems#)
                          (recur
                           (.op reduce-op#
                                accum#
                                (.read reader#
                                       (+ idx# start-idx#)))
                           (inc idx#))
                          accum#)))))
            (reduce (fn [accum# next-elem#]
                      (.op reduce-op# accum# next-elem#)))
            (#(.finalize reduce-op# % n-elems#))))))


(def commutative-reader-reduce-table
  (casting/make-base-datatype-table
   make-commutative-reader-reduce-fn))


(defn commutative-reader-reduce
  [{:keys [datatype unchecked?] :as options} reduce-op values]
  (if (and (dtype-proto/convertible-to-reader? values)
           (> (dtype-base/ecount values) 200))
    (let [datatype (or datatype (dtype-base/get-datatype values))
          reduce-fn (get commutative-reader-reduce-table
                         (casting/safe-flatten datatype))]
      (reduce-fn reduce-op values unchecked?))
    (iterable-reduce-map options reduce-op values)))


(defmacro commutative-reduce
  ([datatype update-code finalize-code values]
   `(commutative-reader-reduce
     {:datatype ~datatype}
     (make-reduce-op ~datatype ~update-code ~finalize-code)
     ~values))
  ([datatype update-code values]
   `(commutative-reduce ~datatype ~update-code ~'accum ~values))
  ([update-code values]
   `(commutative-reduce :object ~update-code ~'accum ~values)))


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


(def dot-product-table (casting/make-base-datatype-table
                        make-dot-product-op))


(defn default-dot-product
  [{:keys [datatype unchecked?] :as options} lhs rhs bin-op reduce-op]
  (if (and (dtype-proto/convertible-to-reader? lhs)
           (dtype-proto/convertible-to-reader? rhs))
    (->> (binary-op/binary-reader-map options bin-op lhs rhs)
         (commutative-reader-reduce options reduce-op))
    (let [datatype (or datatype (dtype-proto/get-datatype lhs))
          dot-prod-fn (get dot-product-table (casting/safe-flatten datatype))]
      (dot-prod-fn lhs rhs bin-op reduce-op unchecked?))))


(defmulti dot-product
  (fn [_options lhs rhs _bin-op _reduce-op]
    [(dtype-base/buffer-type lhs)
     (dtype-base/buffer-type rhs)]))


(defmethod dot-product :default
  [options lhs rhs bin-op reduce-op]
  (default-dot-product options lhs rhs bin-op reduce-op))
