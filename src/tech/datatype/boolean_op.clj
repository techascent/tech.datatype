(ns tech.datatype.boolean-op
  (:require [tech.datatype.typecast :as typecast]
            [tech.datatype.casting :as casting]
            [tech.datatype.iterator :as iterator]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.reader :as reader]
            [tech.datatype.unary-op :as dtype-unary]
            [tech.datatype.binary-op :as dtype-binary]
            [tech.datatype.base :as dtype-base])
  (:import [tech.datatype
            BooleanOp$ByteUnary BooleanOp$ByteBinary
            BooleanOp$ShortUnary BooleanOp$ShortBinary
            BooleanOp$IntUnary BooleanOp$IntBinary
            BooleanOp$LongUnary BooleanOp$LongBinary
            BooleanOp$FloatUnary BooleanOp$FloatBinary
            BooleanOp$DoubleUnary BooleanOp$DoubleBinary
            UnaryOperators$BooleanUnary BinaryOperators$BooleanBinary
            BooleanOp$ObjectUnary BooleanOp$ObjectBinary]
           [clojure.lang IFn]))


(defn datatype->boolean-unary-type
  [datatype]
  (case datatype
    :int8 'tech.datatype.BooleanOp$ByteUnary
    :int16 'tech.datatype.BooleanOp$ShortUnary
    :int32 'tech.datatype.BooleanOp$IntUnary
    :int64 'tech.datatype.BooleanOp$LongUnary
    :float32 'tech.datatype.BooleanOp$FloatUnary
    :float64 'tech.datatype.BooleanOp$DoubleUnary
    :boolean 'tech.datatype.UnaryOperators$BooleanUnary
    :object 'tech.datatype.BooleanOp$ObjectUnary))


(defmacro make-boolean-unary-op
  "Make a boolean unary operator.  Input is named 'arg and output will be expected to be
  boolean."
  [datatype body]
  (let [host-dtype (casting/safe-flatten datatype)]
    `(reify
       ~(datatype->boolean-unary-type host-dtype)
       (op [item# ~'arg]
         ~body)
       dtype-proto/PDatatype
       (get-datatype [item#] ~host-dtype)
       IFn
       (invoke [item# arg#]
         (.op item# (casting/datatype->cast-fn :unknown ~datatype arg#))))))


(defmacro implement-unary-typecast
  [datatype]
  (let [expected-type (resolve (datatype->boolean-unary-type datatype))]
    `(if (instance? ~expected-type ~'item)
       ~'item
       (throw (ex-info (format "Expected %s, got %s" ~expected-type (type ~'item)) {})))))


(defn int8->boolean-unary ^BooleanOp$ByteUnary [item] (implement-unary-typecast :int8))
(defn int16->boolean-unary ^BooleanOp$ShortUnary [item] (implement-unary-typecast :int16))
(defn int32->boolean-unary ^BooleanOp$IntUnary [item] (implement-unary-typecast :int32))
(defn int64->boolean-unary ^BooleanOp$LongUnary [item] (implement-unary-typecast :int64))
(defn float32->boolean-unary ^BooleanOp$FloatUnary [item] (implement-unary-typecast :float32))
(defn float64->boolean-unary ^BooleanOp$DoubleUnary [item] (implement-unary-typecast :float64))
(defn boolean->boolean-unary ^UnaryOperators$BooleanUnary [item] (implement-unary-typecast :boolean))
(defn object->boolean-unary ^BooleanOp$ObjectUnary [item] (implement-unary-typecast :object))


(defmacro datatype->boolean-unary
  [datatype item]
  (case datatype
    :int8 `(int8->boolean-unary ~item)
    :int16 `(int16->boolean-unary ~item)
    :int32 `(int32->boolean-unary ~item)
    :int64 `(int64->boolean-unary ~item)
    :float32 `(float32->boolean-unary ~item)
    :float64 `(float64->boolean-unary ~item)
    :boolean `(boolean->boolean-unary ~item)
    :object `(object->boolean-unary ~item)))


(defn datatype->boolean-binary-type
  [datatype]
  (case datatype
    :int8 'tech.datatype.BooleanOp$ByteBinary
    :int16 'tech.datatype.BooleanOp$ShortBinary
    :int32 'tech.datatype.BooleanOp$IntBinary
    :int64 'tech.datatype.BooleanOp$LongBinary
    :float32 'tech.datatype.BooleanOp$FloatBinary
    :float64 'tech.datatype.BooleanOp$DoubleBinary
    :boolean 'tech.datatype.BinaryOperators$BooleanBinary
    :object 'tech.datatype.BooleanOp$ObjectBinary))


(defmacro make-boolean-binary-op
    "Make a boolean unary operator.  Inputs are named 'x' and 'y' respectively and
  output will be expected to be boolean."
  [datatype body]
  (let [host-dtype (casting/safe-flatten datatype)]
    `(reify ~(datatype->boolean-binary-type host-dtype)
       (op [item# ~'x ~'y]
         ~body)
       dtype-proto/PDatatype
       (get-datatype [item#] ~datatype)
       IFn
       (invoke [item# x# y#]
         (.op item#
              (casting/datatype->cast-fn :unknown ~datatype x#)
              (casting/datatype->cast-fn :unknown ~datatype y#))))))


(defmacro implement-binary-typecast
  [datatype]
  (let [expected-type (resolve (datatype->boolean-binary-type datatype))]
    `(if (instance? ~expected-type ~'item)
       ~'item
       (throw (ex-info (format "Expected %s, got %s" ~expected-type (type ~'item)) {})))))


(defn int8->boolean-binary ^BooleanOp$ByteBinary [item] (implement-binary-typecast :int8))
(defn int16->boolean-binary ^BooleanOp$ShortBinary [item] (implement-binary-typecast :int16))
(defn int32->boolean-binary ^BooleanOp$IntBinary [item] (implement-binary-typecast :int32))
(defn int64->boolean-binary ^BooleanOp$LongBinary [item] (implement-binary-typecast :int64))
(defn float32->boolean-binary ^BooleanOp$FloatBinary [item] (implement-binary-typecast :float32))
(defn float64->boolean-binary ^BooleanOp$DoubleBinary [item] (implement-binary-typecast :float64))
(defn boolean->boolean-binary ^BinaryOperators$BooleanBinary [item] (implement-binary-typecast :boolean))
(defn object->boolean-binary ^BooleanOp$ObjectBinary [item] (implement-binary-typecast :object))


(defmacro datatype->boolean-binary
  [datatype item]
  (case datatype
    :int8 `(int8->boolean-binary ~item)
    :int16 `(int16->boolean-binary ~item)
    :int32 `(int32->boolean-binary ~item)
    :int64 `(int64->boolean-binary ~item)
    :float32 `(float32->boolean-binary ~item)
    :float64 `(float64->boolean-binary ~item)
    :boolean `(boolean->boolean-binary ~item)
    :object `(object->boolean-binary ~item)))



(defmacro make-boolean-unary-iterable
  [datatype]
  (let [op-dtype (casting/safe-flatten datatype)]
    `(fn [src-seq# bool-op# unchecked?#]
       (let [bool-op# (datatype->boolean-unary ~op-dtype bool-op#)]
         (reify
           dtype-proto/PDatatype
           (get-datatype [item#] :boolean)
           Iterable
           (iterator [item#]
             (let [src-iter# (typecast/datatype->iter ~datatype src-seq# unchecked?#)]
               (reify
                 dtype-proto/PDatatype
                 (get-datatype [item#] :boolean)
                 ~(typecast/datatype->iter-type :boolean)
                 (hasNext [iter#] (.hasNext src-iter#))
                 (~(typecast/datatype->iter-next-fn-name :boolean)
                  [iter#]
                  (let [retval# (.current iter#)]
                    (typecast/datatype->iter-next-fn ~op-dtype src-iter#)
                    retval#))
                 (current [iter#]
                   (.op bool-op# (.current src-iter#)))))))))))


(defmacro make-boolean-unary-iterable-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-boolean-unary-iterable ~dtype)])]
        (into {})))


(def boolean-unary-iterable-table (make-boolean-unary-iterable-table))


(defn boolean-unary-iterable
  "Create an iterable that transforms one sequence of arbitrary datatypes into boolean
  sequence given a boolean unary op."
  [bool-un-op src-data {:keys [unchecked? datatype]}]
  (let [datatype (or datatype (dtype-base/get-datatype src-data))
        create-fn (get boolean-unary-iterable-table (casting/safe-flatten datatype))]
    (create-fn src-data bool-un-op unchecked?)))


(defn unary-iterable-filter
  "Filter a sequence via a typed unary operation."
  [{:keys [datatype unchecked?] :as options} bool-unary-filter-op filter-seq]
  (let [bool-iterable (boolean-unary-iterable bool-unary-filter-op filter-seq options)]
    (iterator/iterable-mask options bool-iterable filter-seq)))


(defn unary-argfilter
  "Returns a (potentially infinite) sequence of indexes that pass the filter."
  [{:keys [unchecked? datatype] :as options} bool-unary-filter-op filter-seq]
  (let [bool-iterable (boolean-unary-iterable bool-unary-filter-op filter-seq options)]
    (iterator/iterable-mask (assoc options :datatype :int32) bool-iterable (range))))


(defmacro make-boolean-binary-iterable
  [datatype]
  (let [op-dtype (casting/safe-flatten datatype)]
    `(fn [lhs-seq# rhs-seq# bool-op# unchecked?#]
       (let [bool-op# (datatype->boolean-binary ~op-dtype bool-op#)]
         (reify
           dtype-proto/PDatatype
           (get-datatype [item#] :boolean)
           Iterable
           (iterator [item#]
             (let [lhs-iter# (typecast/datatype->iter ~datatype lhs-seq# unchecked?#)
                   rhs-iter# (typecast/datatype->iter ~datatype rhs-seq# unchecked?#)]
               (reify
                 dtype-proto/PDatatype
                 (get-datatype [item#] :boolean)
                 ~(typecast/datatype->iter-type :boolean)
                 (hasNext [iter#] (and (.hasNext lhs-iter#)
                                       (.hasNext rhs-iter#)))
                 (~(typecast/datatype->iter-next-fn-name :boolean)
                  [iter#]
                  (let [retval# (.current iter#)]
                    (typecast/datatype->iter-next-fn ~op-dtype lhs-iter#)
                    (typecast/datatype->iter-next-fn ~op-dtype rhs-iter#)
                    retval#))
                 (current [iter#]
                   (.op bool-op#
                        (.current lhs-iter#)
                        (.current rhs-iter#)))))))))))


(defmacro make-boolean-binary-iterable-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-boolean-binary-iterable ~dtype)])]
        (into {})))


(def boolean-binary-iterable-table (make-boolean-binary-iterable-table))


(defn boolean-binary-iterable
  "Create an iterable that transforms one sequence of arbitrary datatypes into boolean
  sequence given a boolean binary op."
  [bool-binary-op lhs-data rhs-data {:keys [unchecked? datatype]}]
  (let [datatype (or datatype (dtype-base/get-datatype lhs-data))
        create-fn (get boolean-binary-iterable-table (casting/safe-flatten datatype))]
    (create-fn lhs-data rhs-data bool-binary-op unchecked?)))


(defn binary-argfilter
  "Returns a (potentially infinite) sequence of indexes that pass the filter."
  [{:keys [unchecked? datatype] :as options} bool-binary-filter-op lhs-seq rhs-seq]
  (let [bool-iterable (boolean-binary-iterable bool-binary-filter-op lhs-seq rhs-seq options)]
    (iterator/iterable-mask (assoc options :datatype :int32) bool-iterable (range))))


(defmacro make-boolean-unary-reader
  [datatype]
  (let [op-dtype (casting/safe-flatten datatype)]
    `(fn [src-seq# bool-op# unchecked?#]
       (let [bool-op# (datatype->boolean-unary ~op-dtype bool-op#)
             src-reader# (typecast/datatype->reader ~datatype src-seq# unchecked?#)]
         (reify
           ~(typecast/datatype->reader-type :boolean)
           (getDatatype [reader#] :boolean)
           (size [reader#] (.size src-reader#))
           (read [reader# idx#]
             (->> (.read src-reader# idx#)
                  (.op bool-op#)))
           (iterator [reader#] (typecast/reader->iterator reader#))
           (invoke [reader# arg#]
             (.read reader# (int arg#))))))))


(defmacro make-boolean-unary-reader-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-boolean-unary-reader ~dtype)])]
        (into {})))


(def boolean-unary-reader-table (make-boolean-unary-reader-table))


(defn boolean-unary-reader
  "Create an reader that transforms one sequence of arbitrary datatypes into boolean
  reader given a boolean unary op."
  [bool-un-op src-data {:keys [unchecked? datatype]}]
  (let [datatype (or datatype (dtype-base/get-datatype src-data))
        create-fn (get boolean-unary-reader-table (casting/safe-flatten datatype))]
    (create-fn src-data bool-un-op unchecked?)))



(defmacro make-boolean-binary-reader
  [datatype]
  (let [op-dtype (casting/safe-flatten datatype)]
    `(fn [lhs-seq# rhs-seq# bool-op# unchecked?#]
       (let [bool-op# (datatype->boolean-binary ~op-dtype bool-op#)
             lhs-reader# (typecast/datatype->reader ~datatype lhs-seq# unchecked?#)
             rhs-reader# (typecast/datatype->reader ~datatype rhs-seq# unchecked?#)
             n-elems# (min (.size lhs-reader# rhs-reader#))]
         (reify
           ~(typecast/datatype->reader-type :boolean)
           (getDatatype [reader#] :boolean)
           (size [reader#] n-elems#)
           (read [reader# idx#]
             (when (>= idx# n-elems#)
               (throw (ex-info (format "Index out of range: %s >= %s" idx# n-elems#) {})))
             (.op bool-op#
                  (.read lhs-reader# idx#)
                  (.read rhs-reader# idx#)))
           (iterator [reader#] (typecast/reader->iterator reader#))
           (invoke [reader# arg#]
             (.read reader# (int arg#))))))))


(defmacro make-boolean-binary-reader-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-boolean-binary-reader ~dtype)])]
        (into {})))


(def boolean-binary-reader-table (make-boolean-binary-reader-table))


(defn boolean-binary-reader
  "Create an reader that transforms one sequence of arbitrary datatypes into boolean
  reader given a boolean binary op."
  [bool-binary-op lhs-data rhs-data {:keys [unchecked? datatype]}]
  (let [datatype (or datatype (dtype-base/get-datatype lhs-data))
        create-fn (get boolean-binary-reader-table (casting/safe-flatten datatype))]
    (create-fn lhs-data rhs-data bool-binary-op unchecked?)))
