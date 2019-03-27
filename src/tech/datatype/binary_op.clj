(ns tech.datatype.binary-op
  (:require [tech.datatype.typecast :as typecast]
            [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto])
  (:import [tech.datatype
            ByteIter ShortIter IntIter LongIter
            FloatIter DoubleIter BooleanIter ObjectIter
            ByteReader ShortReader IntReader LongReader
            FloatReader DoubleReader BooleanReader ObjectReader
            BinaryOperators$ByteBinary  BinaryOperators$ShortBinary
            BinaryOperators$IntBinary  BinaryOperators$LongBinary
            BinaryOperators$FloatBinary  BinaryOperators$DoubleBinary
            BinaryOperators$BooleanBinary  BinaryOperators$ObjectBinary]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn datatype->binary-op-type
  [datatype]
  (let [datatype (casting/datatype->safe-host-type datatype)]
    (case datatype
      :int8 'tech.datatype.BinaryOperators$ByteBinary
      :int16 'tech.datatype.BinaryOperators$ShortBinary
      :int32 'tech.datatype.BinaryOperators$IntBinary
      :int64 'tech.datatype.BinaryOperators$LongBinary
      :float32 'tech.datatype.BinaryOperators$FloatBinary
      :float64 'tech.datatype.BinaryOperators$DoubleBinary
      :boolean 'tech.datatype.BinaryOperators$BooleanBinary
      :object 'tech.datatype.BinaryOperators$ObjectBinary)))


(defmacro extend-binary-op
  [datatype]
  `(clojure.core/extend
       ~(datatype->binary-op-type datatype)
     dtype-proto/PToBinaryOp
     {:->binary-op
      (fn [item# datatype# unchecked?#]
        (when-not (= (dtype-proto/get-datatype item#)
                     datatype#)
          (throw (ex-info (format "Cannot convert unary operator %s->%s"
                                  ~datatype datatype#)
                          {})))
        item#)}))


(extend-binary-op :int8)
(extend-binary-op :int16)
(extend-binary-op :int32)
(extend-binary-op :int64)
(extend-binary-op :float32)
(extend-binary-op :float64)
(extend-binary-op :boolean)
(extend-binary-op :object)


(defmacro make-binary-op
  "Make a binary op of type datatype.  Arguments to the operation
  are exposed to the local scope as 'x' and 'y' respectively.
  (make-binary-op :float32 (Math/pow x y))"
  [datatype & body]
  `(reify ~(datatype->binary-op-type datatype)
     (getDatatype [item#] ~datatype)
     (op [item# ~'x ~'y]
       ~@body)
     (invoke [item# x# y#]
       (let [~'x (casting/datatype->cast-fn :unknown ~datatype x#)
             ~'y (casting/datatype->cast-fn :unknown ~datatype y#)]
         ~@body))))


(defmacro impl-binary-op-cast
  [datatype item]
  `(if (instance? ~(resolve (datatype->binary-op-type datatype)) ~item)
     ~item
     (dtype-proto/->binary-op ~item ~datatype ~'unchecked?)))


(defn int8->binary-op ^BinaryOperators$ByteBinary [item unchecked?]
  (impl-binary-op-cast :int8 item))
(defn int16->binary-op ^BinaryOperators$ShortBinary [item unchecked?]
  (impl-binary-op-cast :int16 item))
(defn int32->binary-op ^BinaryOperators$IntBinary [item unchecked?]
  (impl-binary-op-cast :int32 item))
(defn int64->binary-op ^BinaryOperators$LongBinary [item unchecked?]
  (impl-binary-op-cast :int64 item))
(defn float32->binary-op ^BinaryOperators$FloatBinary [item unchecked?]
  (impl-binary-op-cast :float32 item))
(defn float64->binary-op ^BinaryOperators$DoubleBinary [item unchecked?]
  (impl-binary-op-cast :float64 item))
(defn boolean->binary-op ^BinaryOperators$BooleanBinary [item unchecked?]
  (impl-binary-op-cast :boolean item))
(defn object->binary-op ^BinaryOperators$ObjectBinary [item unchecked?]
  (impl-binary-op-cast :object item))


(defmacro datatype->binary-op
  [datatype item unchecked?]
  (let [host-dtype (casting/datatype->safe-host-type datatype)]
    (case host-dtype
      :int8 `(int8->binary-op ~item ~unchecked?)
      :int16 `(int16->binary-op ~item ~unchecked?)
      :int32 `(int32->binary-op ~item ~unchecked?)
      :int64 `(int64->binary-op ~item ~unchecked?)
      :float32 `(float32->binary-op ~item ~unchecked?)
      :float64 `(float64->binary-op ~item ~unchecked?)
      :boolean `(boolean->binary-op ~item ~unchecked?)
      :object `(object->binary-op ~item ~unchecked?))))


(defmacro make-binary-op-iterator
  [dtype lhs-item rhs-item binary-op]
  `(let [lhs-iter# (typecast/datatype->iter ~dtype ~lhs-item)
         rhs-iter# (typecast/datatype->iter ~dtype ~rhs-item)
         bin-op# (datatype->binary-op ~dtype ~binary-op true)]
     (reify ~(typecast/datatype->iter-type dtype)
       (getDatatype [item#] ~dtype)
       (hasNext [item#] (and (.hasNext lhs-iter#)
                             (.hasNext rhs-iter#)))
       (~(typecast/datatype->iter-next-fn-name dtype)
        [item#]
        (.op bin-op#
             (typecast/datatype->iter-next-fn
              ~dtype lhs-iter#)
             (typecast/datatype->iter-next-fn
              ~dtype rhs-iter#)))
       (current [item#]
         (.op bin-op#
              (.current lhs-iter#)
              (.current rhs-iter#))))))

(defmacro make-binary-op-iter-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             (let [host-dtype (casting/datatype->safe-host-type dtype)]
               [dtype
                `(fn [lhs# rhs# bin-op#]
                   (reify
                     Iterable
                     (iterator [iter-item#]
                       (make-binary-op-iterator ~dtype lhs# rhs# bin-op#))
                     dtype-proto/PDatatype
                     (get-datatype [iter-item#] ~dtype)))]))]
        (into {})))

(def binary-op-iter-table (make-binary-op-iter-table))

(defn binary-iterable-map
  [bin-op lhs rhs]
  (let [dtype (dtype-proto/get-datatype bin-op)]
    (if-let [iter-fn (get binary-op-iter-table (casting/flatten-datatype dtype))]
      (iter-fn lhs rhs bin-op)
      (throw (ex-info (format "Cannot unary map datatype %s" dtype) {})))))


(defmacro make-binary-op-reader-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             (let [host-dtype (casting/datatype->safe-host-type dtype)]
               [dtype
                `(fn [lhs# rhs# bin-op#]
                   (let [bin-op# (datatype->binary-op ~dtype bin-op# true)
                         lhs-reader# (typecast/datatype->reader ~dtype lhs#)
                         rhs-reader# (typecast/datatype->reader ~dtype rhs#)
                         n-elems# (min (.size lhs-reader#)
                                       (.size rhs-reader#))]
                     (-> (reify ~(typecast/datatype->reader-type dtype)
                           (getDatatype [item#] ~dtype)
                           (size [item#] n-elems#)
                           (read [item# idx#]
                             (.op bin-op#
                                  (.read lhs-reader# idx#)
                                  (.read rhs-reader# idx#)))
                           (iterator [item#]
                             (make-binary-op-iterator
                              ~dtype lhs-reader# rhs-reader# bin-op#))
                           (invoke [item# idx#]
                             (.read item# (int idx#)))))))]))]
        (into {})))

(def binary-op-reader-table (make-binary-op-reader-table))

(defn binary-reader-map
  [bin-op lhs rhs]
  (let [dtype (dtype-proto/get-datatype bin-op)]
    (if-let [reader-fn (get binary-op-reader-table (casting/flatten-datatype dtype))]
      (reader-fn lhs rhs bin-op)
      (throw (ex-info (format "Cannot binary map datatype %s" dtype) {})))))
