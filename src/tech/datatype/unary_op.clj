(ns tech.datatype.unary-op
  (:require [tech.datatype.typecast :as typecast]
            [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.iterator :as iterator])
  (:import [tech.datatype DatatypeIterable
            ByteIter ShortIter IntIter LongIter
            FloatIter DoubleIter BooleanIter ObjectIter
            ByteReader ShortReader IntReader LongReader
            FloatReader DoubleReader BooleanReader ObjectReader
            UnaryOperators$ByteUnary  UnaryOperators$ShortUnary
            UnaryOperators$IntUnary  UnaryOperators$LongUnary
            UnaryOperators$FloatUnary  UnaryOperators$DoubleUnary
            UnaryOperators$BooleanUnary  UnaryOperators$ObjectUnary]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn datatype->unary-op-type
  [datatype]
  (let [datatype (casting/datatype->safe-host-type datatype)]
    (case datatype
      :int8 'UnaryOperators$ByteUnary
      :int16 'UnaryOperators$ShortUnary
      :int32 'UnaryOperators$IntUnary
      :int64 'UnaryOperators$LongUnary
      :float32 'UnaryOperators$FloatUnary
      :float64 'UnaryOperators$DoubleUnary
      :boolean 'UnaryOperators$BooleanUnary
      :object 'UnaryOperators$ObjectUnary)))


(defmacro extend-unary-op
  [datatype]
  `(clojure.core/extend
       ~(datatype->unary-op-type datatype)
     dtype-proto/PToUnaryOp
     {:->unary-op
      (fn [item# datatype# unchecked?#]
        (when-not (= (dtype-proto/get-datatype item#)
                     datatype#)
          (throw (ex-info (format "Cannot convert unary operator %s->%s"
                                  ~datatype datatype#)
                          {})))
        item#)}))

(extend-unary-op :int8)
(extend-unary-op :int16)
(extend-unary-op :int32)
(extend-unary-op :int64)
(extend-unary-op :float32)
(extend-unary-op :float64)
(extend-unary-op :boolean)
(extend-unary-op :object)


(defmacro make-unary-op
  [datatype & body]
  `(reify ~(datatype->unary-op-type datatype)
     (getDatatype [item#] ~datatype)
     (op [item# ~'arg]
       ~@body)
     (invoke [item# arg#]
       (let [~'arg (casting/datatype->cast-fn :unknown ~datatype arg#)]
         ~@body))))


(defmacro impl-unary-op-cast
  [datatype item]
  `(if (instance? ~(resolve (datatype->unary-op-type datatype)) ~item)
     ~item
     (dtype-proto/->unary-op ~item ~datatype ~'unchecked?)))


(defn int8->unary-op ^UnaryOperators$ByteUnary [item unchecked?]
  (impl-unary-op-cast :int8 item))
(defn int16->unary-op ^UnaryOperators$ShortUnary [item unchecked?]
  (impl-unary-op-cast :int16 item))
(defn int32->unary-op ^UnaryOperators$IntUnary [item unchecked?]
  (impl-unary-op-cast :int32 item))
(defn int64->unary-op ^UnaryOperators$LongUnary [item unchecked?]
  (impl-unary-op-cast :int64 item))
(defn float32->unary-op ^UnaryOperators$FloatUnary [item unchecked?]
  (impl-unary-op-cast :float32 item))
(defn float64->unary-op ^UnaryOperators$DoubleUnary [item unchecked?]
  (impl-unary-op-cast :float64 item))
(defn boolean->unary-op ^UnaryOperators$BooleanUnary [item unchecked?]
  (impl-unary-op-cast :boolean item))
(defn object->unary-op ^UnaryOperators$ObjectUnary [item unchecked?]
  (impl-unary-op-cast :object item))


(defmacro datatype->unary-op
  [datatype item unchecked?]
  (let [host-dtype (casting/datatype->safe-host-type datatype)]
    (case host-dtype
      :int8 `(int8->unary-op ~item ~unchecked?)
      :int16 `(int16->unary-op ~item ~unchecked?)
      :int32 `(int32->unary-op ~item ~unchecked?)
      :int64 `(int64->unary-op ~item ~unchecked?)
      :float32 `(float32->unary-op ~item ~unchecked?)
      :float64 `(float64->unary-op ~item ~unchecked?)
      :boolean `(boolean->unary-op ~item ~unchecked?)
      :object `(object->unary-op ~item ~unchecked?))))


(defmacro make-unary-op-iterator
  [dtype item unary-op]
  `(let [src-iter# (typecast/datatype->iter ~dtype ~item)
         un-op# (datatype->unary-op ~dtype ~unary-op true)]
     (reify ~(typecast/datatype->iter-type dtype)
       (getDatatype [item#] ~dtype)
       (hasNext [item#] (.hasNext src-iter#))
       (~(typecast/datatype->iter-next-fn-name dtype)
        [item#]
        (let [data-val# (typecast/datatype->iter-next-fn
                         ~dtype src-iter#)]
          (.op un-op# data-val#)))
       (current [item#]
         (->> (.current src-iter#)
              (.op un-op#))))))


(defmacro make-unary-op-iter-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             (let [host-dtype (casting/datatype->safe-host-type dtype)]
               [dtype
                `(fn [item# un-op#]
                   (reify
                     dtype-proto/PDatatype
                     (get-datatype [iter-item#] ~dtype)
                     Iterable
                     (iterator [iter-item#]
                       (make-unary-op-iterator ~dtype item# un-op#))))]))]
        (into {})))

(def unary-op-iter-table (make-unary-op-iter-table))


(defn unary-iterable-map
  [un-op item]
  (let [dtype (dtype-proto/get-datatype un-op)]
    (if-let [iter-fn (get unary-op-iter-table (casting/flatten-datatype dtype))]
      (iter-fn item un-op)
      (throw (ex-info (format "Cannot unary map datatype %s" dtype) {})))))


(defn iterable-remove
  [options filter-iter values]
  (iterator/iterable-mask options
                          (unary-iterable-map
                           (make-unary-op :boolean (not arg))
                           filter-iter)
                          values))


(defmacro make-unary-op-reader-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             (let [host-dtype (casting/datatype->safe-host-type dtype)]
               [dtype
                `(fn [item# un-op#]
                   (let [un-op# (datatype->unary-op ~dtype un-op# true)
                         src-reader# (typecast/datatype->reader ~dtype item#)]
                     (-> (reify ~(typecast/datatype->reader-type dtype)
                           (getDatatype [item#] ~dtype)
                           (size [item#] (.size src-reader#))
                           (read [item# idx#]
                             (->> (.read src-reader# idx#)
                                  (.op un-op#)))
                           (iterator [item#]
                             (make-unary-op-iterator ~dtype src-reader# un-op#))
                           (invoke [item# idx#]
                             (.read item# (int idx#)))))))]))]
        (into {})))

(def unary-op-reader-table (make-unary-op-reader-table))

(defn unary-reader-map
  [un-op item]
  (let [dtype (dtype-proto/get-datatype un-op)]
    (if-let [reader-fn (get unary-op-reader-table (casting/flatten-datatype dtype))]
      (reader-fn item un-op)
      (throw (ex-info (format "Cannot unary map datatype %s" dtype) {})))))
