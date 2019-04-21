(ns tech.v2.datatype.boolean-op
  (:require [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.iterator :as iterator]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.reader :as reader]
            [tech.v2.datatype.unary-op :as dtype-unary]
            [tech.v2.datatype.binary-op :as dtype-binary]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.argtypes :as argtypes])
  (:import [tech.v2.datatype
            BooleanOp$ByteBinary
            BooleanOp$ShortBinary
            BooleanOp$IntBinary
            BooleanOp$LongBinary
            BooleanOp$FloatBinary
            BooleanOp$DoubleBinary
            BinaryOperators$BooleanBinary
            BooleanOp$ObjectBinary
            BooleanOp$ByteUnary
            BooleanOp$ShortUnary
            BooleanOp$IntUnary
            BooleanOp$LongUnary
            BooleanOp$FloatUnary
            BooleanOp$DoubleUnary
            UnaryOperators$BooleanUnary
            BooleanOp$ObjectUnary]
           [clojure.lang IFn]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn datatype->boolean-unary-type
  [datatype]
  (case datatype
    :int8 'tech.v2.datatype.BooleanOp$ByteUnary
    :int16 'tech.v2.datatype.BooleanOp$ShortUnary
    :int32 'tech.v2.datatype.BooleanOp$IntUnary
    :int64 'tech.v2.datatype.BooleanOp$LongUnary
    :float32 'tech.v2.datatype.BooleanOp$FloatUnary
    :float64 'tech.v2.datatype.BooleanOp$DoubleUnary
    :boolean 'tech.v2.datatype.UnaryOperators$BooleanUnary
    :object 'tech.v2.datatype.BooleanOp$ObjectUnary))


(defmacro make-boolean-unary-op
  "Make a boolean unary operator.  Input is named 'arg and output will be expected to be
  boolean."
  [datatype body]
  (let [host-dtype (casting/safe-flatten datatype)]
    `(reify
       ~(datatype->boolean-unary-type host-dtype)
       (op [item# ~'x]
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
       (if (satisfies? dtype-proto/PToUnaryBooleanOp ~'item)
         (dtype-proto/->unary-boolean-op ~'item ~datatype ~'unchecked?)
         (-> (dtype-proto/->unary-op ~'item ~datatype ~'unchecked?)
             (dtype-proto/->unary-boolean-op ~datatype ~'unchecked?))))))


(defn int8->boolean-unary
  ^BooleanOp$ByteUnary [item unchecked?] (implement-unary-typecast :int8))
(defn int16->boolean-unary
  ^BooleanOp$ShortUnary [item unchecked?] (implement-unary-typecast :int16))
(defn int32->boolean-unary
  ^BooleanOp$IntUnary [item unchecked?] (implement-unary-typecast :int32))
(defn int64->boolean-unary
  ^BooleanOp$LongUnary [item unchecked?] (implement-unary-typecast :int64))
(defn float32->boolean-unary
  ^BooleanOp$FloatUnary [item unchecked?] (implement-unary-typecast :float32))
(defn float64->boolean-unary
  ^BooleanOp$DoubleUnary [item unchecked?] (implement-unary-typecast :float64))
(defn boolean->boolean-unary
  ^UnaryOperators$BooleanUnary [item unchecked?] (implement-unary-typecast :boolean))
(defn object->boolean-unary
  ^BooleanOp$ObjectUnary [item unchecked?] (implement-unary-typecast :object))


(defmacro datatype->boolean-unary
  [datatype item unchecked?]
  (case datatype
    :int8 `(int8->boolean-unary ~item ~unchecked?)
    :int16 `(int16->boolean-unary ~item ~unchecked?)
    :int32 `(int32->boolean-unary ~item ~unchecked?)
    :int64 `(int64->boolean-unary ~item ~unchecked?)
    :float32 `(float32->boolean-unary ~item ~unchecked?)
    :float64 `(float64->boolean-unary ~item ~unchecked?)
    :boolean `(boolean->boolean-unary ~item ~unchecked?)
    :object `(object->boolean-unary ~item ~unchecked?)))


(defn datatype->boolean-binary-type
  [datatype]
  (case datatype
    :int8 'tech.v2.datatype.BooleanOp$ByteBinary
    :int16 'tech.v2.datatype.BooleanOp$ShortBinary
    :int32 'tech.v2.datatype.BooleanOp$IntBinary
    :int64 'tech.v2.datatype.BooleanOp$LongBinary
    :float32 'tech.v2.datatype.BooleanOp$FloatBinary
    :float64 'tech.v2.datatype.BooleanOp$DoubleBinary
    :boolean 'tech.v2.datatype.BinaryOperators$BooleanBinary
    :object 'tech.v2.datatype.BooleanOp$ObjectBinary))


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
       (if (satisfies? dtype-proto/PToBinaryBooleanOp ~'item)
         (dtype-proto/->binary-boolean-op ~'item ~datatype ~'unchecked?)
         (-> (dtype-proto/->binary-op ~'item ~datatype ~'unchecked?)
             (dtype-proto/->binary-boolean-op ~datatype ~'unchecked?))))))


(defn int8->boolean-binary
  ^BooleanOp$ByteBinary [item unchecked?] (implement-binary-typecast :int8))
(defn int16->boolean-binary
  ^BooleanOp$ShortBinary [item unchecked?] (implement-binary-typecast :int16))
(defn int32->boolean-binary
  ^BooleanOp$IntBinary [item unchecked?] (implement-binary-typecast :int32))
(defn int64->boolean-binary
  ^BooleanOp$LongBinary [item unchecked?] (implement-binary-typecast :int64))
(defn float32->boolean-binary
  ^BooleanOp$FloatBinary [item unchecked?] (implement-binary-typecast :float32))
(defn float64->boolean-binary
  ^BooleanOp$DoubleBinary [item unchecked?] (implement-binary-typecast :float64))
(defn boolean->boolean-binary
  ^BinaryOperators$BooleanBinary [item unchecked?] (implement-binary-typecast :boolean))
(defn object->boolean-binary
  ^BooleanOp$ObjectBinary [item unchecked?] (implement-binary-typecast :object))


(defmacro datatype->boolean-binary
  [datatype item unchecked?]
  (case datatype
    :int8 `(int8->boolean-binary ~item ~unchecked?)
    :int16 `(int16->boolean-binary ~item ~unchecked?)
    :int32 `(int32->boolean-binary ~item ~unchecked?)
    :int64 `(int64->boolean-binary ~item ~unchecked?)
    :float32 `(float32->boolean-binary ~item ~unchecked?)
    :float64 `(float64->boolean-binary ~item ~unchecked?)
    :boolean `(boolean->boolean-binary ~item ~unchecked?)
    :object `(object->boolean-binary ~item ~unchecked?)))



(defmacro make-marshalling-boolean-unary
  [src-dtype dst-dtype]
  `(fn [item# datatype# unchecked?#]
     (let [src-op# (datatype->boolean-unary ~src-dtype item# unchecked?#)]
       (reify ~(datatype->boolean-unary-type dst-dtype)
         (op [item# arg#]
           (.op src-op# (casting/datatype->cast-fn ~dst-dtype ~src-dtype arg#)))
         dtype-proto/PDatatype
         (get-datatype [item#] datatype#)
         IFn
         (invoke [item# arg#]
           (.op item# (casting/datatype->cast-fn :unknown ~dst-dtype arg#)))))))

(defmacro make-marshalling-boolean-unary-table
  []
  `(->> [~@(for [src-dtype casting/base-host-datatypes
                 dst-dtype casting/base-host-datatypes]
             [[src-dtype dst-dtype]
              `(make-marshalling-boolean-unary ~src-dtype ~dst-dtype)])]
        (into {})))


(def marshalling-boolean-unary-table (make-marshalling-boolean-unary-table))


(defmacro make-marshalling-boolean-binary
  [src-dtype dst-dtype]
  `(fn [item# datatype# unchecked?#]
     (let [src-op# (datatype->boolean-binary ~src-dtype item# unchecked?#)]
       (reify ~(datatype->boolean-binary-type dst-dtype)
         (op [item# x# y#]
           (.op src-op#
                (casting/datatype->cast-fn ~dst-dtype ~src-dtype x#)
                (casting/datatype->cast-fn ~dst-dtype ~src-dtype y#)))
         dtype-proto/PDatatype
         (get-datatype [item#] datatype#)
         IFn
         (invoke [item# x# y#]
           (.op item#
                (casting/datatype->cast-fn :unknown ~dst-dtype x#)
                (casting/datatype->cast-fn :unknown ~dst-dtype y#)))))))


(defmacro make-marshalling-boolean-binary-table
  []
  `(->> [~@(for [src-dtype casting/base-host-datatypes
                 dst-dtype casting/base-host-datatypes]
             [[src-dtype dst-dtype]
              `(make-marshalling-boolean-binary ~src-dtype ~dst-dtype)])]
        (into {})))


(def marshalling-boolean-binary-table (make-marshalling-boolean-binary-table))


(defmacro extend-unary-op-types
  [datatype]
  `(do
     (clojure.core/extend
         ~(datatype->boolean-unary-type datatype)
       dtype-proto/PToUnaryBooleanOp
       {:->unary-boolean-op
        (fn [item# dtype# unchecked?#]
          (if (= dtype# (dtype-base/get-datatype item#))
            item#
            (let [cast-fn# (get marshalling-boolean-unary-table
                                [~datatype (casting/safe-flatten dtype#)])]
              (cast-fn# item# dtype# unchecked?#))))}
       dtype-proto/PToUnaryOp
       {:->unary-op
        (fn [item# dtype# unchecked?#]
          (let [bool-item# (datatype->boolean-unary ~datatype item# unchecked?#)]
            (-> (reify ~(dtype-unary/datatype->unary-op-type datatype)
                  (getDatatype [bool-item#] (dtype-base/get-datatype item#))
                  (op [unary-item# arg#]
                    (let [retval# (.op bool-item# arg#)]
                      (casting/datatype->cast-fn :boolean ~datatype retval#)))
                  (invoke [unary-item# arg#]
                    (.op unary-item# (casting/datatype->cast-fn
                                      :unknown ~datatype arg#))))
                (dtype-proto/->unary-op dtype# unchecked?#))))})

     (clojure.core/extend
         ~(dtype-unary/datatype->unary-op-type datatype)
       dtype-proto/PToUnaryBooleanOp
       {:->unary-boolean-op
        (fn [item# datatype# unchecked?#]
          (let [item# (dtype-unary/datatype->unary-op ~datatype item# unchecked?#)]
            (-> (reify
                  ~(datatype->boolean-unary-type datatype)

                  (op [bool-item# arg#]
                    (let [retval# (.op item# arg#)]
                      (casting/datatype->cast-fn ~datatype :boolean retval#)))
                  dtype-proto/PDatatype
                  (get-datatype [item#] (dtype-base/get-datatype item#))
                  IFn
                  (invoke [bool-item# arg#]
                    (.op bool-item# (casting/datatype->cast-fn
                                     :unknown ~datatype arg#))))
                (dtype-proto/->unary-boolean-op datatype# unchecked?#))))})))


(extend-unary-op-types :int8)
(extend-unary-op-types :int16)
(extend-unary-op-types :int32)
(extend-unary-op-types :int64)
(extend-unary-op-types :float32)
(extend-unary-op-types :float64)
(extend-unary-op-types :boolean)
(extend-unary-op-types :object)


(defmacro extend-binary-op-types
  [datatype]
  `(do
     (clojure.core/extend
         ~(datatype->boolean-binary-type datatype)
       dtype-proto/PToBinaryBooleanOp
       {:->binary-boolean-op
        (fn [item# dtype# unchecked?#]
          (if (= dtype# (dtype-base/get-datatype item#))
            item#
            (let [cast-fn# (get marshalling-boolean-binary-table
                                [~datatype (casting/safe-flatten dtype#)])]
              (cast-fn# item# dtype# unchecked?#))))}
       dtype-proto/PToBinaryOp
       {:->binary-op
        (fn [item# dtype# unchecked?#]
          (let [bool-item# (datatype->boolean-binary ~datatype item# unchecked?#)]
            (-> (reify ~(dtype-binary/datatype->binary-op-type datatype)
                  (getDatatype [bool-item#] (dtype-base/get-datatype item#))
                  (op [binary-item# x# y#]
                    (let [retval# (.op bool-item# x# y#)]
                      (casting/datatype->cast-fn :boolean ~datatype retval#)))
                  (invoke [binary-item# x# y#]
                    (.op binary-item#
                         (casting/datatype->cast-fn
                          :unknown ~datatype x#)
                         (casting/datatype->cast-fn
                          :unknown ~datatype y#))))
                (dtype-proto/->binary-op dtype# unchecked?#))))})

     (clojure.core/extend
         ~(dtype-binary/datatype->binary-op-type datatype)
       dtype-proto/PToBinaryBooleanOp
       {:->binary-boolean-op
        (fn [item# datatype# unchecked?#]
          (let [item# (dtype-binary/datatype->binary-op ~datatype item# unchecked?#)]
            (-> (reify
                  ~(datatype->boolean-binary-type datatype)

                  (op [bool-item# x# y#]
                    (let [retval# (.op item# x# y#)]
                      (casting/datatype->cast-fn ~datatype :boolean retval#)))
                  dtype-proto/PDatatype
                  (get-datatype [bool-item#] (dtype-base/get-datatype item#))
                  IFn
                  (invoke [bool-item# x# y#]
                    (.op bool-item#
                         (casting/datatype->cast-fn
                          :unknown ~datatype x#)
                         (casting/datatype->cast-fn
                          :unknown ~datatype y#))))
                (dtype-proto/->binary-boolean-op datatype# unchecked?#))))})))


(extend-type BinaryOperators$BooleanBinary
  dtype-proto/PToBinaryBooleanOp
  (->binary-boolean-op [item dtype unchecked?]
    (if (= :boolean dtype)
      item
      (let [cast-fn (get marshalling-boolean-binary-table
                         [:boolean (casting/safe-flatten dtype)])]
        (cast-fn item dtype unchecked?)))))


(extend-binary-op-types :int8)
(extend-binary-op-types :int16)
(extend-binary-op-types :int32)
(extend-binary-op-types :int64)
(extend-binary-op-types :float32)
(extend-binary-op-types :float64)
(extend-binary-op-types :object)


(defmacro make-boolean-unary-iterable
  [datatype]
  (let [op-dtype (casting/safe-flatten datatype)]
    `(fn [src-seq# bool-op# unchecked?#]
       (let [bool-op# (datatype->boolean-unary ~op-dtype bool-op# unchecked?#)]
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


(defn boolean-unary-iterable-map
  "Create an iterable that transforms one sequence of arbitrary datatypes into boolean
  sequence given a boolean unary op."
  [{:keys [unchecked? datatype]} bool-un-op src-data]
  (let [datatype (or datatype (dtype-base/get-datatype src-data))
        create-fn (get boolean-unary-iterable-table (casting/safe-flatten datatype))]
    (create-fn src-data bool-un-op unchecked?)))


(defmacro boolean-unary-iterable
  ([datatype opcode item]
   `(boolean-unary-iterable-map
     {:datatype ~datatype}
     (make-boolean-unary-op ~datatype ~opcode)
     ~item))
  ([opcode item]
   `(boolean-unary-iterable :object ~opcode ~item)))


(defn unary-iterable-filter
  "Filter a sequence via a typed unary operation."
  [{:keys [datatype unchecked?] :as options} bool-unary-filter-op filter-seq]
  (let [bool-iterable (boolean-unary-iterable-map
                       options bool-unary-filter-op
                       filter-seq)]
    (iterator/iterable-mask options bool-iterable filter-seq)))


(defn unary-argfilter
  "Returns a (potentially infinite) sequence of indexes that pass the filter."
  [{:keys [unchecked? datatype] :as options} bool-unary-filter-op filter-seq]
  (let [bool-iterable (boolean-unary-iterable-map options bool-unary-filter-op filter-seq)]
    (iterator/iterable-mask (assoc options :datatype :int32) bool-iterable (range))))


(defn argfind
  [{:keys [unchecked? datatype] :as options} bool-unary-filter-op filter-seq]
  (first (unary-argfilter options bool-unary-filter-op filter-seq)))


(defmacro make-boolean-binary-iterable
  [datatype]
  (let [op-dtype (casting/safe-flatten datatype)]
    `(fn [lhs-seq# rhs-seq# bool-op# unchecked?#]
       (let [bool-op# (datatype->boolean-binary ~op-dtype bool-op# unchecked?#)]
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


(defn boolean-binary-iterable-map
  "Create an iterable that transforms one sequence of arbitrary datatypes into boolean
  sequence given a boolean binary op."
  [{:keys [unchecked? datatype]} bool-binary-op lhs-data rhs-data]
  (let [datatype (or datatype (dtype-base/get-datatype lhs-data))
        create-fn (get boolean-binary-iterable-table (casting/safe-flatten datatype))]
    (create-fn lhs-data rhs-data bool-binary-op unchecked?)))


(defmacro boolean-binary-iterable
  ([datatype opcode lhs rhs]
   `(boolean-binary-iterablemap
     {:datatype ~datatype}
     (make-boolean-binary-op ~datatype ~opcode)
     ~lhs ~rhs))
  ([opcode lhs rhs]
   `(boolean-binary-iterable :object ~opcode lhs rhs)))


(defn binary-argfilter
  "Returns a (potentially infinite) sequence of indexes that pass the filter."
  [{:keys [unchecked? datatype] :as options} bool-binary-filter-op lhs-seq rhs-seq]
  (let [bool-iterable (boolean-binary-iterable-map options
                                               bool-binary-filter-op
                                               lhs-seq rhs-seq)]
    (iterator/iterable-mask (assoc options :datatype :int32) bool-iterable (range))))


(declare boolean-unary-reader)


(defmacro make-boolean-unary-reader
  [datatype]
  (let [op-dtype (casting/safe-flatten datatype)]
    `(fn [src-seq# bool-op# unchecked?#]
       (let [bool-op# (datatype->boolean-unary ~op-dtype bool-op# unchecked?#)
             src-reader# (typecast/datatype->reader ~datatype src-seq# unchecked?#)
             create-fn# #(boolean-unary-reader {:datatype (dtype-base/get-datatype
                                                           src-seq#)
                                                :unchecked? %3}
                                               bool-op#
                                               %1)]
         (reader/make-derived-reader :boolean :boolean unchecked?#
                                     src-reader#
                                     (.op bool-op# (.read src-reader# ~'idx))
                                     create-fn#)))))


(defmacro make-boolean-unary-reader-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-boolean-unary-reader ~dtype)])]
        (into {})))


(def boolean-unary-reader-table (make-boolean-unary-reader-table))


(defmulti boolean-unary-reader-map
    "Create an reader that transforms one sequence of arbitrary datatypes into boolean
  reader given a boolean unary op."
  (fn [options bool-unary-op src-reader]
    (dtype-base/buffer-type src-reader)))


(defmacro boolean-unary-reader
  ([datatype opcode item]
   `(boolean-unary-reader-map
     {:datatype ~datatype}
     (make-boolean-unary-op ~datatype ~opcode)
     ~item))
  ([opcode item]
   `(boolean-unary-reader :object ~opcode ~item)))


(defmethod boolean-unary-reader-map :default
  [{:keys [unchecked? datatype]} bool-un-op src-data]
  (let [datatype (or datatype (dtype-base/get-datatype src-data))
        create-fn (get boolean-unary-reader-table (casting/safe-flatten datatype))]
    (create-fn src-data bool-un-op unchecked?)))



(defmacro make-boolean-binary-reader
  [datatype]
  (let [op-dtype (casting/safe-flatten datatype)]
    `(fn [lhs-seq# rhs-seq# bool-op# unchecked?#]
       (let [bool-op# (datatype->boolean-binary ~op-dtype bool-op# unchecked?#)
             lhs-reader# (typecast/datatype->reader ~datatype lhs-seq# unchecked?#)
             rhs-reader# (typecast/datatype->reader ~datatype rhs-seq# unchecked?#)
             n-elems# (min (.lsize lhs-reader#)
                           (.lsize rhs-reader#))]
         (reify
           ~(typecast/datatype->reader-type :boolean)
           (getDatatype [reader#] :boolean)
           (lsize [reader#] n-elems#)
           (read [reader# idx#]
             (when (>= idx# n-elems#)
               (throw (ex-info (format "Index out of range: %s >= %s"
                                       idx# n-elems#) {})))
             (.op bool-op#
                  (.read lhs-reader# idx#)
                  (.read rhs-reader# idx#))))))))


(defmacro make-boolean-binary-reader-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-boolean-binary-reader ~dtype)])]
        (into {})))


(def boolean-binary-reader-table (make-boolean-binary-reader-table))



(defmulti boolean-binary-reader-map
    "Create an reader that transforms one sequence of arbitrary datatypes into boolean
  reader given a boolean binary op."
  (fn [options bool-binary-op lhs rhs]
    [(dtype-base/buffer-type lhs)
     (dtype-base/buffer-type rhs)]))


(defmethod boolean-binary-reader-map :default
  [{:keys [unchecked? datatype]} bool-binary-op lhs-data rhs-data]
  (let [datatype (or datatype (dtype-base/get-datatype lhs-data))
        create-fn (get boolean-binary-reader-table (casting/safe-flatten datatype))]
    (create-fn lhs-data rhs-data bool-binary-op unchecked?)))


(defmacro boolean-binary-reader
  ([datatype opcode lhs rhs]
   `(boolean-binary-reader-map
     {:datatype ~datatype}
     (make-boolean-binary-op ~datatype ~opcode)
     ~lhs ~rhs))
  ([opcode lhs rhs]
   `(boolean-binary-reader :object ~opcode ~lhs ~rhs)))


(defmacro make-numeric-binary-boolean-op
  [opcode]
  `(reify
     dtype-proto/PToBinaryBooleanOp
     (->binary-boolean-op [item# datatype# unchecked?#]
       (let [host-dtype# (if (casting/numeric-type? datatype#)
                           (casting/safe-flatten datatype#)
                           :float64)]
         (-> (case host-dtype#
               :int8 (make-boolean-binary-op :int8 ~opcode)
               :int16 (make-boolean-binary-op :int16 ~opcode)
               :int32 (make-boolean-binary-op :int32 ~opcode)
               :int64 (make-boolean-binary-op :int64 ~opcode)
               :float32 (make-boolean-binary-op :float32 ~opcode)
               :float64 (make-boolean-binary-op :float64 ~opcode)
               :object (make-boolean-binary-op :object ~opcode))
             (dtype-proto/->binary-boolean-op datatype# unchecked?#))))))


(defmacro make-all-datatype-binary-boolean-op
  [opcode]
  `(reify
     dtype-proto/PToBinaryBooleanOp
     (->binary-boolean-op [item# datatype# unchecked?#]
       (let [host-dtype# (casting/safe-flatten datatype#)]
         (case host-dtype#
           :int8 (make-boolean-binary-op :int8 ~opcode)
           :int16 (make-boolean-binary-op :int16 ~opcode)
           :int32 (make-boolean-binary-op :int32 ~opcode)
           :int64 (make-boolean-binary-op :int64 ~opcode)
           :float32 (make-boolean-binary-op :float32 ~opcode)
           :float64 (make-boolean-binary-op :float64 ~opcode)
           :boolean (make-boolean-binary-op :boolean ~opcode)
           :object (make-boolean-binary-op :object ~opcode))))))


(def builtin-boolean-unary-ops
  {:not (make-boolean-unary-op :boolean (not x))})


(set! *unchecked-math* false)


(def builtin-boolean-binary-ops
  {:and (make-boolean-binary-op :boolean (boolean (and x y)))
   :or (make-boolean-binary-op :boolean (boolean (or x y)))
   :eq (make-all-datatype-binary-boolean-op (= x y))
   :not-eq (make-all-datatype-binary-boolean-op (not= x y))
   :> (make-numeric-binary-boolean-op (> x y))
   :>= (make-numeric-binary-boolean-op (>= x y))
   :< (make-numeric-binary-boolean-op (< x y))
   :<= (make-numeric-binary-boolean-op (<= x y))})
