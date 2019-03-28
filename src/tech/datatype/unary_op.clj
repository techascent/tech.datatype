(ns tech.datatype.unary-op
  (:require [tech.datatype.typecast :as typecast]
            [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.iterator :as iterator]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.nio-access :as nio-access])
  (:import [tech.datatype
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
      :int8 'tech.datatype.UnaryOperators$ByteUnary
      :int16 'tech.datatype.UnaryOperators$ShortUnary
      :int32 'tech.datatype.UnaryOperators$IntUnary
      :int64 'tech.datatype.UnaryOperators$LongUnary
      :float32 'tech.datatype.UnaryOperators$FloatUnary
      :float64 'tech.datatype.UnaryOperators$DoubleUnary
      :boolean 'tech.datatype.UnaryOperators$BooleanUnary
      :object 'tech.datatype.UnaryOperators$ObjectUnary)))


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


(defmacro make-marshalling-unary-op-impl
  [dst-datatype src-datatype]
  (let [host-datatype (casting/safe-flatten dst-datatype)
        src-host-datatype (casting/safe-flatten src-datatype)]
    `(fn [un-op# datatype# unchecked?#]
       (let [src-op# (datatype->unary-op ~src-host-datatype un-op# unchecked?#)]
         (if unchecked?#
           (reify ~(datatype->unary-op-type host-datatype)
             (getDatatype [item#] datatype#)
             (op [item# arg#]
               (let [value# (.op src-op# (nio-access/unchecked-full-cast arg#
                                                                         ~host-datatype
                                                                         ~dst-datatype
                                                                         ~src-datatype))]
                 (nio-access/unchecked-full-cast value#
                                                 ~src-host-datatype
                                                 ~src-datatype
                                                 ~dst-datatype)))
             (invoke [item# arg#]
               (.op item# (casting/datatype->cast-fn
                           :unknown ~dst-datatype arg#))))
           (reify ~(datatype->unary-op-type host-datatype)
             (getDatatype [item#] ~dst-datatype)
             (op [item# arg#]
               (let [value# (.op src-op# (nio-access/checked-full-write-cast
                                          arg#
                                          ~host-datatype
                                          ~dst-datatype
                                          ~src-datatype))]
                 (nio-access/checked-full-write-cast value#
                                                     ~src-host-datatype
                                                     ~src-datatype
                                                     ~dst-datatype)))
             (invoke [item# arg#]
               (.op item# (casting/datatype->cast-fn
                           :unknown ~dst-datatype arg#)))))))))


(defmacro make-marshalling-unary-table
  []
  `(->> [~@(for [src-dtype casting/base-host-datatypes
                 dst-dtype casting/base-host-datatypes]
             [[src-dtype dst-dtype] `(make-marshalling-unary-op-impl
                                      ~dst-dtype ~src-dtype)])]
        (into {})))


(def marshalling-unary-op-table (make-marshalling-unary-table))


(defmacro extend-unary-op
  [datatype]
  `(clojure.core/extend
       ~(datatype->unary-op-type datatype)
     dtype-proto/PToUnaryOp
     {:->unary-op (fn [item# un-dtype# unchecked?#]
                    (if (= (casting/safe-flatten un-dtype#)
                           ~datatype)
                      item#
                      (let [marshal-fn# (get marshalling-unary-op-table
                                             [~datatype (casting/safe-flatten
                                                         un-dtype#)])]
                        (marshal-fn# item# un-dtype# unchecked?#))))}))


(extend-unary-op :int8)
(extend-unary-op :int16)
(extend-unary-op :int32)
(extend-unary-op :int64)
(extend-unary-op :float32)
(extend-unary-op :float64)
(extend-unary-op :boolean)
(extend-unary-op :object)



(defmacro make-unary-op
  "Make a unary operation with a given datatype.  The argument is
  placed into the local namespace as 'arg'.
  (make-unary-op :int32 (+ arg 10))"
  [datatype body]
  `(reify ~(datatype->unary-op-type datatype)
     (getDatatype [item#] ~datatype)
     (op [item# ~'arg]
       ~body)
     (invoke [item# arg#]
       (.op item# (casting/datatype->cast-fn :unknown ~datatype arg#)))))


(defmacro make-unary-op-iterator
  [dtype item unary-op unchecked?]
  `(let [src-iter# (typecast/datatype->iter ~dtype ~item ~unchecked?)
         un-op# (datatype->unary-op ~dtype ~unary-op ~unchecked?)]
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
                `(fn [item# un-op# unchecked?#]
                   (reify
                     dtype-proto/PDatatype
                     (get-datatype [iter-item#] ~dtype)
                     Iterable
                     (iterator [iter-item#]
                       (make-unary-op-iterator ~dtype item# un-op# unchecked?#))))]))]
        (into {})))

(def unary-op-iter-table (make-unary-op-iter-table))


(defn unary-iterable-map
  [{:keys [datatype unchecked?]} un-op item]
  (let [datatype (or datatype (dtype-base/get-datatype item))]
    (if (= un-op :no-op)
      (dtype-proto/->iterable-of-type item datatype unchecked?)
      (if-let [iter-fn (get unary-op-iter-table (casting/flatten-datatype datatype))]
        (iter-fn item un-op unchecked?)
        (throw (ex-info (format "Cannot unary map datatype %s" datatype) {}))))))


(defn iterable-remove
  [options filter-iter values]
  (iterator/iterable-mask options
                          (unary-iterable-map options
                           (make-unary-op :boolean (not arg))
                           filter-iter)
                          values))


(defmacro make-unary-op-reader-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             (let [host-dtype (casting/datatype->safe-host-type dtype)]
               [dtype
                `(fn [item# un-op# unchecked?#]
                   (let [un-op# (datatype->unary-op ~dtype un-op# true)
                         src-reader# (typecast/datatype->reader ~dtype item# unchecked?#)]
                     (-> (reify ~(typecast/datatype->reader-type dtype)
                           (getDatatype [item#] ~dtype)
                           (size [item#] (.size src-reader#))
                           (read [item# idx#]
                             (->> (.read src-reader# idx#)
                                  (.op un-op#)))
                           (iterator [item#]
                             (make-unary-op-iterator ~dtype src-reader#
                                                     un-op# unchecked?#))
                           (invoke [item# idx#]
                             (.read item# (int idx#)))))))]))]
        (into {})))


(def unary-op-reader-table (make-unary-op-reader-table))


(defn unary-reader-map
  [{:keys [datatype unchecked?]} un-op item]
  (let [datatype (or datatype (dtype-base/get-datatype item))]
    (if (= un-op :no-op)
      (dtype-proto/->reader-of-type item datatype unchecked?)
      (if-let [reader-fn (get unary-op-reader-table (casting/flatten-datatype datatype))]
        (reader-fn item un-op unchecked?)
        (throw (ex-info (format "Cannot unary map datatype %s" datatype) {}))))))



(defmacro make-double-unary-op
  [op-code]
  `(make-unary-op :float64 ~op-code))


(defmacro make-numeric-unary-op
  [op-code]
  `(reify
     dtype-proto/PToUnaryOp
     (->unary-op [item# datatype# unchecked?#]
       (when-not (casting/numeric-type? datatype#)
         (throw (ex-info (format "datatype is not numeric: %s" datatype#) {})))
       (case (casting/safe-flatten datatype#)
         :int8 (make-unary-op :int8 (unchecked-byte ~op-code))
         :int16 (make-unary-op :int16 (unchecked-short ~op-code))
         :int32 (make-unary-op :int32 (unchecked-int ~op-code))
         :int64 (make-unary-op :int64 ~op-code)
         :float32 (make-unary-op :float32 ~op-code)
         :float64 (make-unary-op :float64 ~op-code)))
     dtype-proto/PDatatype
     (get-datatype [item#] :float64)
     ))


(defmacro make-float-double-unary-op
  [op-code]
  `(reify
     dtype-proto/PToUnaryOp
     (->unary-op [item# datatype# unchecked?#]
       (when-not (casting/numeric-type? datatype#)
         (throw (ex-info (format "datatype is not numeric: %s" datatype#) {})))
       (let [op-dtype# (if (or (= datatype# :float32)
                               (= datatype# :float64))
                         datatype#
                         :float64)
             retval# (case op-dtype#
                       :float32 (make-unary-op :float32 ~op-code)
                       :float64 (make-unary-op :float64 ~op-code))]
         (if-not (= op-dtype# datatype#)
           (dtype-proto/->unary-op retval# datatype# unchecked?#)
           retval#)))))


(def builtin-unary-ops
  {:floor (make-double-unary-op (Math/floor arg))
   :ceil (make-double-unary-op (Math/ceil arg))
   :round (make-double-unary-op (unchecked-double (Math/round arg)))
   :rint (make-double-unary-op (Math/rint arg))
   :- (make-double-unary-op (- arg))
   :logistic (make-double-unary-op
              (/ 1.0
                 (+ 1.0 (Math/exp (- arg)))))
   :not (make-double-unary-op (if (= 0.0 arg) 1.0 0.0))
   :expp (make-double-unary-op (Math/exp arg))
   :expm1 (make-double-unary-op (Math/expm1 arg))
   :log (make-double-unary-op (Math/log arg))
   :log10 (make-double-unary-op (Math/log10 arg))
   :log1p (make-double-unary-op (Math/log1p arg))
   :signum (make-double-unary-op (Math/signum arg))
   :sqrt (make-double-unary-op (Math/sqrt arg))
   :cbrt (make-double-unary-op (Math/cbrt arg))
   :abs (make-double-unary-op (Math/abs arg))
   :sq (make-numeric-unary-op (unchecked-multiply arg arg))
   :sin (make-double-unary-op (Math/sin arg))
   :sinh (make-double-unary-op (Math/sinh arg))
   :cos (make-double-unary-op (Math/cos arg))
   :cosh (make-double-unary-op (Math/cosh arg))
   :tan (make-double-unary-op (Math/tan arg))
   :tanh (make-double-unary-op (Math/tanh arg))
   :acos (make-double-unary-op (Math/acos arg))
   :asin (make-double-unary-op (Math/asin arg))
   :atan (make-double-unary-op (Math/atan arg))
   :to-degrees (make-double-unary-op (Math/toDegrees arg))
   :to-radians (make-double-unary-op (Math/toRadians arg))

   :next-up (make-float-double-unary-op (Math/nextUp arg))
   :next-down (make-float-double-unary-op (Math/nextDown arg))
   :ulp (make-float-double-unary-op (Math/ulp arg))

   :bit-not (make-unary-op :int64 (bit-not arg))
   :/ (make-numeric-unary-op (/ arg))
   :no-op :no-op})


(defn apply-unary-op
    "Perform operation returning a scalar, reader, or an iterator.  Note that the
  results of this could be a reader, iterable or a scalar depending on what was passed
  in.  Also note that the results are lazyily calculated so no computation is done in
  this method aside from building the next thing *unless* the inputs are scalar in which
  case the operation is evaluated immediately."
  [{:keys [datatype unchecked?] :as options} un-op arg]
  (cond
    (satisfies? dtype-proto/PToReader arg)
    (unary-reader-map options un-op arg)

    (or (instance? Iterable arg)
        (satisfies? dtype-proto/PToIterable arg))
    (unary-iterable-map options un-op arg)
    :else
    (let [datatype (or datatype (dtype-base/get-datatype arg))]
      (if (= :no-op un-op)
        (if unchecked?
          (casting/unchecked-cast arg datatype)
          (casting/cast arg datatype)))
      (case (casting/safe-flatten datatype)
        :int8 (.op (datatype->unary-op :int8 un-op unchecked?) arg)
        :int16 (.op (datatype->unary-op :int16 un-op unchecked?) arg)
        :int32 (.op (datatype->unary-op :int32 un-op unchecked?) arg)
        :int64 (.op (datatype->unary-op :int64 un-op unchecked?) arg)
        :float32 (.op (datatype->unary-op :float32 un-op unchecked?) arg)
        :float64 (.op (datatype->unary-op :float64 un-op unchecked?) arg)
        :boolean (.op (datatype->unary-op :boolean un-op unchecked?) arg)
        :object (.op (datatype->unary-op :object un-op unchecked?) arg)))))
