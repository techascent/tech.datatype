(ns tech.datatype.binary-op
  (:require [tech.datatype.typecast :as typecast]
            [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.nio-access :as nio-access]
            [tech.datatype.reader :as reader])
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


(defmacro make-marshalling-binary-op-impl
  [dst-datatype src-datatype]
  (let [host-datatype (casting/safe-flatten dst-datatype)
        src-host-datatype (casting/safe-flatten src-datatype)]
    `(fn [un-op# datatype# unchecked?#]
       (let [src-op# (datatype->binary-op ~src-host-datatype un-op# unchecked?#)]
         (if unchecked?#
           (reify ~(datatype->binary-op-type host-datatype)
             (getDatatype [item#] datatype#)
             (op [item# x# y#]
               (let [value# (.op src-op#
                                 (nio-access/unchecked-full-cast x#
                                                                 ~host-datatype
                                                                 ~dst-datatype
                                                                 ~src-datatype)
                                 (nio-access/unchecked-full-cast y#
                                                                 ~host-datatype
                                                                 ~dst-datatype
                                                                 ~src-datatype))]
                 (nio-access/unchecked-full-cast value#
                                                 ~src-host-datatype
                                                 ~src-datatype
                                                 ~dst-datatype)))
             (invoke [item# x# y#]
               (.op item#
                    (casting/datatype->cast-fn
                     :unknown ~dst-datatype x#)
                    (casting/datatype->cast-fn
                     :unknown ~dst-datatype y#))))
           (reify ~(datatype->binary-op-type host-datatype)
             (getDatatype [item#] ~dst-datatype)
             (op [item# x# y#]
               (let [value# (.op src-op#
                                 (nio-access/checked-full-write-cast
                                  x#
                                  ~host-datatype
                                  ~dst-datatype
                                  ~src-datatype)
                                 (nio-access/checked-full-write-cast
                                  y#
                                  ~host-datatype
                                  ~dst-datatype
                                  ~src-datatype))]
                 (nio-access/checked-full-write-cast value#
                                                     ~src-host-datatype
                                                     ~src-datatype
                                                     ~dst-datatype)))
             (invoke [item# x# y#]
               (.op item#
                    (casting/datatype->cast-fn
                     :unknown ~dst-datatype x#)
                    (casting/datatype->cast-fn
                     :unknown ~dst-datatype y#)))))))))


(defmacro make-marshalling-binary-table
  []
  `(->> [~@(for [src-dtype casting/base-host-datatypes
                 dst-dtype casting/base-host-datatypes]
             [[src-dtype dst-dtype] `(make-marshalling-binary-op-impl
                                      ~dst-dtype ~src-dtype)])]
        (into {})))


(def marshalling-binary-op-table (make-marshalling-binary-table))


(defmacro extend-binary-op
  [datatype]
  `(clojure.core/extend
       ~(datatype->binary-op-type datatype)
     dtype-proto/PToBinaryOp
     {:->binary-op
      (fn [item# datatype# unchecked?#]
        (if (= (casting/safe-flatten datatype#)
               ~datatype)
          item#
          (let [marshal-fn# (get marshalling-binary-op-table
                                 [~datatype (casting/safe-flatten
                                             datatype#)])]
            (marshal-fn# item# datatype# unchecked?#))))}))


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
       (.op item#
            (casting/datatype->cast-fn :unknown ~datatype x#)
            (casting/datatype->cast-fn :unknown ~datatype y#)))))


(defmacro make-binary-op-iterator
  [dtype lhs-item rhs-item binary-op unchecked?]
  `(let [lhs-iter# (typecast/datatype->iter ~dtype ~lhs-item ~unchecked?)
         rhs-iter# (typecast/datatype->iter ~dtype ~rhs-item ~unchecked?)
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
                `(fn [lhs# rhs# bin-op# unchecked?#]
                   (reify
                     Iterable
                     (iterator [iter-item#]
                       (make-binary-op-iterator ~dtype lhs# rhs# bin-op# unchecked?#))
                     dtype-proto/PDatatype
                     (get-datatype [iter-item#] ~dtype)))]))]
        (into {})))

(def binary-op-iter-table (make-binary-op-iter-table))

(defn binary-iterable-map
  [{:keys [datatype unchecked?]} bin-op lhs rhs]
  (let [dtype (or datatype (dtype-proto/get-datatype lhs))]
    (if-let [iter-fn (get binary-op-iter-table (casting/flatten-datatype dtype))]
      (iter-fn lhs rhs bin-op unchecked?)
      (throw (ex-info (format "Cannot unary map datatype %s" dtype) {})))))


(defmacro make-binary-op-reader-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             (let [host-dtype (casting/datatype->safe-host-type dtype)]
               [dtype
                `(fn [lhs# rhs# bin-op# unchecked?#]
                   (let [bin-op# (datatype->binary-op ~dtype bin-op# true)
                         lhs-reader# (typecast/datatype->reader ~dtype lhs# unchecked?#)
                         rhs-reader# (typecast/datatype->reader ~dtype rhs# unchecked?#)
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
                              ~dtype lhs-reader# rhs-reader# bin-op# unchecked?#) )
                           (invoke [item# idx#]
                             (.read item# (int idx#)))))))]))]
        (into {})))

(def binary-op-reader-table (make-binary-op-reader-table))

(defn binary-reader-map
  [{:keys [datatype unchecked?]} bin-op lhs rhs]
  (let [dtype (or datatype (dtype-proto/get-datatype lhs))]
    (if-let [reader-fn (get binary-op-reader-table (casting/flatten-datatype dtype))]
      (reader-fn lhs rhs bin-op unchecked?)
      (throw (ex-info (format "Cannot binary map datatype %s" dtype) {})))))


(defmacro make-double-binary-op
  [op-code]
  `(make-binary-op :float64 ~op-code))


(defmacro make-numeric-binary-op
  [op-code]
  `(reify
     dtype-proto/PToBinaryOp
     (->binary-op [item# datatype# unchecked?#]
       (when-not (casting/numeric-type? datatype#)
         (throw (ex-info (format "datatype is not numeric: %s" datatype#) {})))
       (case (casting/safe-flatten datatype#)
         :int8 (make-binary-op :int8 (unchecked-byte ~op-code))
         :int16 (make-binary-op :int16 (unchecked-short ~op-code))
         :int32 (make-binary-op :int32 (unchecked-int ~op-code))
         :int64 (make-binary-op :int64 (unchecked-long ~op-code))
         :float32 (make-binary-op :float32 (unchecked-float ~op-code))
         :float64 (make-binary-op :float64 (unchecked-double ~op-code))))
     dtype-proto/PDatatype
     (get-datatype [item#] :float64)))


(defmacro make-long-binary-op
  [op-code]
  `(make-binary-op :int64 ~op-code))


(def builtin-binary-ops
  {
   :+ (make-numeric-binary-op (+ x y))
   :- (make-numeric-binary-op (- x y))
   :/ (make-numeric-binary-op (/ x y))
   :* (make-numeric-binary-op (* x y))
   :rem (long-binary-op (Math/floorMod x y))
   :quot (make-long-binary-op (Math/floorDiv x y))
   :pow (make-double-binary-op (Math/pow x y))
   ;;Math/max and friends aren't defined for all primitives leading to reflection
   ;;warnings.
   :max (make-numeric-binary-op (if (> x y) x y))
   :min (make-numeric-binary-op (if (> x y) y x))
   :and (make-double-binary-op (if (and (not= 0.0 x)
                                      (not= 0.0 y))
                               1.0 0.0))
   :or (make-double-binary-op (if (or (not= 0.0 x)
                                    (not= 0.0 y))
                              1.0 0.0))
   :bit-and (make-long-binary-op (bit-and x y))
   :bit-and-not (make-long-binary-op (bit-and-not x y))
   :bit-or (make-long-binary-op (bit-or x y))
   :bit-xor (make-long-binary-op (bit-xor x y))
   :bit-clear (make-long-binary-op (bit-clear x y))
   :bit-flip (make-long-binary-op (bit-flip x y))
   :bit-test (make-long-binary-op (bit-test x y))
   :bit-set (make-long-binary-op (bit-set x y))
   :bit-shift-left (make-long-binary-op (bit-shift-left x y))
   :bit-shift-right (make-long-binary-op (bit-shift-right x y))
   :unsigned-bit-shift-right (make-long-binary-op (unsigned-bit-shift-right x y))
   :eq (make-numeric-binary-op (if (= x y) 1 0))
   :not-eq (make-numeric-binary-op (if (not= x y) 1 0))
   :> (make-numeric-binary-op (if (> x y) 1 0))
   :>= (make-numeric-binary-op (if (>= x y) 1 0))
   :< (make-numeric-binary-op (if (< x y) 1 0))
   :<= (make-numeric-binary-op (if (<= x y) 1 0))
   :atan2 (make-double-binary-op (Math/atan2 x y))
   :hypot (make-double-binary-op (Math/hypot x y))
   :ieee-remainder (make-double-binary-op (Math/IEEEremainder x y))
   })


(defn- arg->arg-type
  [arg]
  (cond
    (satisfies? dtype-proto/PToReader arg) :reader
    (or (instance? Iterable arg)
        (satisfies? dtype-proto/PToIterable arg)) :iterable
    :else
    :scalar))


(defn apply-binary-op
  "We perform a left-to-right reduction making scalars/readers/etc.  This matches
  clojure semantics.  Note that the results of this could be a reader, iterable or a
  scalar depending on what was passed in.  Also note that the results are lazyily
  calculated so no computation is done in this method aside from building the next thing
  *unless* the inputs are scalar in which case the operation is evaluated immediately."
  [{:keys [datatype unchecked?] :as options}
   bin-op arg1 arg2 & args]
  (let [all-args (concat [arg1 arg2] args)
        all-arg-types (->> all-args
                           (map arg->arg-type)
                           set)
        op-arg-type (cond
                      (all-arg-types :iterable)
                      :iterable
                      (all-arg-types :reader)
                      :reader
                      :else
                      :scalar)
        datatype (or datatype (dtype-base/get-datatype arg1))
        n-elems (long (if (= op-arg-type :reader)
                        (->> all-args
                             (remove #(= :scalar (arg->arg-type %)))
                             (map dtype-base/ecount)
                             (apply min))
                        Integer/MAX_VALUE))]
    (loop [arg1 arg1
           arg2 arg2
           args args]
      (let [arg1-type (arg->arg-type arg1)
            arg2-type (arg->arg-type arg2)
            op-map-fn (case op-arg-type
                        :iterable
                        (partial binary-iterable-map (assoc options :datatype datatype)
                                 bin-op)
                        :reader
                        (partial binary-reader-map (assoc options :datatype datatype)
                                 bin-op)
                        :scalar
                        nil)
            arg-result
            (cond
              (and (= arg1-type :scalar)
                   (= arg2-type :scalar))
              (case (casting/safe-flatten datatype)
                :int8 (.op (datatype->binary-op :int8 bin-op unchecked?) arg1 arg2)
                :int16 (.op (datatype->binary-op :int16 bin-op unchecked?) arg1 arg2)
                :int32 (.op (datatype->binary-op :int32 bin-op unchecked?) arg1 arg2)
                :int64 (.op (datatype->binary-op :int64 bin-op unchecked?) arg1 arg2)
                :float32 (.op (datatype->binary-op :float32 bin-op unchecked?)
                              arg1 arg2)
                :float64 (.op (datatype->binary-op :float64 bin-op unchecked?)
                              arg1 arg2)
                :boolean (.op (datatype->binary-op :boolean bin-op unchecked?)
                              arg1 arg2)
                :object (.op (datatype->binary-op :object bin-op unchecked?)
                             arg1 arg2))
              (= arg1-type :scalar)
              (op-map-fn (reader/make-const-reader arg1 datatype) arg2)
              (= arg2-type :scalar)
              (op-map-fn arg1 (reader/make-const-reader arg2 datatype))
              :else
              (op-map-fn arg1 arg2))]
        (if (first args)
          (recur arg-result (first args) (rest args))
          arg-result)))))
