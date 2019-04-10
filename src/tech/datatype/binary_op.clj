(ns tech.datatype.binary-op
  (:require [tech.datatype.typecast :as typecast]
            [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.nio-access :as nio-access]
            [tech.datatype.reader :as reader]
            [tech.datatype.unary-op :as unary-op]
            [tech.datatype.argtypes :as argtypes])
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
       (let [src-op# (datatype->binary-op ~src-host-datatype un-op# unchecked?#)
             op-name# (dtype-base/op-name un-op#)]
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
                     :unknown ~dst-datatype y#)))
             dtype-proto/POperator
             (op-name [item#] op-name#))
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
                     :unknown ~dst-datatype y#)))
             dtype-proto/POperator
             (op-name [item#] op-name#)))))))


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
  ([opname datatype body]
   `(reify ~(datatype->binary-op-type datatype)
      (getDatatype [item#] ~datatype)
      (op [item# ~'x ~'y]
        ~body)
      (invoke [item# x# y#]
        (.op item#
             (casting/datatype->cast-fn :unknown ~datatype x#)
             (casting/datatype->cast-fn :unknown ~datatype y#)))
      dtype-proto/POperator
      (op-name [item#] ~opname)))
  ([datatype body]
   `(make-binary-op :unnamed ~datatype ~body)))


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
                             (.read item# (int idx#)))
                           dtype-proto/PToBackingStore
                           (->backing-store-seq  [item#]
                             (concat (dtype-proto/->backing-store-seq lhs-reader#)
                                     (dtype-proto/->backing-store-seq rhs-reader#))))
                         )))]))]
        (into {})))


(def binary-op-reader-table (make-binary-op-reader-table))


(defn default-binary-reader-map
  [{:keys [datatype unchecked?]} bin-op lhs rhs]
  (let [dtype (or datatype (dtype-proto/get-datatype lhs))]
    (if-let [reader-fn (get binary-op-reader-table (casting/flatten-datatype dtype))]
      (reader-fn lhs rhs bin-op unchecked?)
      (throw (ex-info (format "Cannot binary map datatype %s" dtype) {})))))


(defmulti binary-reader-map
  (fn [options binary-op lhs rhs]
    [(dtype-base/buffer-type lhs)
     (dtype-base/buffer-type rhs)]))


(defmethod binary-reader-map :default
  [{:keys [datatype unchecked?] :as options} bin-op lhs rhs]
  (default-binary-reader-map options bin-op lhs rhs))


(defmacro make-double-binary-op
  [opname op-code]
  `(make-binary-op ~opname :float64 ~op-code))


(defmacro make-numeric-binary-op
  [opname op-code]
  `(reify
     dtype-proto/PToBinaryOp
     (->binary-op [item# datatype# unchecked?#]
       (let [bin-dtype# (if (casting/numeric-type? datatype#)
                          datatype#
                          :float64)]
         (-> (case (casting/safe-flatten bin-dtype#)
               :int8 (make-binary-op ~opname :int8 (unchecked-byte ~op-code))
               :int16 (make-binary-op ~opname :int16 (unchecked-short ~op-code))
               :int32 (make-binary-op ~opname :int32 (unchecked-int ~op-code))
               :int64 (make-binary-op ~opname :int64 (unchecked-long ~op-code))
               :float32 (make-binary-op ~opname :float32 (unchecked-float ~op-code))
               :float64 (make-binary-op ~opname :float64 (unchecked-double ~op-code)))
             (dtype-proto/->binary-op datatype# unchecked?#))))
     dtype-proto/POperator
     (op-name [item#] ~opname)
     dtype-proto/PDatatype
     (get-datatype [item#] :float64)))


(defmacro make-numeric-object-binary-op
  [opname op-code]
  `(reify
     dtype-proto/PToBinaryOp
     (->binary-op [item# datatype# unchecked?#]
       (let [bin-dtype# (if (casting/numeric-type? datatype#)
                          datatype#
                          :float64)]
         (-> (case (casting/safe-flatten bin-dtype#)
               :int8 (make-binary-op ~opname :int8 (unchecked-byte ~op-code))
               :int16 (make-binary-op ~opname :int16 (unchecked-short ~op-code))
               :int32 (make-binary-op ~opname :int32 (unchecked-int ~op-code))
               :int64 (make-binary-op ~opname :int64 (unchecked-long ~op-code))
               :float32 (make-binary-op ~opname :float32 (unchecked-float ~op-code))
               :float64 (make-binary-op ~opname :float64 (unchecked-double ~op-code))
               :object (make-binary-op ~opname :object ~op-code))
             (dtype-proto/->binary-op datatype# unchecked?#))))
     dtype-proto/POperator
     (op-name [item#] ~opname)
     dtype-proto/PDatatype
     (get-datatype [item#] :object)))


(defmacro make-long-binary-op
  [opname op-code]
  `(make-binary-op ~opname :int64 ~op-code))


(set! *unchecked-math* false)


(def builtin-binary-ops
  (->> [(make-numeric-object-binary-op :+ (+ x y))
        (make-numeric-object-binary-op :- (- x y))
        (make-numeric-object-binary-op :/ (/ x y))
        (make-numeric-object-binary-op :* (* x y))
        (make-long-binary-op :rem (Math/floorMod x y))
        (make-long-binary-op :quot (Math/floorDiv x y))
        (make-double-binary-op :pow (Math/pow x y))
        (make-numeric-object-binary-op :max (if (> x y) x y))
        (make-numeric-object-binary-op :min (if (> x y) y x))
        (make-long-binary-op :bit-and (bit-and x y))
        (make-long-binary-op :bit-and-not (bit-and-not x y))
        (make-long-binary-op :bit-or (bit-or x y))
        (make-long-binary-op :bit-xor (bit-xor x y))
        (make-long-binary-op :bit-clear (bit-clear x y))
        (make-long-binary-op :bit-flip (bit-flip x y))
        (make-long-binary-op :bit-test (bit-test x y))
        (make-long-binary-op :bit-set (bit-set x y))
        (make-long-binary-op :bit-shift-left (bit-shift-left x y))
        (make-long-binary-op :bit-shift-right (bit-shift-right x y))
        (make-long-binary-op :unsigned-bit-shift-right (unsigned-bit-shift-right x y))
        (make-double-binary-op :atan2 (Math/atan2 x y))
        (make-double-binary-op :hypot (Math/hypot x y))
        (make-double-binary-op :ieee-remainder (Math/IEEEremainder x y))]
       (map #(vector (dtype-proto/op-name %) %))
       (into {})))


(defmacro make-binary->unary
  [datatype]
  `(fn [bin-op# dtype# const-val# left-assoc?#]
     (let [bin-op# (datatype->binary-op ~datatype bin-op# true)
           const-val# (casting/datatype->cast-fn :unknown ~datatype const-val#)
           op-name# (if left-assoc?#
                      (keyword (str (name (dtype-base/op-name bin-op#))
                                    " - " const-val# " - " "arg"))
                      (keyword (str (name (dtype-base/op-name bin-op#))
                                    " - " "arg" " - " (str const-val#))))]
       (if left-assoc?#
         (reify
           ~(unary-op/datatype->unary-op-type datatype)
           (getDatatype [op#] dtype#)
           (op [item# arg#]
             (.op bin-op# const-val# arg#))
           (invoke [item# arg#]
             (.op item# (casting/datatype->cast-fn :unknown ~datatype arg#)))
           dtype-proto/POperator
           (op-name [item#] op-name#))
         (reify
           ~(unary-op/datatype->unary-op-type datatype)
           (getDatatype [op#] dtype#)
           (op [item# arg#]
             (.op bin-op# arg# const-val#))
           (invoke [item# arg#]
             (.op item# (casting/datatype->cast-fn :unknown ~datatype arg#)))
           dtype-proto/POperator
           (op-name [item#] op-name#))))))


(defmacro make-binary->unary-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-binary->unary ~dtype)])]
        (into {})))

(def binary->unary-table (make-binary->unary-table))

(defn binary->unary
  [{:keys [datatype unchecked? left-associate?]} bin-op constant-value]
  (let [datatype (or datatype (dtype-base/get-datatype constant-value))
        create-fn (get binary->unary-table (casting/safe-flatten datatype))]
    (create-fn bin-op datatype constant-value left-associate?)))
