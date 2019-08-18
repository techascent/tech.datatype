(ns tech.v2.datatype.unary-op
  (:require [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.iterator :as iterator]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.nio-access :as nio-access]
            [tech.v2.datatype.argtypes :as argtypes]
            [tech.v2.datatype.reader :as reader])
  (:import [tech.v2.datatype
            ByteIter ShortIter IntIter LongIter
            FloatIter DoubleIter BooleanIter ObjectIter
            ByteReader ShortReader IntReader LongReader
            FloatReader DoubleReader BooleanReader ObjectReader
            UnaryOperators$ByteUnary  UnaryOperators$ShortUnary
            UnaryOperators$IntUnary  UnaryOperators$LongUnary
            UnaryOperators$FloatUnary  UnaryOperators$DoubleUnary
            UnaryOperators$BooleanUnary  UnaryOperators$ObjectUnary]
           [clojure.lang IFn]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn datatype->unary-op-type
  [datatype]
  (let [datatype (casting/datatype->safe-host-type datatype)]
    (case datatype
      :int8 'tech.v2.datatype.UnaryOperators$ByteUnary
      :int16 'tech.v2.datatype.UnaryOperators$ShortUnary
      :int32 'tech.v2.datatype.UnaryOperators$IntUnary
      :int64 'tech.v2.datatype.UnaryOperators$LongUnary
      :float32 'tech.v2.datatype.UnaryOperators$FloatUnary
      :float64 'tech.v2.datatype.UnaryOperators$DoubleUnary
      :boolean 'tech.v2.datatype.UnaryOperators$BooleanUnary
      :object 'tech.v2.datatype.UnaryOperators$ObjectUnary)))


(defmacro extend-unary-op
  [datatype]
  `(clojure.core/extend
       ~(datatype->unary-op-type datatype)
     dtype-proto/PToUnaryOp
     {:convertible-to-unary-op? (constantly true)
      :->unary-op
      (fn [item# options#]
        (when-not (= (dtype-proto/get-datatype item#)
                     (:datatype options#))
          (throw (ex-info (format "Cannot convert unary operator %s->%s"
                                  ~datatype (:datatype options#))
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
     (dtype-proto/->unary-op ~item {:datatype ~datatype
                                    :unchecked? ~'unchecked?})))


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
  [src-datatype dst-datatype]
  (let [host-datatype (casting/safe-flatten dst-datatype)
        src-host-datatype (casting/safe-flatten src-datatype)]
    `(fn [un-op# datatype# unchecked?#]
       (let [src-op# (datatype->unary-op ~src-host-datatype un-op# unchecked?#)
             op-name# (dtype-base/op-name un-op#)]
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
                           :unknown ~dst-datatype arg#)))
             dtype-proto/POperator
             (op-name [item#] op-name#))
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
                           :unknown ~dst-datatype arg#)))
             dtype-proto/POperator
             (op-name [item#] op-name#)))))))



;; (def marshalling-unary-op-table (casting/make-marshalling-item-table
;;                                  make-marshalling-unary-op-impl))


(defmacro extend-unary-op
  [datatype]
  `(clojure.core/extend
       ~(datatype->unary-op-type datatype)
     dtype-proto/PToUnaryOp
     {:convertible-to-unary-op? (constantly true)
      :->unary-op (fn [item# options#]
                    (let [un-dtype# (or (:datatype options#)
                                        (dtype-proto/get-datatype item#))
                          unchecked?# (:unchecked? options#)]
                      (if (= (casting/safe-flatten un-dtype#)
                             ~datatype)
                        item#
                        (throw (ex-info "Unary operators cannot marshal" {}))
                        ;; (let [marshal-fn# (get marshalling-unary-op-table
                        ;;                        [~datatype (casting/safe-flatten
                        ;;                                    un-dtype#)])]
                        ;;   (marshal-fn# item# un-dtype# unchecked?#))
                        )))}))


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
  placed into the local namespace as 'x'.
  (make-unary-op :plus10 :int32 (+ arg 10))"
  ([opname datatype body]
   `(reify
      ~(datatype->unary-op-type datatype)
      (getDatatype [item#] ~datatype)
      (op [item# ~'x]
        ~body)
      (invoke [item# arg#]
        (.op item# (casting/datatype->cast-fn :unknown ~datatype arg#)))
      dtype-proto/POperator
      (op-name [item#] ~opname)))
  ([datatype body]
   `(make-unary-op :unamed ~datatype ~body)))


(extend-type Object
  dtype-proto/PToUnaryOp
  (convertible-to-unary-op? [item] (instance? clojure.lang.IFn item))
  (->unary-op [item options]
    (let [dtype (casting/safe-flatten (or (:datatype options) :object))]
      (case dtype
        :int8 (make-unary-op :int8 (byte (item x)))
        :int16 (make-unary-op :int16 (short (item x)))
        :int32 (make-unary-op :int32 (int (item x)))
        :int64 (make-unary-op :int64 (long (item x)))
        :float32 (make-unary-op :float32 (float (item x)))
        :float64 (make-unary-op :float64 (double (item x)))
        :boolean (make-unary-op :boolean (casting/datatype->cast-fn
                                                   :unkown
                                                   :boolean
                                                   (item x)))
        :object (make-unary-op :object (item x))))))


(defmacro make-unary-op-iterator
  [dtype]
  `(fn [item# un-op# unchecked?#]
     (reify
       dtype-proto/PDatatype
       (get-datatype [iter-item#] ~dtype)
       Iterable
       (iterator [iter-item#]
         (let [src-iter# (typecast/datatype->iter ~dtype item# unchecked?#)
               un-op# (datatype->unary-op ~dtype un-op# unchecked?#)]
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
                    (.op un-op#)))))))))



(def unary-op-iter-table (casting/make-base-datatype-table
                          make-unary-op-iterator))


(defn unary-iterable-map
  [{:keys [datatype unchecked?] :as options} un-op item]
  (let [datatype (or datatype (dtype-base/get-datatype item))]
    (if (= (dtype-proto/op-name un-op) :identity)
      (dtype-proto/->iterable item options)
      ;;For object iteration map is probably faster
      (if (= datatype :object)
        (map (datatype->unary-op :object un-op true) item)
        (if-let [iter-fn (get unary-op-iter-table (casting/safe-flatten datatype))]
          (iter-fn item un-op unchecked?)
          (throw (ex-info (format "Cannot unary map datatype %s" datatype) {})))))))


(defmacro unary-iterable
  ([datatype op-code item]
   `(unary-iterable-map
     {:datatype ~datatype}
     (make-unary-op ~datatype ~op-code)
     ~item))
  ([op-code item]
   `(unary-iterable :object ~op-code ~item)))


(declare unary-reader-map)


(defmacro make-unary-op-reader-table
  [dtype]
  `(fn [item# un-op# unchecked?#]
     (let [un-op# (datatype->unary-op ~dtype un-op# true)
           src-reader# (typecast/datatype->reader ~dtype item#
                                                  unchecked?#)
           src-dtype# (dtype-base/get-datatype src-reader#)
           constructor# #(unary-reader-map
                          (select-keys %2 [:unchecked?])
                          un-op#
                          %1)]
       (reader/make-derived-reader ~dtype src-dtype# unchecked?#
                                   src-reader#
                                   (->> (.read src-reader# ~'idx)
                                        (.op un-op#))
                                   constructor#))))


(def unary-op-reader-table (casting/make-base-datatype-table
                            make-unary-op-reader-table))


(defmulti unary-reader-map
  (fn [options un-op item]
    (dtype-base/buffer-type item)))


(defn default-unary-reader-map
  [{:keys [datatype unchecked?]} un-op item]
  (let [datatype (or datatype (dtype-base/get-datatype item))]
    (if (= (dtype-proto/op-name un-op) :identity)
      item
      (if-let [reader-fn (get unary-op-reader-table
                              (casting/flatten-datatype datatype))]
        (reader-fn item un-op unchecked?)
        (throw (ex-info (format "Cannot unary map datatype %s" datatype) {}))))))


(defmethod unary-reader-map :default
  [{:keys [datatype unchecked?] :as options} un-op item]
  (default-unary-reader-map options un-op item))


(defmacro unary-reader
  ([datatype un-op item]
   `(unary-reader-map
     {:datatype ~datatype}
     (make-unary-op ~datatype ~un-op)
     ~item))
  ([un-op item]
   `(unary-reader :object ~un-op ~item)))


(defmacro make-double-unary-op
  [opname op-code]
  `(reify
     dtype-proto/PToUnaryOp
     (convertible-to-unary-op? [item#] true)
     (->unary-op [item# options#]
       (let [{datatype# :datatype
              unchecked?# :unchecked?} options#]
         (when-not (or (= :object datatype#)
                       (casting/numeric-type? datatype#))
           (throw (ex-info (format "datatype is not numeric: %s" datatype#) {})))
         (case (casting/safe-flatten datatype#)
           :int32 (make-unary-op ~opname :int32 (unchecked-int ~op-code))
           :int64 (make-unary-op ~opname :int64 (unchecked-long ~op-code))
           :float32 (make-unary-op ~opname :float32 (unchecked-float ~op-code))
           :float64 (make-unary-op ~opname :float64 ~op-code)
           :object (make-unary-op ~opname :object ~op-code))))
     dtype-proto/PDatatype
     (get-datatype [item#] :float64)
     dtype-proto/POperator
     (op-name [item#] ~opname)
     IFn
     (invoke [item# ~'x]
       ~op-code)))


(defmacro make-numeric-unary-op
  [opname op-code]
  `(reify
     dtype-proto/PToUnaryOp
     (convertible-to-unary-op? [item#] true)
     (->unary-op [item# options#]
       (let [{datatype# :datatype
              unchecked?# :unchecked?} options#]
         (when-not (or (= :object datatype#)
                       (casting/numeric-type? datatype#))
           (throw (ex-info (format "datatype is not numeric: %s" datatype#) {})))
         (case (casting/safe-flatten datatype#)
           :int8 (make-unary-op ~opname :int8 (byte ~op-code))
           :int16 (make-unary-op ~opname :int16 (short ~op-code))
           :int32 (make-unary-op ~opname :int32 (int ~op-code))
           :int64 (make-unary-op ~opname :int64 ~op-code)
           :float32 (make-unary-op ~opname :float32 ~op-code)
           :float64 (make-unary-op ~opname :float64 ~op-code)
           :object (make-unary-op ~opname :object ~op-code))))
     dtype-proto/PDatatype
     (get-datatype [item#] :float64)
     dtype-proto/POperator
     (op-name [item#] ~opname)
     IFn
     (invoke [item# ~'x]
       ~op-code)))


(defmacro make-numeric-object-unary-op
  [opname op-code]
  `(reify
     dtype-proto/PToUnaryOp
     (convertible-to-unary-op? [item#] true)
     (->unary-op [item# options#]
       (let [{datatype# :datatype
              unchecked?# :unchecked?} options#]
         (when-not (casting/numeric-type? datatype#)
           (throw (ex-info (format "datatype is not numeric: %s" datatype#) {})))
         (case (casting/safe-flatten datatype#)
           :int8 (make-unary-op ~opname :int8 (byte ~op-code))
           :int16 (make-unary-op ~opname :int16 (short ~op-code))
           :int32 (make-unary-op ~opname :int32 (int ~op-code))
           :int64 (make-unary-op ~opname :int64 ~op-code)
           :float32 (make-unary-op ~opname :float32 ~op-code)
           :float64 (make-unary-op ~opname :float64 ~op-code)
           :object (make-unary-op ~opname :object ~op-code))))
     dtype-proto/PDatatype
     (get-datatype [item#] :object)
     dtype-proto/POperator
     (op-name [item#] ~opname)
     IFn
     (invoke [item# ~'x]
       ~op-code)))


(defmacro make-float-double-unary-op
  [opname op-code]
  `(reify
     dtype-proto/POperator
     (op-name [item#] ~opname)
     dtype-proto/PDatatype
     (get-datatype [item#] :float64)
     dtype-proto/PToUnaryOp
     (convertible-to-unary-op? [item#] true)
     (->unary-op [item# options#]
       (let [{datatype# :datatype
              unchecked?# :unchecked?} options#]
         (when-not (#{:float64 :float32 :object} (casting/flatten-datatype
                                                  datatype#))
           (throw (ex-info (format "datatype is not float or double: %s"
                                   datatype#) {})))
         (let [op-dtype# (if (or (= datatype# :float32)
                                 (= datatype# :float64)
                                 (= datatype# :object))
                           datatype#
                           :float64)
               retval# (case op-dtype#
                         :float32 (make-unary-op ~opname :float32 ~op-code)
                         :float64 (make-unary-op ~opname :float64 ~op-code)
                         :object (make-unary-op ~opname :object (let [~'x (double ~'x)]
                                                                  ~op-code)))]
           (if-not (= op-dtype# datatype#)
             (dtype-proto/->unary-op retval# options#)
             retval#))))
     IFn
     (invoke [item# ~'x]
       (let [~'x (double ~'x)]
         ~op-code))))

(defmacro make-all-datatype-unary-op
  [opname op-code]
  `(reify
     dtype-proto/POperator
     (op-name [item#] ~opname)
     dtype-proto/PToUnaryOp
     (convertible-to-unary-op? [item#] true)
     (->unary-op [item# options#]
       (let [{datatype# :datatype
              unchecked?# :unchecked?} options#]
         (case (casting/safe-flatten datatype#)
           :int8 (make-unary-op ~opname :int8 ~op-code)
           :int16 (make-unary-op ~opname :int16 ~op-code)
           :int32 (make-unary-op ~opname :int32 ~op-code)
           :int64 (make-unary-op ~opname :int64 ~op-code)
           :float32 (make-unary-op ~opname :float32 ~op-code)
           :float64 (make-unary-op ~opname :float64 ~op-code)
           :boolean (make-unary-op ~opname :boolean ~op-code)
           :object (make-unary-op ~opname :object ~op-code))))
     IFn
     (invoke [item# ~'x]
       ~op-code)))


(set! *unchecked-math* false)


(def builtin-unary-ops
  (->> [(make-double-unary-op :floor (Math/floor x))
        (make-double-unary-op :ceil (Math/ceil x))
        (make-double-unary-op :round (unchecked-double (Math/round (double x))))
        (make-double-unary-op :rint (Math/rint x))
        (make-numeric-object-unary-op :- (- x))
        (make-float-double-unary-op :logistic
                                    (/ 1.0
                                       (+ 1.0 (Math/exp (- x)))))
        (make-float-double-unary-op :exp (Math/exp x))
        (make-float-double-unary-op :expm1 (Math/expm1 x))
        (make-float-double-unary-op :log (Math/log x))
        (make-float-double-unary-op :log10 (Math/log10 x))
        (make-float-double-unary-op :log1p (Math/log1p x))
        (make-float-double-unary-op :signum (Math/signum x))
        (make-float-double-unary-op :sqrt (Math/sqrt x))
        (make-float-double-unary-op :cbrt (Math/cbrt x))
        (make-float-double-unary-op :abs (Math/abs x))
        (make-numeric-unary-op :sq (unchecked-multiply x x))
        (make-float-double-unary-op :sin (Math/sin x))
        (make-float-double-unary-op :sinh (Math/sinh x))
        (make-float-double-unary-op :cos (Math/cos x))
        (make-float-double-unary-op :cosh (Math/cosh x))
        (make-float-double-unary-op :tan (Math/tan x))
        (make-float-double-unary-op :tanh (Math/tanh x))
        (make-float-double-unary-op :acos (Math/acos x))
        (make-float-double-unary-op :asin (Math/asin x))
        (make-float-double-unary-op :atan (Math/atan x))
        (make-float-double-unary-op :to-degrees (Math/toDegrees x))
        (make-float-double-unary-op :to-radians (Math/toRadians x))

        (make-float-double-unary-op :next-up (Math/nextUp x))
        (make-float-double-unary-op :next-down (Math/nextDown x))
        (make-float-double-unary-op :ulp (Math/ulp x))

        (make-unary-op :bit-not :int64 (bit-not x))
        (make-numeric-unary-op :/ (/ x))
        (make-all-datatype-unary-op :identity x)
        (make-all-datatype-unary-op :+ x)]
       (map #(vector (dtype-proto/op-name %) %))
       (into {})))
