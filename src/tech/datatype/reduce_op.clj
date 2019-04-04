(ns tech.datatype.reduce-op
  (:require [tech.datatype.reader :as reader]
            [tech.datatype.casting :as casting]
            [tech.datatype.typecast :as typecast]
            [tech.datatype.iterator :as iterator]
            [tech.datatype.binary-op :as binary-op]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.nio-access :as nio-access]
            [tech.datatype.argtypes :as argtypes])
  (:import [tech.datatype
            ReduceOperators$ByteReduce
            ReduceOperators$ShortReduce
            ReduceOperators$IntReduce
            ReduceOperators$LongReduce
            ReduceOperators$FloatReduce
            ReduceOperators$DoubleReduce
            ReduceOperators$BooleanReduce
            ReduceOperators$ObjectReduce]
           [clojure.lang IFn]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn datatype->reduce-op-type
  [datatype]
  (let [datatype (casting/datatype->safe-host-type datatype)]
    (case datatype
      :int8 'tech.datatype.ReduceOperators$ByteReduce
      :int16 'tech.datatype.ReduceOperators$ShortReduce
      :int32 'tech.datatype.ReduceOperators$IntReduce
      :int64 'tech.datatype.ReduceOperators$LongReduce
      :float32 'tech.datatype.ReduceOperators$FloatReduce
      :float64 'tech.datatype.ReduceOperators$DoubleReduce
      :boolean 'tech.datatype.ReduceOperators$BooleanReduce
      :object 'tech.datatype.ReduceOperators$ObjectReduce)))


(defmacro impl-reduce-op-cast
  [datatype item]
  `(if (instance? ~(resolve (datatype->reduce-op-type datatype)) ~item)
     ~item
     (if (satisfies? dtype-proto/PToReduceOp ~item)
       (dtype-proto/->reduce-op ~item ~datatype ~'unchecked?)
       (-> (dtype-proto/->binary-op ~item ~datatype true)
           (dtype-proto/->reduce-op ~datatype ~'unchecked?)))))


(defn int8->reduce-op ^ReduceOperators$ByteReduce [item unchecked?]
  (impl-reduce-op-cast :int8 item))
(defn int16->reduce-op ^ReduceOperators$ShortReduce [item unchecked?]
  (impl-reduce-op-cast :int16 item))
(defn int32->reduce-op ^ReduceOperators$IntReduce [item unchecked?]
  (impl-reduce-op-cast :int32 item))
(defn int64->reduce-op ^ReduceOperators$LongReduce [item unchecked?]
  (impl-reduce-op-cast :int64 item))
(defn float32->reduce-op ^ReduceOperators$FloatReduce [item unchecked?]
  (impl-reduce-op-cast :float32 item))
(defn float64->reduce-op ^ReduceOperators$DoubleReduce [item unchecked?]
  (impl-reduce-op-cast :float64 item))
(defn boolean->reduce-op ^ReduceOperators$BooleanReduce [item unchecked?]
  (impl-reduce-op-cast :boolean item))
(defn object->reduce-op ^ReduceOperators$ObjectReduce [item unchecked?]
  (impl-reduce-op-cast :object item))


(defmacro datatype->reduce-op
  [datatype item unchecked?]
  (let [host-dtype (casting/datatype->safe-host-type datatype)]
    (case host-dtype
      :int8 `(int8->reduce-op ~item ~unchecked?)
      :int16 `(int16->reduce-op ~item ~unchecked?)
      :int32 `(int32->reduce-op ~item ~unchecked?)
      :int64 `(int64->reduce-op ~item ~unchecked?)
      :float32 `(float32->reduce-op ~item ~unchecked?)
      :float64 `(float64->reduce-op ~item ~unchecked?)
      :boolean `(boolean->reduce-op ~item ~unchecked?)
      :object `(object->reduce-op ~item ~unchecked?))))


(defmacro make-marshalling-reduce-op-impl
  [dst-datatype src-datatype]
  (let [host-datatype (casting/safe-flatten dst-datatype)
        src-host-datatype (casting/safe-flatten src-datatype)]
    `(fn [un-op# datatype# unchecked?#]
       (let [src-op# (datatype->reduce-op ~src-host-datatype un-op# unchecked?#)
             op-name# (dtype-base/op-name src-op#)]
         (if unchecked?#
           (reify ~(datatype->reduce-op-type host-datatype)
             (getDatatype [item#] datatype#)
             (update [item# accum# next#]
               (let [value# (.update src-op#
                                     (nio-access/unchecked-full-cast accum#
                                                                     ~host-datatype
                                                                     ~dst-datatype
                                                                     ~src-datatype)
                                     (nio-access/unchecked-full-cast next#
                                                                     ~host-datatype
                                                                     ~dst-datatype
                                                                     ~src-datatype))]
                 (nio-access/unchecked-full-cast value#
                                                 ~src-host-datatype
                                                 ~src-datatype
                                                 ~dst-datatype)))

             (finalize [item# accum# num-elems#]
               (let [value# (.finalize src-op#
                                     (nio-access/unchecked-full-cast accum#
                                                                     ~host-datatype
                                                                     ~dst-datatype
                                                                     ~src-datatype)
                                     num-elems#)]
                 (nio-access/unchecked-full-cast value#
                                                 ~src-host-datatype
                                                 ~src-datatype
                                                 ~dst-datatype)))
             IFn
             (invoke [item# accum# next#]
               (.update item#
                        (casting/datatype->cast-fn
                         :unknown ~dst-datatype accum#)
                        (casting/datatype->cast-fn
                         :unknown ~dst-datatype next#)))
             dtype-proto/POperator
             (op-name [item#] op-name#))
           (reify ~(datatype->reduce-op-type host-datatype)
             (getDatatype [item#] ~dst-datatype)
             (update [item# accum# next#]
               (let [value# (.update src-op#
                                     (nio-access/checked-full-write-cast
                                      accum#
                                      ~host-datatype
                                      ~dst-datatype
                                      ~src-datatype)
                                     (nio-access/checked-full-write-cast
                                      next#
                                      ~host-datatype
                                      ~dst-datatype
                                      ~src-datatype))]
                 (nio-access/checked-full-write-cast value#
                                                     ~src-host-datatype
                                                     ~src-datatype
                                                     ~dst-datatype)))
             (finalize [item# accum# num-elems#]
               (let [value# (.finalize src-op#
                                       (nio-access/checked-full-write-cast
                                        accum#
                                        ~host-datatype
                                        ~dst-datatype
                                        ~src-datatype)
                                       num-elems#)]
                 (nio-access/checked-full-write-cast value#
                                                     ~src-host-datatype
                                                     ~src-datatype
                                                     ~dst-datatype)))
             IFn
             (invoke [item# accum# next#]
               (.update item#
                        (casting/datatype->cast-fn
                         :unknown ~dst-datatype accum#)
                        (casting/datatype->cast-fn
                         :unknown ~dst-datatype next#)))
             dtype-proto/POperator
             (op-name [item#] op-name#)))))))


(defmacro make-marshalling-reduce-table
  []
  `(->> [~@(for [src-dtype casting/base-host-datatypes
                 dst-dtype casting/base-host-datatypes]
             [[src-dtype dst-dtype] `(make-marshalling-reduce-op-impl
                                      ~dst-dtype ~src-dtype)])]
        (into {})))


(def marshalling-reduce-op-table (make-marshalling-reduce-table))


(defmacro extend-reduce-op
  [datatype]
  `(clojure.core/extend
       ~(datatype->reduce-op-type datatype)
     dtype-proto/PToReduceOp
     {:->reduce-op
      (fn [item# datatype# unchecked?#]
        (if (= (casting/safe-flatten datatype#)
               ~datatype)
          item#
          (let [marshal-fn# (get marshalling-reduce-op-table
                                 [~datatype (casting/safe-flatten
                                             datatype#)])]
            (marshal-fn# item# datatype# unchecked?#))))}))


(extend-reduce-op :int8)
(extend-reduce-op :int16)
(extend-reduce-op :int32)
(extend-reduce-op :int64)
(extend-reduce-op :float32)
(extend-reduce-op :float64)
(extend-reduce-op :boolean)
(extend-reduce-op :object)


(defmacro binary-op->reduce-op
  [datatype]
  `(fn [bin-op# unchecked?#]
     (let [actual-op# (binary-op/datatype->binary-op ~datatype bin-op# unchecked?#)]
       (reify
         ~(datatype->reduce-op-type datatype)
         (getDatatype [item#] (.getDatatype actual-op#))
         (update [item# accum# next#] (.op actual-op# accum# next#))
         (finalize [item# accum# num-elems#] accum#)
         IFn
         (invoke [item# accum# next#] (actual-op# accum# next#))))))

(defmacro make-binary-op->reduce-op-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(binary-op->reduce-op ~dtype)])]
        (into {})))


(def binary-op->reduce-op-table (make-binary-op->reduce-op-table))


(defmacro extend-binary-op
  [datatype]
  `(clojure.core/extend
       ~(binary-op/datatype->binary-op-type datatype)
     dtype-proto/PToReduceOp
     {:->reduce-op (fn [item# dest-dtype# unchecked?#]
                     (let [binop-fn# (get binary-op->reduce-op-table
                                          (casting/safe-flatten ~datatype))]
                       (-> (binop-fn# item# unchecked?#)
                           (dtype-proto/->reduce-op dest-dtype# unchecked?#))))}))


(extend-binary-op :int8)
(extend-binary-op :int16)
(extend-binary-op :int32)
(extend-binary-op :int64)
(extend-binary-op :float32)
(extend-binary-op :float64)
(extend-binary-op :boolean)
(extend-binary-op :object)


(defmacro make-reduce-op
  "Make a reduce op of type datatype.  Arguments to the operation
  are exposed to the local scope as 'x' and 'y' respectively.
  (make-reduce-op :float32 (Math/pow x y))"
  ([opname datatype update finalize]
   `(reify ~(datatype->reduce-op-type datatype)
      (getDatatype [item#] ~datatype)
      (update [item# ~'accum ~'next]
        ~update)
      (finalize [item# ~'accum ~'num-elems]
        ~(if finalize
           `~finalize
           `~'accum))
      IFn
      (invoke [item# accum# next#]
        (.update item#
                 (casting/datatype->cast-fn :unknown ~datatype accum#)
                 (casting/datatype->cast-fn :unknown ~datatype next#)))
      dtype-proto/POperator
      (op-name [item] ~opname)))
  ([datatype update finalize]
   `(make-reduce-op :unnamed ~datatype ~update ~finalize))
  ([datatype update]
   `(make-reduce-up ~datatype ~update nil)))


(defmacro make-iterable-reduce-fn
  [datatype]
  `(fn [reduce-op# iterable# unchecked?#]
     (let [reduce-op# (datatype->reduce-op ~datatype reduce-op# unchecked?#)
           iter# (typecast/datatype->iter ~datatype iterable# unchecked?#)]
       (loop [accum# (casting/datatype->sparse-value ~datatype)
              next?# (.hasNext iter#)
              item-count# (int 0)]
         (if next?#
           (if (= item-count# 0)
             (recur (typecast/datatype->iter-next-fn ~datatype iter#)
                    (.hasNext iter#) 1)
             (recur (.update reduce-op# accum# (.next iter#))
                    (.hasNext iter#)
                    (inc item-count#)))
           (.finalize reduce-op# accum# item-count#))))))


(defmacro make-iterable-reduce-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-iterable-reduce-fn ~dtype)])]
        (into {})))


(def iterable-reduce-table (make-iterable-reduce-table))


(defmulti iterable-reduce
  (fn [options reduce-op values]
    (dtype-base/buffer-type values)))


(defn default-iterable-reduce
  [{:keys [datatype unchecked?] :as options} reduce-op values]
  (let [datatype (or datatype (dtype-base/get-datatype values))
        reduce-fn (get iterable-reduce-table (casting/safe-flatten datatype))]
    (reduce-fn reduce-op values unchecked?)))


(defmethod iterable-reduce :default
  [options reduce-op values]
  (default-iterable-reduce options reduce-op values))


(defn apply-reduce-op
  "Reduce an iterable into one thing.  This is not currently parallelized."
  [{:keys [datatype unchecked?] :as options} reduce-op values]
  (let [datatype (or datatype (dtype-base/get-datatype values))]
    (if (= (argtypes/arg->arg-type values) :scalar)
      (case (casting/safe-flatten datatype)
        :int8 (.finalize (datatype->reduce-op :int8 reduce-op unchecked?)
                         values 1)
        :int16 (.finalize (datatype->reduce-op :int16 reduce-op unchecked?)
                          values 1)
        :int32 (.finalize (datatype->reduce-op :int32 reduce-op unchecked?)
                          values 1)
        :int64 (.finalize (datatype->reduce-op :int64 reduce-op unchecked?)
                          values 1)
        :float32 (.finalize (datatype->reduce-op :float32 reduce-op unchecked?)
                            values 1)
        :float64 (.finalize (datatype->reduce-op :float64 reduce-op unchecked?)
                            values 1)
        :boolean (.finalize (datatype->reduce-op :boolean reduce-op unchecked?)
                            values 1)
        :object (.finalize (datatype->reduce-op :object reduce-op unchecked?)
                           values 1))
      (iterable-reduce options reduce-op values))))


(defn dot-product
  "apply a binary operation across all iterables and then apply the reduce
  operation to the result.
  dot-op - binary operation applied across all items.
  reduce-op - Reduction operation applied at the end."
  [{:keys [datatype unchecked?] :as options}
   dot-op reduce-op lhs-iterable rhs-iterable & args]
  (->> (apply binary-op/apply-binary-op options dot-op
              lhs-iterable rhs-iterable args)
       (apply-reduce-op options reduce-op)))
