(ns tech.v2.datatype.builtin-op-providers
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.base :as base]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.binary-search :refer [binary-search]]
            [tech.parallel.for :as parallel-for]
            [tech.v2.datatype.readers.const :refer [make-const-reader]]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.binary-op :as binary-op]
            [tech.v2.datatype.boolean-op :as boolean-op]
            [tech.v2.datatype.reduce-op :as reduce-op]
            [tech.v2.datatype.operation-provider :as op-provider]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.argsort :refer [argsort]]
            [tech.v2.datatype.bitmap :refer (->bitmap)])
  (:import [it.unimi.dsi.fastutil.longs LongArrayList]
           [it.unimi.dsi.fastutil.ints IntArrayList]
           [org.roaringbitmap RoaringBitmap]
           [java.util HashMap]
           [java.util.concurrent ConcurrentHashMap]
           [tech.v2.datatype Functions$LongTriFunction
            Functions$LongCtxBiFunction]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn widest-datatype
  ([lhs-dtype rhs-dtype]
   (if (= lhs-dtype rhs-dtype)
     lhs-dtype
     (first (filter (casting/type-path->root-set lhs-dtype)
                    (casting/type-path->root rhs-dtype)))))
  ([lhs-dtype] lhs-dtype))


(defn- get-op
  [op op-map]
  (if-let [retval (op op-map)]
    retval
    (throw (Exception. (format "Failed to find op: %s" op)))))


(defmethod op-provider/half-dispatch-unary-op :default
  [op lhs {:keys [datatype] :as options}]
  (let [op (if (keyword? op)
             (get-op op unary-op/builtin-unary-ops)
             op)
        op-space (:operation-space (meta op))
        datatype (cond-> (or datatype (base/get-datatype lhs))
                   op-space
                   (widest-datatype op-space))
        options (assoc options :datatype datatype)
        op (dtype-proto/->unary-op op options)]
    (if (= :scalar (base/operation-type lhs))
      (op lhs)
      (unary-op/unary-map options op lhs))))


(defmacro define-scalar-unary-ops
  []
  `(do
     ~@(->> (keys unary-op/builtin-unary-ops)
            (map (fn [op-name]
                   `(let [op-value# (~op-name unary-op/builtin-unary-ops)]
                      (defmethod op-provider/unary-op
                        [:scalar ~op-name]
                        [op# lhs# options#]
                        (op-value# lhs#))))))))

(define-scalar-unary-ops)


(defmethod op-provider/half-dispatch-boolean-unary-op :default
  [op lhs {:keys [datatype] :as options}]
  (let [datatype (or datatype (base/get-datatype lhs))
        options (assoc options :datatype datatype)
        op (-> (if (keyword? op)
                 (get-op op boolean-op/builtin-boolean-unary-ops)
                 op)
               (dtype-proto/->unary-boolean-op options))]
    (if (= :scalar (base/operation-type lhs))
      (op lhs)
      (boolean-op/boolean-unary-map options op lhs))))


(defmacro define-scalar-boolean-unary-ops
  []
  `(do
     ~@(->> (keys boolean-op/builtin-boolean-unary-ops)
            (map (fn [op-name]
                   `(let [op-value# (~op-name boolean-op/builtin-boolean-unary-ops)]
                      (defmethod op-provider/unary-op
                        [:scalar ~op-name]
                        [op# lhs# options#]
                        (op-value# lhs#))))))))

(define-scalar-boolean-unary-ops)


(defn op-argtype
  [lhs-arg-type rhs-arg-type]
  (cond (or (= lhs-arg-type :iterable)
            (= rhs-arg-type :iterable))
        :iterable
        (or (= lhs-arg-type :reader)
            (= rhs-arg-type :reader))
        :reader))


(defn- generalize-binary-scalars
  [lhs rhs op-datatype]
  (let [lhs-arg-type (base/operation-type lhs)
        rhs-arg-type (base/operation-type rhs)
        argtype (op-argtype lhs-arg-type rhs-arg-type)
        lhs (if (= :scalar lhs-arg-type)
              (make-const-reader lhs op-datatype
                                 (if (= argtype :reader)
                                   (base/ecount rhs)
                                   nil))
              lhs)
        rhs (if (= :scalar rhs-arg-type)
              (make-const-reader rhs op-datatype
                                 (if (= argtype :reader)
                                   (base/ecount lhs)
                                   nil))
              rhs)]
    [lhs rhs]))


(defmethod op-provider/half-dispatch-binary-op :default
  [op lhs rhs options]
  (let [op-datatype (or (:datatype options)
                        (widest-datatype (base/get-datatype lhs)
                                         (base/get-datatype rhs)))
        op (if (keyword? op)
             (get-op op binary-op/builtin-binary-ops)
             op)
        op-datatype (if-let [op-space (:operation-space (meta op))]
                      (widest-datatype op-datatype op-space)
                      op-datatype)
        options (assoc options :datatype op-datatype)

        op (-> (if (keyword? op)
                 (get-op op binary-op/builtin-binary-ops)
                 op)
               (dtype-proto/->binary-op options))
        [lhs rhs] (generalize-binary-scalars lhs rhs op-datatype)]
    (binary-op/binary-map options op lhs rhs)))


(defmacro define-scalar-builtin-binary-ops
  []
  `(do
     ~@(->> (keys binary-op/builtin-binary-ops)
            (map (fn [op-name]
                   `(let [op-value# (~op-name binary-op/builtin-binary-ops)]
                      (defmethod op-provider/binary-op
                        [:scalar :scalar ~op-name]
                        [op# lhs# rhs# options#]
                        (op-value# lhs# rhs#))))))))


(define-scalar-builtin-binary-ops)


(defmethod op-provider/half-dispatch-boolean-binary-op :default
  [op lhs rhs options]
  (let [op-datatype (or (:datatype options)
                        (widest-datatype (base/get-datatype lhs)
                                         (base/get-datatype rhs)))
        options (assoc options :datatype op-datatype)
        op (-> (if (keyword? op)
                 (get-op op boolean-op/builtin-boolean-binary-ops)
                 op)
               (dtype-proto/->binary-boolean-op options))
        [lhs rhs] (generalize-binary-scalars lhs rhs op-datatype)]
    (boolean-op/boolean-binary-map options op lhs rhs)))


(defmacro define-scalar-builtin-boolean-binary-ops
  []
  `(do
     ~@(->> (keys boolean-op/builtin-boolean-binary-ops)
            (map (fn [op-name]
                   `(let [op-value# (~op-name boolean-op/builtin-boolean-binary-ops)]
                      (defmethod op-provider/boolean-binary-op
                        [:scalar :scalar ~op-name]
                        [op# lhs# rhs# options#]
                        (op-value# lhs# rhs#))))))))


(define-scalar-builtin-boolean-binary-ops)


(def commutative-ops (set [:* :+ :rem :min :max]))

(defmethod op-provider/half-dispatch-reduce-op :default
  [op lhs {:keys [datatype commutative?] :as options}]
  (let [datatype (or datatype (base/get-datatype lhs))
        commutative? (or commutative?
                         (and (keyword? op) (commutative-ops op)))
        op (if (keyword? op)
                 (get-op op binary-op/builtin-binary-ops)
                 op)
        commutative? (or commutative? (:commutative? (meta op)))
        datatype (if-let [op-space (:operation-space (meta op))]
                   (widest-datatype datatype op-space)
                   datatype)
        options (assoc options :datatype datatype)
        op (dtype-proto/->binary-op op options)]
    (if commutative?
      (reduce-op/commutative-reader-reduce options op lhs)
      (reduce-op/iterable-reduce-map options op lhs))))


(defmacro def-unary-op
  [item-seq & body]
  `(do
     ~@(map (fn [item-pair]
              `(defmethod op-provider/unary-op
                 ~item-pair
                 ~@body))
            item-seq)))


(def-unary-op
 [[:reader :argsort]
  [:iterable :argsort]]
  [op values options]
  (argsort values options))


(def-unary-op
 [[:iterable :argfilter]
  [:reader :argfilter]]
  [op filter-seq bool-op]
  (boolean-op/unary-argfilter {}
                              bool-op
                              filter-seq))


(defmacro def-binary-op
  [item-seq & body]
  `(do
     ~@(map (fn [item-pair]
              `(defmethod op-provider/binary-op
                 ~item-pair
                 ~@body))
            item-seq)))


(defn standard-binary-op-key-seq
  [opname]
  (-> (set (for [lhs [:scalar :iterable :reader]
                 rhs [:scalar :iterable :reader]]
             [lhs rhs opname]))
      (disj [[:scalar :scalar] opname])))


(defmacro def-standard-binary-op
  [opname & body]
  `(def-binary-op
     ~(standard-binary-op-key-seq opname)
     ~@body))


(def-standard-binary-op
  :argfilter
  [op lhs rhs bool-op]
  (let [op-datatype (widest-datatype (base/get-datatype lhs)
                                     (base/get-datatype rhs))
        [lhs rhs] (generalize-binary-scalars lhs rhs op-datatype)]
    (boolean-op/binary-argfilter {:datatype op-datatype}
                                 bool-op
                                 lhs
                                 rhs)))




(defn- perform-boolean-binary-op
  [op lhs args]
  (let [op-item (if-let [bool-op (get boolean-op/builtin-boolean-binary-ops
                                      op)]
                  bool-op
                  (dtype-proto/->binary-op op {:datatype :boolean}))]
    (reduce-op/commutative-reader-reduce (merge {:datatype :boolean} args)
                                         op-item lhs)))


(defmethod op-provider/reduce-op [:scalar :and]
  [op lhs args]
  lhs)
(defmethod op-provider/reduce-op [:iterable :and]
  [op lhs args]
  (perform-boolean-binary-op op lhs args))
(defmethod op-provider/reduce-op [:reader :and]
  [op lhs args]
  (perform-boolean-binary-op op lhs args))

(defmethod op-provider/reduce-op [:scalar :or]
  [op lhs args]
  lhs)
(defmethod op-provider/reduce-op [:iterable :or]
  [op lhs args]
  (perform-boolean-binary-op op lhs args))
(defmethod op-provider/reduce-op [:reader :or]
  [op lhs args]
  (perform-boolean-binary-op op lhs args))




(defmacro dtype->storage-constructor
  [datatype]
  (case datatype
    :int32 `(IntArrayList.)
    :int64 `(LongArrayList.)
    :bitmap `(->bitmap)))

(defn cast-bitmap
  ^RoaringBitmap [item] item)

(defmacro dtype->single-add!
  [datatype existing val]
  (if (= datatype :bitmap)
    `(.add (cast-bitmap ~existing) (unchecked-int ~val))
    `(.add (typecast/datatype->list-cast-fn ~datatype ~existing)
           (casting/datatype->unchecked-cast-fn :int64 ~datatype ~val))))


(defmacro dtype->bulk-add!
  [datatype target new-vals]
  (if (= datatype :bitmap)
    `(.or (cast-bitmap ~target) (cast-bitmap ~new-vals))
    `(.addAll (typecast/datatype->list-cast-fn ~datatype ~target)
              (typecast/datatype->list-cast-fn ~datatype ~new-vals))))


(defmacro arggroup-by-impl
  [datatype partition-fn item-reader options]
  `(let [item-reader# ~item-reader
         n-elems# (base/ecount item-reader#)
         reader-dtype# (clojure.core/or (:datatype ~options) :object)
         item-reader# (typecast/datatype->reader
                       :object
                       (if (or (= ~partition-fn :identity)
                               (identical? ~partition-fn identity))
                         item-reader#
                         (let [un-op# ~partition-fn]
                           (->> (base/->reader item-reader# reader-dtype#)
                                (unary-op/unary-map un-op#)))))
         result# (ConcurrentHashMap.)]
    (parallel-for/indexed-map-reduce
     n-elems#
     (fn [offset# n-indexes#]
       (let [offset# (long offset#)
             n-indexes# (long n-indexes#)
             end-offset# (+ offset# n-indexes#)
             inner-fn#
             (reify
               Functions$LongTriFunction
               (apply [this# idx# key# value#]
                 (let [value# (or value# (dtype->storage-constructor ~datatype))]
                   (dtype->single-add! ~datatype value# idx#)
                   value#)))
             compute-fn# (Functions$LongCtxBiFunction. inner-fn#)]
         (loop [idx# offset#]
           (when (< idx# end-offset#)
             (.setContext compute-fn# idx#)
             (.compute result# (.read item-reader# idx#) compute-fn#)
             (recur (unchecked-inc idx#)))))))
    result#))

(defn arggroup-by-int
  "Returns a map of partitioned-items->indexes.  Index generation is parallelized.
  Specifically optimized for integer indexes."
  [partition-fn item-reader options]
  (arggroup-by-impl :int32 partition-fn item-reader options))


(defn arggroup-by-bitmap
  [partition-fn item-reader options]
  (arggroup-by-impl :bitmap partition-fn item-reader options))


(defn arggroup-by
  "Returns a map of partitioned-items->indexes.  Index generation is parallelized."
  [partition-fn item-reader & [options]]
  (arggroup-by-impl :int64 partition-fn item-reader options))



(def-unary-op
 [[:iterable :arggroup-by-int]]
  [op item-reader [partition-fn options]]
  (arggroup-by-int partition-fn
                   ;;Algorithm only works on readers; not on iterables
                   (dtype-proto/make-container
                    :java-array
                    (base/get-datatype item-reader)
                    item-reader) options))

(def-unary-op
 [[:reader :arggroup-by-int]]
  [op item-reader [partition-fn options]]
  (arggroup-by-int partition-fn item-reader options))


(def-unary-op
  [[:iterable :arggroup-by-bitmap]]
  [op item-reader [partition-fn options]]
  (arggroup-by-bitmap partition-fn
                      ;;Algorithm only works on readers; not on iterables
                      (dtype-proto/make-container
                       :java-array
                       (base/get-datatype item-reader)
                       item-reader) options))

(def-unary-op
 [[:reader :arggroup-by-bitmap]]
  [op item-reader [partition-fn options]]
  (arggroup-by-bitmap partition-fn item-reader options))




(def-unary-op
 [[:iterable :arggroup-by]]
  [op item-reader [partition-fn options]]
  (arggroup-by partition-fn
               ;;Algorithm only works on readers; not on iterables
               (dtype-proto/make-container
                :java-array
                (base/get-datatype item-reader)
                item-reader) options))

(def-unary-op
 [[:reader :arggroup-by]]
  [op item-reader [partition-fn options]]
  (arggroup-by partition-fn item-reader options))


(def-binary-op
  [[:reader :scalar :binary-search]]
  [op item-reader target {:as options}]
  (binary-search item-reader target options))
