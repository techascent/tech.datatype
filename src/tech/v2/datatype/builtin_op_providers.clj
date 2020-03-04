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
            [tech.v2.datatype.argsort :refer [argsort]])
  (:import [it.unimi.dsi.fastutil.longs LongArrayList]
           [java.util HashMap]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn- get-op
  [op op-map]
  (if-let [retval (op op-map)]
    retval
    (throw (Exception. (format "Failed to find op: %s" op)))))


(defmethod op-provider/half-dispatch-unary-op :default
  [op lhs {:keys [datatype] :as options}]
  (let [datatype (or datatype (base/get-datatype lhs))
        options (assoc options :datatype datatype)
        op (-> (if (keyword? op)
                 (get-op op unary-op/builtin-unary-ops)
                 op)
               (dtype-proto/->unary-op options))]
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


(def datatype-width
  (->> [:object :float64 [:int64 :uint64]
        :float32 [:int32 :uint32]
        [:uint16 :int16]
        [:int8 :uint8] :boolean]
       (map-indexed vector)
       (mapcat (fn [[idx entry]]
                 (if (keyword? entry)
                   [[entry idx]]
                   (map vector entry (repeat idx)))))
       (into {})))


(defn next-integer-type
  [lhs rhs]
  (case (max (casting/int-width lhs)
             (casting/int-width rhs))
    8 :int16
    16 :int32
    :int64))


(defn widest-datatype
  [lhs-dtype rhs-dtype]
  (if (= lhs-dtype rhs-dtype)
    lhs-dtype
    (let [lhs-rank (long (datatype-width lhs-dtype))
          rhs-rank (long (datatype-width rhs-dtype))]
      (cond
        (< lhs-rank rhs-rank)
        lhs-dtype
        (= lhs-rank rhs-rank)
        (next-integer-type lhs-dtype rhs-dtype)
        :else
        rhs-dtype))))


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
        options (assoc options :datatype datatype)
        commutative? (or commutative?
                         (and (keyword? op) (commutative-ops op)))
        op (-> (if (keyword? op)
                 (get-op op binary-op/builtin-binary-ops)
                 op)
               (dtype-proto/->binary-op options))]
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


(defn arggroup-by
  "Returns a map of partitioned-items->indexes.  Index generation is parallelized."
  [partition-fn item-reader & [options]]
  (let [n-elems (base/ecount item-reader)
        reader-dtype (clojure.core/or (:datatype options) :object)
        item-reader (->> (base/->reader item-reader
                                              reader-dtype
                                              (assoc options :datatype reader-dtype))
                         (unary-op/unary-map partition-fn)
                         (typecast/datatype->reader :object))
        list-fn (reify
                  java.util.function.Function
                  (apply [this _key]
                    (LongArrayList.)))
        bimap-fn (reify
                   java.util.function.BiFunction
                   (apply [this lhs rhs]
                     (.addAll ^LongArrayList lhs
                              ^LongArrayList rhs)
                     lhs))]
    (parallel-for/indexed-pmap
     (fn [idx n-indexes]
       (let [idx (long idx)
             last-index (clojure.core/+ idx (long n-indexes))
             retval (HashMap.)]
         (loop [idx idx]
           (if (clojure.core/< idx last-index)
             (let [partition-key (.read item-reader idx)
                   ^LongArrayList existing-list
                   (.computeIfAbsent retval partition-key list-fn)]
               (.add existing-list idx)
               (recur (inc idx)))))
         retval))
     n-elems
     (partial reduce (fn [^HashMap last-map ^HashMap next-map]
                       (let [entry-set (.entrySet next-map)
                             set-iter (.iterator entry-set)]
                         (loop [continue? (.hasNext set-iter)]
                           (when continue?
                             (let [^java.util.Map$Entry entry (.next set-iter)]
                               (.merge last-map
                                       (.getKey entry)
                                       (.getValue entry) bimap-fn)
                               (recur (.hasNext set-iter))))))
                       last-map)))))


(def-unary-op
 [[:iterable :arggroup-by]]
  [op item-reader [partition-fn options]]
  (arggroup-by partition-fn
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
