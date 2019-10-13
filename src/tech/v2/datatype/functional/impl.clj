(ns tech.v2.datatype.functional.impl
  (:require [tech.v2.datatype.unary-op
             :refer [datatype->unary-op]
             :as unary]
            [tech.v2.datatype.binary-op
             :refer [datatype->binary-op]
             :as binary]
            [tech.v2.datatype.reduce-op
             :as reduce-op]
            [tech.v2.datatype.boolean-op
             :refer [datatype->boolean-unary
                     datatype->boolean-binary
                     boolean-unary-iterable-map
                     boolean-binary-iterable-map
                     boolean-unary-reader-map
                     boolean-binary-reader-map]
             :as boolean-op]
            [tech.v2.datatype.argtypes :as argtypes]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.readers.range :as reader-range]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.array]
            [tech.v2.datatype.list]
            [tech.v2.datatype.primitive]
            [tech.v2.datatype.sparse.reader :as sparse-reader]
            [tech.v2.datatype.comparator :as comparator]
            [tech.parallel.utils :as parallel-utils])
  (:import [java.util List RandomAccess]))

(def ^:dynamic *datatype* nil)
(def ^:dynamic *unchecked?* nil)


(defmacro export-symbols
  [src-ns & symbol-list]
  `(parallel-utils/export-symbols ~src-ns ~@symbol-list))


(defn default-options
  [options]
  (merge options
         (when *datatype*
           {:datatype *datatype*})
         (when *unchecked?*
           {:unchecked? *unchecked?*})))


(defn apply-reduce-op
  "Reduce an iterable into one thing.  This is not currently parallelized."
  [{:keys [datatype unchecked?] :as options} reduce-op values]
  (let [datatype (or datatype (dtype-base/get-datatype values))]
    (if (= (argtypes/arg->arg-type values) :scalar)
      (case (casting/safe-flatten datatype)
        :int8 (.finalize (datatype->binary-op :int8 reduce-op unchecked?)
                         values 1)
        :int16 (.finalize (datatype->binary-op :int16 reduce-op unchecked?)
                          values 1)
        :int32 (.finalize (datatype->binary-op :int32 reduce-op unchecked?)
                          values 1)
        :int64 (.finalize (datatype->binary-op :int64 reduce-op unchecked?)
                          values 1)
        :float32 (.finalize (datatype->binary-op :float32 reduce-op unchecked?)
                            values 1)
        :float64 (.finalize (datatype->binary-op :float64 reduce-op unchecked?)
                            values 1)
        :boolean (.finalize (datatype->binary-op :boolean reduce-op unchecked?)
                            values 1)
        :object (.finalize (datatype->binary-op :object reduce-op unchecked?)
                           values 1))
      (reduce-op/iterable-reduce-map options reduce-op values))))


(defn apply-unary-op
    "Perform operation returning a scalar, reader, or an iterator.  Note that the
  results of this could be a reader, iterable or a scalar depending on what was passed
  in.  Also note that the results are lazyily calculated so no computation is done in
  this method aside from building the next thing *unless* the inputs are scalar in which
  case the operation is evaluated immediately."
  [{:keys [datatype unchecked?] :as options} un-op arg]
  (case (argtypes/arg->arg-type arg)
    :reader
    (unary/unary-reader-map options un-op arg)
    :iterable
    (unary/unary-iterable-map options un-op arg)
    :scalar
    (let [datatype (or datatype (dtype-base/get-datatype arg))]
      (if (= :identity (dtype-proto/op-name un-op))
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
  [dtype-seq]
  (reduce (fn [existing dtype]
            (if (= existing dtype)
              existing
              (let [exist-rank (datatype-width existing)
                    new-rank (datatype-width dtype)]
                (cond
                  (< exist-rank new-rank)
                  existing
                  (= exist-rank new-rank)
                  (next-integer-type existing dtype)
                  :else
                  dtype))))
          dtype-seq))


(defn apply-binary-op
  "We perform a left-to-right reduction making scalars/readers/etc.  This matches
  clojure semantics.  Note that the results of this could be a reader, iterable or a
  scalar depending on what was passed in.  Also note that the results are lazily
  calculated so no computation is done in this method aside from building the next
  thing *unless* the inputs are scalar in which case the operation is evaluated
  immediately."
  [{:keys [datatype unchecked?] :as options}
   bin-op arg1 arg2 & args]
  (let [all-args (concat [arg1 arg2] args)
        all-arg-types (->> all-args
                           (map argtypes/arg->arg-type)
                           set)
        op-arg-type (cond
                      (all-arg-types :iterable)
                      :iterable
                      (all-arg-types :reader)
                      :reader
                      :else
                      :scalar)
        datatype (or datatype (widest-datatype
                               (map dtype-base/get-datatype
                                    (concat [arg1 arg2] args))))]
    (loop [arg1 arg1
           arg2 arg2
           args args]
      (let [arg1-type (argtypes/arg->arg-type arg1)
            arg2-type (argtypes/arg->arg-type arg2)
            op-map-fn (case op-arg-type
                        :iterable
                        (partial binary/binary-iterable-map
                                 (assoc options :datatype datatype)
                                 bin-op)
                        :reader
                        (partial binary/binary-reader-map
                                 (assoc options :datatype datatype)
                                 bin-op)
                        :scalar
                        nil)
            un-map-fn (case op-arg-type
                        :iterable
                        (partial unary/unary-iterable-map
                                 (assoc options :datatype datatype))
                        :reader
                        (partial unary/unary-reader-map
                                 (assoc options :datatype datatype))
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
              (un-map-fn (binary/binary->unary {:datatype datatype
                                                :left-associate? true}
                                               bin-op arg1)
                         arg2)
              (= arg2-type :scalar)
              (un-map-fn (binary/binary->unary {:datatype datatype}
                                               bin-op arg2)
                         arg1)
              :else
              (op-map-fn arg1 arg2))]
        (if (first args)
          (recur arg-result (first args) (rest args))
          arg-result)))))


(defn apply-unary-boolean-op
    "Perform operation returning a scalar, reader, or an iterator.  Note that the
  results of this could be a reader, iterable or a scalar depending on what was passed
  in.  Also note that the results are lazyily calculated so no computation is done in
  this method aside from building the next thing *unless* the inputs are scalar in which
  case the operation is evaluated immediately."
  [{:keys [datatype unchecked?] :as options} un-op arg]
  (case (argtypes/arg->arg-type arg)
    :reader
    (boolean-unary-reader-map options un-op arg)
    :iterable
    (boolean-unary-iterable-map options un-op arg)
    :scalar
    (let [datatype (or datatype (dtype-base/get-datatype arg))]
      (if (= :no-op un-op)
        (if unchecked?
          (casting/unchecked-cast arg datatype)
          (casting/cast arg datatype)))
      (case (casting/safe-flatten datatype)
        :int8 (.op (datatype->boolean-unary :int8 un-op unchecked?) arg)
        :int16 (.op (datatype->boolean-unary :int16 un-op unchecked?) arg)
        :int32 (.op (datatype->boolean-unary :int32 un-op unchecked?) arg)
        :int64 (.op (datatype->boolean-unary :int64 un-op unchecked?) arg)
        :float32 (.op (datatype->boolean-unary :float32 un-op unchecked?) arg)
        :float64 (.op (datatype->boolean-unary :float64 un-op unchecked?) arg)
        :boolean (.op (datatype->boolean-unary :boolean un-op unchecked?) arg)
        :object (.op (datatype->boolean-unary :object un-op unchecked?) arg)))))


(defn apply-binary-boolean-op
  "We perform a left-to-right reduction making scalars/readers/etc.  This matches
  clojure semantics.  Note that the results of this could be a reader, iterable or a
  scalar depending on what was passed in.  Also note that the results are lazily
  calculated so no computation is done in this method aside from building the next thing
  *unless* the inputs are scalar in which case the operation is evaluated immediately."
  [{:keys [datatype unchecked?] :as options}
   bin-op arg1 arg2 & args]
  (let [all-args (concat [arg1 arg2] args)
        all-arg-types (->> all-args
                           (map argtypes/arg->arg-type)
                           set)
        op-arg-type (cond
                      (all-arg-types :iterable)
                      :iterable
                      (all-arg-types :reader)
                      :reader
                      :else
                      :scalar)
        datatype (or datatype (dtype-base/get-datatype arg1))]
    (loop [arg1 arg1
           arg2 arg2
           args args]
      (let [arg1-type (argtypes/arg->arg-type arg1)
            arg2-type (argtypes/arg->arg-type arg2)
            op-map-fn (case op-arg-type
                        :iterable
                        (partial boolean-binary-iterable-map
                                 (assoc options :datatype datatype)
                                 bin-op)
                        :reader
                        (partial boolean-binary-reader-map
                                 (assoc options :datatype datatype)
                                 bin-op)
                        :scalar
                        nil)
            arg-result
            (cond
              (and (= arg1-type :scalar)
                   (= arg2-type :scalar))
              (case (casting/safe-flatten datatype)
                :int8 (.op (datatype->boolean-binary :int8 bin-op unchecked?)
                           arg1 arg2)
                :int16 (.op (datatype->boolean-binary :int16 bin-op unchecked?)
                            arg1 arg2)
                :int32 (.op (datatype->boolean-binary :int32 bin-op unchecked?)
                            arg1 arg2)
                :int64 (.op (datatype->boolean-binary :int64 bin-op unchecked?)
                            arg1 arg2)
                :float32 (.op (datatype->boolean-binary :float32 bin-op unchecked?)
                              arg1 arg2)
                :float64 (.op (datatype->boolean-binary :float64 bin-op unchecked?)
                              arg1 arg2)
                :boolean (.op (datatype->boolean-binary :boolean bin-op unchecked?)
                              arg1 arg2)
                :object (.op (datatype->boolean-binary :object bin-op unchecked?)
                             arg1 arg2))
              (= arg1-type :scalar)
              (op-map-fn (sparse-reader/const-sparse-reader arg1 datatype) arg2)
              (= arg2-type :scalar)
              (op-map-fn arg1 (sparse-reader/const-sparse-reader arg2 datatype))
              :else
              (op-map-fn arg1 arg2))]
        (if (first args)
          (recur arg-result (first args) (rest args))
          arg-result)))))


(def all-builtins (->> (concat (->> unary/builtin-unary-ops
                                    (map (fn [[k v]]
                                           {:name k
                                            :type :unary
                                            :operator v})))
                               (->> binary/builtin-binary-ops
                                    (map (fn [[k v]]
                                           {:name k
                                            :type :binary
                                            :operator v})))
                               (->> boolean-op/builtin-boolean-unary-ops
                                    (map (fn [[k v]]
                                           {:name k
                                            :type :boolean-unary
                                            :operator v})))
                               (->> boolean-op/builtin-boolean-binary-ops
                                    (map (fn [[k v]]
                                           {:name k
                                            :type :boolean-binary
                                            :operator v}))))
                       (group-by :name)))


(defn- safe-name
  [item]
  (let [item-name (name item)]
    (if (= item-name "/")
      "div"
      item-name)))


(defmacro def-builtin-operator
  [op-name op-seq]
  (let [op-types (->> (map :type op-seq)
                      set)
        op-name-symbol (symbol (name op-name))
        argnum-types (->> op-types
                          (map {:unary :unary
                                :boolean-unary :unary
                                :binary :binary
                                :boolean-binary :binary})
                          set)]
    `(do
       (defn ~op-name-symbol
         ~(str "Operator " (name op-name) ":" (vec op-types) "." )
         [& ~'args]
         (let [~'n-args (count ~'args)]
           ~(cond
              (= argnum-types #{:unary :binary})
              `(when-not (> ~'n-args 0)
                 (throw (ex-info (format "Operator called with too few (%s) arguments."
                                         ~'n-args)
                                 {})))
              (= argnum-types #{:unary})
              `(when-not (= ~'n-args 1)
                 (throw (ex-info (format "Operator takes 1 argument, (%s) given."
                                         ~'n-args)
                                 {})))
              (= argnum-types #{:binary})
              `(when-not (> ~'n-args 0)
                 (throw (ex-info (format "Operator called with too few (%s) arguments"
                                         ~'n-args)
                                 {})))
              :else
              (throw (ex-info "Incorrect op types" {:types argnum-types
                                                    :op-types op-types})))
           (let [~'datatype (or *datatype*
                                (widest-datatype
                                 (map
                                  dtype-base/get-datatype
                                  ~'args)))
                 ~'options {:datatype ~'datatype
                            :unchecked? *unchecked?*}]
             (if (= ~'n-args 1)
               ~(cond
                  (contains? op-types :boolean-unary)
                  `(apply-unary-boolean-op
                    ~'options
                    (get boolean-op/builtin-boolean-unary-ops
                         ~op-name)
                    (first ~'args))
                  :else
                  `(if-let [un-op# (get unary/builtin-unary-ops ~op-name)]
                     (apply-unary-op
                      ~'options
                      un-op#
                      (first ~'args))
                     (throw (ex-info
                             (format "No unary operator defined for operand %s"
                                     ~op-name)
                             {}))))
               ~(if (contains? op-types :boolean-binary)
                  `(apply apply-binary-boolean-op
                          ~'options
                          (get boolean-op/builtin-boolean-binary-ops
                               ~op-name)
                          ~'args)
                  `(apply apply-binary-op
                          ~'options
                          (get binary/builtin-binary-ops ~op-name)
                          ~'args))))))
       ~(when (contains? argnum-types :binary)
          (let [op-name-symbol (symbol (str "reduce-" (safe-name op-name)))]
            `(defn ~op-name-symbol
               ~(str "Operator reduce-" (name op-name)"." )
               [& ~'args]
               (let [~'n-args (count ~'args)]
                 (when-not (> ~'n-args 0)
                   (throw (ex-info
                           (format "Operator called with too few (%s) arguments."
                                   ~'n-args)
                           {})))
                 (let [~'datatype (or *datatype*
                                      (widest-datatype
                                       (map
                                        dtype-base/get-datatype
                                        ~'args)))
                       ~'options {:datatype ~'datatype
                                  :unchecked? *unchecked?*}]
                   (if (= ~'n-args 1)
                     (apply-reduce-op
                      ~'options
                      (get binary/builtin-binary-ops
                           ~op-name)
                      (first ~'args))
                     (~op-name-symbol
                      (mapv ~op-name-symbol ~'args)))))))))))


(defmacro define-all-builtins
  []
  `(do
     ~@(->> all-builtins
            (map (fn [[op-name op-seq]]
                   `(def-builtin-operator ~op-name ~op-seq))))))


(defn- ->list
  ^List [item]
  (when-not (instance? List item)
    (throw (ex-info "Item is not a list." {})))
  item)


(defmacro impl-arg-op
  [datatype op]
  (if (= datatype :object)
    `(fn [values#]
       (if (instance? RandomAccess values#)
         (let [values# (->list values#)
               n-elems# (.size values#)]
           (reduce (fn [lhs# rhs#]
                     (if (~op
                          (.get values# lhs#)
                          (.get values# rhs#))
                       lhs#
                       rhs#))
                   (range n-elems#)))
         (let [value-reader# (typecast/datatype->reader ~datatype values#)
               n-elems# (.size value-reader#)]
           (reduce-op/iterable-reduce
            :int32
            (if (~op
                 (.read value-reader# ~'accum)
                 (.read value-reader# ~'next))
              ~'accum
              ~'next)
            (reader-range/reader-range :int32 0 n-elems#)))))
    `(fn [values#]
       (let [value-reader# (typecast/datatype->reader ~datatype values#)
             n-elems# (.size value-reader#)]
         (reduce-op/iterable-reduce
          :int32
          (if (~op
               (.read value-reader# ~'accum)
               (.read value-reader# ~'next))
            ~'accum
            ~'next)
          (reader-range/reader-range :int32 0 n-elems#))))))


(defmacro make-no-boolean-macro-table
  [sub-macro]
  `(->> [~@(for [dtype (concat casting/host-numeric-types
                               [:object])]
             [dtype `(~sub-macro ~dtype)])]
        (into {})))

(defmacro make-maxarg
  [datatype]
  `(impl-arg-op ~datatype >=))

(def maxarg-table (make-no-boolean-macro-table make-maxarg))

(defn argmax
  "Return index of first maximal value"
  ([{:keys [datatype]} values]
   (let [datatype (or datatype (dtype-base/get-datatype values))
         maxarg-fn (get maxarg-table (casting/safe-flatten datatype))]
     (maxarg-fn values)))
  ([values]
   (argmax {} values)))

(defmacro make-minarg
  [datatype]
  `(impl-arg-op ~datatype <=))

(def minarg-table (make-no-boolean-macro-table make-minarg))

(defn argmin
  "Return index of first minimal value"
  ([{:keys [datatype]} values]
   (let [datatype (or datatype (dtype-base/get-datatype values))
         minarg-fn (get minarg-table (casting/safe-flatten datatype))]
     (minarg-fn values)))
  ([values]
   (argmin {} values)))

(defmacro make-last-maxarg
  [datatype]
  `(impl-arg-op ~datatype >))

(def last-maxarg-table (make-no-boolean-macro-table make-last-maxarg))

(defn argmax-last
  "Return index of last maximal value"
  ([{:keys [datatype]} values]
   (let [datatype (or datatype (dtype-base/get-datatype values))
         maxarg-fn (get last-maxarg-table (casting/safe-flatten datatype))]
     (maxarg-fn values)))
  ([values]
   (argmax-last {}  values)))

(defmacro make-last-minarg
  [datatype]
  `(impl-arg-op ~datatype <))

(def last-minarg-table (make-no-boolean-macro-table make-last-minarg))


(defn argmin-last
  "Return index of first minimal value"
  ([{:keys [datatype]} values]
   (let [datatype (or datatype (dtype-base/get-datatype values))
         minarg-fn (get last-minarg-table (casting/safe-flatten datatype))]
     (minarg-fn values)))
  ([values]
   (argmin-last {} values)))


(defmacro impl-compare-arg-op
  [datatype]
  `(fn [values# comparator#]
     (let [value-reader# (typecast/datatype->reader ~datatype values#)
           n-elems# (.size value-reader#)
           comparator# (comparator/datatype->comparator ~datatype comparator#)]
       (reduce-op/iterable-reduce
        :int32
        (if (<= (.compare
                 comparator#
                 (.read value-reader# ~'accum)
                 (.read value-reader# ~'next))
                0)
          ~'accum
          ~'next)
        (reader-range/reader-range :int32 0 n-elems#)))))

(def compare-arg-ops (make-no-boolean-macro-table impl-compare-arg-op))

(defn argcompare
  "Given a reader of values and a comparator, return the first item where
  the comparator's value is less than zero."
  ([{:keys [datatype]} values comp-item]
   (let [datatype (or datatype (dtype-base/get-datatype values))
         compare-fn (get compare-arg-ops (casting/safe-flatten datatype))]
     (compare-fn values comp-item)))
  ([values comp-item]
   (argcompare {} values comp-item)))


(defmacro impl-compare-last-arg-op
  [datatype]
  `(fn [values# comparator#]
     (let [value-reader# (typecast/datatype->reader ~datatype values#)
           n-elems# (.size value-reader#)
           comparator# (comparator/datatype->comparator ~datatype comparator#)]
       (reduce-op/iterable-reduce
        :int32
        (if (>= (.compare
                 comparator#
                 (.read value-reader# ~'accum)
                 (.read value-reader# ~'next))
                0)
          ~'accum
          ~'next)
        (reader-range/reader-range :int32 0 n-elems#)))))

(def compare-last-arg-ops (make-no-boolean-macro-table impl-compare-last-arg-op))

(defn argcompare-last
  "Given a reader of values and a comparator, return the last item where
  the comparator's value is less than zero."
  ([{:keys [datatype]} values comp-item]
   (let [datatype (or datatype (dtype-base/get-datatype values))
         compare-fn (get compare-last-arg-ops (casting/safe-flatten datatype))]
     (compare-fn values comp-item)))
  ([values comp-item]
   (argcompare-last {} values comp-item)))
