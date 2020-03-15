(ns tech.v2.datatype.functional
  (:require [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.binary-op :as binary-op]
            [tech.v2.datatype.reduce-op :as reduce-op]
            [tech.v2.datatype.functional.impl :as impl]
            [tech.v2.datatype.operation-provider :as op-provider]
            [tech.v2.datatype.readers.indexed :as indexed-reader]
            [tech.v2.datatype.base :as dtype-base]
            ;;For functional to work right a lot of the requires in datatype
            ;;need to be working
            [tech.v2.datatype.array]
            [tech.v2.datatype.nio-buffer]
            [tech.v2.datatype.typed-buffer]
            [tech.v2.datatype.jna]
            [tech.v2.datatype.list]
            [tech.v2.datatype.clj-range]
            [tech.v2.datatype.object-datatypes]
            [tech.v2.datatype.builtin-op-providers])
  (:import [java.util Iterator])
  (:refer-clojure :exclude [+ - / *
                            <= < >= >
                            identity
                            min max
                            bit-xor bit-and bit-and-not bit-not bit-set bit-test
                            bit-or bit-flip bit-clear
                            bit-shift-left bit-shift-right unsigned-bit-shift-right
                            quot rem cast not and or]))


(def all-builtins (impl/define-all-builtins))


(impl/export-symbols tech.v2.datatype.unary-op
                     unary-iterable-map
                     unary-reader-map)


(impl/export-symbols tech.v2.datatype.binary-op
                     binary-iterable-map
                     binary-reader-map)


(impl/export-symbols tech.v2.datatype.boolean-op
                     boolean-unary-iterable-map
                     boolean-unary-reader-map
                     boolean-binary-iterable-map
                     boolean-binary-reader-map)


(impl/export-symbols tech.v2.datatype.functional.impl
                     argmin argmin-last
                     argmax argmax-last
                     argcompare
                     argcompare-last
                     apply-reduce-op
                     apply-unary-op
                     apply-binary-op
                     apply-unary-boolean-op
                     apply-binary-boolean-op)


(impl/export-symbols tech.v2.datatype.statistics
                     supported-descriptive-stats
                     descriptive-stats
                     geometric-mean
                     harmonic-mean
                     kendalls-correlation
                     kurtosis
                     kurtosis-population
                     mean
                     median
                     pearsons-correlation
                     percentile
                     variance-population
                     product
                     quadratic-mean
                     quartile-1
                     quartile-3
                     quartile-outlier-fn
                     quartiles
                     skew
                     skewness
                     skewness-population
                     spearmans-correlation
                     standard-deviation
                     standard-deviation-population
                     standard-error
                     sum
                     sum-of-logs
                     sum-of-squares
                     variance
                     variance-population)


(impl/export-symbols tech.v2.datatype.rolling
                     fixed-rolling-window)


(defn indexed-reader
  [indexes data & {:as options}]
  (indexed-reader/make-indexed-reader indexes data options))


(defmacro def-provider-fn
  [fn-name docstring first-arg & args]
  (let [opname (keyword (name fn-name))]
    `(defn ~fn-name ~docstring
       [~first-arg ~@args]
       (op-provider/apply-fn (op-provider/unary-provider ~first-arg)
                             ~opname
                             ~first-arg
                             [~@args]))))


(defn argsort
  "Return a list of indexes in sorted-values order.  Values must be
  convertible to a reader.  Sorts least-to-greatest by default
  unless either reverse? is specified or a correctly typed comparator
  is provided.
  Returns an int32 array or indexes."
  [values & {:keys [parallel?]
             :or {parallel? true}
             :as options}]
  (let [options (impl/default-options values (assoc options :parallel? parallel?))]
    (op-provider/unary-op :argsort values options)))


(defn binary-search
  "Perform a binary search of (convertible to reader) values for target and return a
  tuple of [found? elem-pos-or-insert-pos].  If the element is found, the elem-pos
  contains the index.  If the element is not found, then it contains the index where
  the element would be inserted to maintain sort order of the values."
  [values target & {:as options}]
  (let [options (impl/default-options values options)
        datatype (clojure.core/or (:datatype options)
                                  (dtype-base/get-datatype target))
        options (assoc options :datatype datatype)]
    (op-provider/binary-op :binary-search values target options)))


(defn argfilter
  "Returns a (potentially infinite) sequence of indexes that pass the filter."
  [bool-op filter-seq & [second-seq]]
  (if second-seq
    (op-provider/binary-op :argfilter filter-seq second-seq bool-op)
    (op-provider/unary-op :argfilter filter-seq bool-op)))


(defn arggroup-by
  [partition-fn item-reader & [options]]
  (op-provider/unary-op :arggroup-by item-reader [partition-fn options]))


(defn arggroup-by-int
  [partition-fn item-reader & [options]]
  (op-provider/unary-op :arggroup-by-int item-reader [partition-fn options]))


(defn arggroup-by-bitmap
  [partition-fn item-reader & [options]]
  (op-provider/unary-op :arggroup-by-bitmap item-reader [partition-fn options]))


(defn- do-argpartition-by
  [^long start-idx ^Iterator item-iterable first-item]
  (let [[end-idx next-item]
        (loop [cur-idx start-idx]
          (let [has-next? (.hasNext item-iterable)
                next-item (when has-next? (.next item-iterable))]
            (if (clojure.core/and has-next?
                                  (= first-item next-item))
              (recur (inc cur-idx))
              [cur-idx next-item])))
        end-idx (inc (long end-idx))]
    (cons [first-item (range (long start-idx) end-idx)]
          (when (.hasNext item-iterable)
            (lazy-seq (do-argpartition-by end-idx item-iterable next-item))))))


(defn argpartition-by
  "Returns a sequence of [partition-key index-reader].  Index generation is not
  parallelized.  This design allows group-by and partition-by to be used
  interchangeably as they both result in a sequence of [partition-key idx-reader].
  This design is lazy."
  [partition-fn item-iterable & [options]]
  (let [reader-dtype (clojure.core/or (:datatype options) :object)
        item-reader (->> (dtype-base/->iterable item-iterable
                                                reader-dtype
                                                (assoc options :datatype reader-dtype))
                         (unary-op/unary-map partition-fn))
        iterator (.iterator ^Iterable item-reader)]
    (when (.hasNext iterator)
      (do-argpartition-by 0 iterator (.next iterator)))))


(defn magnitude-squared
  [item & [options]]
  (->> (unary-op/unary-map options (:sq unary-op/builtin-unary-ops) item)
       (reduce-op/commutative-reader-reduce options
                                            (:+ binary-op/builtin-binary-ops))))


(defn magnitude
  (^double [item options]
   (Math/sqrt (double (magnitude-squared item options))))
  (^double [item]
   (Math/sqrt (double (magnitude-squared item nil)))))


(defn dot-product
  ([lhs rhs bin-op reduce-op options]
   (reduce-op/dot-product options lhs rhs bin-op reduce-op))
  ([lhs rhs bin-op reduce-op]
   (reduce-op/dot-product nil lhs rhs bin-op reduce-op))
  ([lhs rhs]
   (reduce-op/dot-product nil lhs rhs
                          (:* binary-op/builtin-binary-ops)
                          (:+ binary-op/builtin-binary-ops))))

(defn distance-squared
  [lhs rhs]
  (magnitude-squared (- lhs rhs)))


(defn distance
  [lhs rhs]
  (magnitude (- lhs rhs)))


(defn equals
  [lhs rhs & [error-bar]]
  (clojure.core/< (double (distance lhs rhs))
                  (double (clojure.core/or error-bar 0.001))))
