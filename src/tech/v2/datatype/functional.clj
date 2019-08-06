(ns tech.v2.datatype.functional
  (:require [tech.v2.datatype.unary-op :as unary]
            [tech.v2.datatype.binary-op :as binary]
            [tech.v2.datatype.reduce-op :as reduce-op]
            [tech.v2.datatype.iterator :as iterator]
            [tech.v2.datatype.functional.impl :as impl]
            [tech.v2.datatype.argsort :as argsort]
            [tech.v2.datatype.reader :as reader]
            [tech.v2.datatype.readers.indexed :as indexed-reader]
            [tech.v2.datatype.boolean-op :as boolean-op]
            [tech.v2.datatype.binary-search :as binary-search]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.sparse.reader :as sparse-reader]
            [tech.v2.datatype.statistics]
            [tech.v2.datatype.rolling])
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
                     mean
                     median
                     geometric-mean
                     harmonic-mean
                     variance
                     variance-population
                     standard-deviation
                     standard-deviation-population
                     standard-error
                     skewness
                     skewness-population
                     kurtosis
                     kurtosis-population
                     pearsons-correlation
                     spearmans-correlation
                     kendalls-correlation
                     percentile
                     quartiles
                     quartile-outlier-fn)


(impl/export-symbols tech.v2.datatype.rolling
                     fixed-rolling-window)


(defn indexed-reader
  [indexes data & {:keys [datatype unchecked?] :as options}]
  (indexed-reader/make-indexed-reader indexes data options))



(defn argsort
  "Return a list of indexes in sorted-values order.  Values must be
  convertible to a reader.  Sorts least-to-greatest by default
  unless either reverse? is specified or a correctly typed comparator
  is provided.
  Returns an int32 array or indexes."
  [values & {:keys [parallel?
                    typed-comparator
                    datatype
                    reverse?]
             :or {parallel? true}
             :as options}]
  (let [options (impl/default-options (assoc options :parallel? parallel?))]
    (argsort/argsort (sparse-reader/->reader values)
                     (impl/default-options options))))


(defn binary-search
  "Perform a binary search of (convertible to reader) values for target and return a
  tuple of [found? elem-pos-or-insert-pos].  If the element is found, the elem-pos
  contains the index.  If the element is not found, then it contains the index where the
  element would be inserted to maintain sort order of the values."
  [values target & {:as options}]
  (let [options (impl/default-options options)
        datatype (clojure.core/or (:datatype options) (dtype-base/get-datatype target))]
    (binary-search/binary-search
     (sparse-reader/->reader values datatype)
     target options)))


(defn argfilter
  "Returns a (potentially infinite) sequence of indexes that pass the filter."
  [bool-op filter-seq & [second-seq]]
  (if second-seq
    (let [bool-op (if (satisfies? dtype-proto/PToBinaryBooleanOp bool-op)
                    bool-op
                    (boolean-op/make-boolean-binary-op
                     :object (casting/datatype->cast-fn :unknown
                                                        :boolean
                                                        (bool-op x y))))]
      (boolean-op/binary-argfilter (impl/default-options {})
                                   bool-op
                                   (iterator/->iterable filter-seq)
                                   (iterator/->iterable second-seq)))
    (let [bool-op (if (satisfies? dtype-proto/PToUnaryBooleanOp bool-op)
                    bool-op
                    (boolean-op/make-boolean-unary-op
                     :object (boolean (bool-op x))))]
      (boolean-op/unary-argfilter (impl/default-options {})
                                  bool-op
                                  filter-seq))))


(defn magnitude-squared
  [item & [options]]
  (let [un-map (unary-iterable-map options (:sq unary/builtin-unary-ops) item)]
    (reduce-op/iterable-reduce-map options (:+ binary/builtin-binary-ops) un-map)))


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
                          (:* binary/builtin-binary-ops)
                          (:+ binary/builtin-binary-ops))))

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
