(ns tech.v2.datatype.statistics
  (:require [tech.v2.datatype.base
             :refer [->double-array]
             :as dtype-base]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.boolean-op :as boolean-op]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.reduce-op :as reduce-op]
            [tech.v2.datatype.binary-op :as binary-op]
            [tech.v2.datatype.array]
            [kixi.stats.core :as kixi])
  (:refer-clojure :exclude [min max])
  (:import [org.apache.commons.math3.stat.correlation
            KendallsCorrelation PearsonsCorrelation SpearmansCorrelation]
           [org.apache.commons.math3.stat.descriptive DescriptiveStatistics]
           [org.apache.commons.math3.stat.descriptive.rank Median]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn- desc-stats-sum-of-squares
  ^double [^DescriptiveStatistics stats]
  (->> (.getSortedValues stats)
       (unary-op/unary-reader :float64 (* x x))
       (reduce-op/iterable-reduce :float64 (+ accum next))))


(defn- desc-stats-sum-of-logs
  ^double [^DescriptiveStatistics stats]
  (->> (.getSortedValues stats)
       (unary-op/unary-reader :float64 (Math/log x))
       (reduce-op/commutative-reduce :float64 (+ accum next))))


(defn- desc-stats-product
  ^double [^DescriptiveStatistics stats]
  (->> (.getSortedValues stats)
       (reduce-op/commutative-reduce :float64 (* accum next))))


(def supported-stats-map
  {:mean #(.getMean ^DescriptiveStatistics %)
   :min #(.getMin ^DescriptiveStatistics %)
   :max #(.getMax ^DescriptiveStatistics %)
   :median #(.getPercentile ^DescriptiveStatistics % 50.0)
   :variance #(.getVariance ^DescriptiveStatistics %)
   :skew #(.getSkewness ^DescriptiveStatistics %)
   :kurtosis #(.getKurtosis ^DescriptiveStatistics %)
   :geometric-mean #(.getGeometricMean ^DescriptiveStatistics %)
   :sum-of-squares desc-stats-sum-of-squares
   :sum-of-logs desc-stats-sum-of-logs
   :quadratic-mean #(.getQuadraticMean ^DescriptiveStatistics %)
   :standard-deviation #(.getStandardDeviation ^DescriptiveStatistics %)
   :variance-population #(.getPopulationVariance ^DescriptiveStatistics %)
   :sum #(.getSum ^DescriptiveStatistics %)
   :product desc-stats-product
   :quartile-1 #(.getPercentile ^DescriptiveStatistics % 25.0)
   :quartile-3 #(.getPercentile ^DescriptiveStatistics % 75.0)
   :ecount #(.getN ^DescriptiveStatistics %)
   })


(defn supported-descriptive-stats
  []
  (keys supported-stats-map))


(defn descriptive-stats
  "Generate descriptive statistics for a particular item."
  [item & [stats-set]]
  (let [stats-set (set (or stats-set [:mean :min :max :ecount :standard-deviation
                                      :median]))
        stats-desc (DescriptiveStatistics. (->double-array item))]
    (->> stats-set
         (map (fn [stats-key]
                (if-let [supported-stat (get supported-stats-map stats-key)]
                  [stats-key (supported-stat stats-desc)]
                  (throw (ex-info (format "Unsupported statistic: %s"
                                          stats-key) {})))))
         (into {}))))


(defn percentile
  "Get the nth percentile.  Percent ranges from 0-100."
  [item percent]
  (-> (DescriptiveStatistics. (->double-array item))
      (.getPercentile (double percent))))


(defmacro define-supported-stats-oneoffs
  []
  `(do
     ~@(->> (supported-descriptive-stats)
            (map (fn [stat-name]
                   `(defn ~(symbol (name stat-name))
                      ~(format "Supported stat %s" (name stat-name))
                      [~'item]
                      (-> (descriptive-stats ~'item [~stat-name])
                          ~stat-name)))))))


(define-supported-stats-oneoffs)


(defn- kixi-apply
  [kixi-fn item]
  (transduce identity kixi-fn (or (dtype-proto/as-reader item)
                                  (dtype-proto/as-iterable item))))

(defn harmonic-mean
  [item]
  (kixi-apply kixi/harmonic-mean item))


(defn standard-deviation-population
  [item]
  (kixi-apply kixi/standard-deviation-p item))


(defn standard-error
  [item]
  (kixi-apply kixi/standard-error item))


(defn skewness
  [item]
  (skew item))


(defn skewness-population
  [item]
  (kixi-apply kixi/skewness-p item))


(defn kurtosis
  [item]
  (kixi-apply kixi/kurtosis item))


(defn kurtosis-population
  [item]
  (kixi-apply kixi/kurtosis-p item))


(defn pearsons-correlation
  [lhs rhs]
  (-> (PearsonsCorrelation.)
      (.correlation (->double-array lhs) (->double-array rhs))))


(defn spearmans-correlation
  [lhs rhs]
  (-> (SpearmansCorrelation.)
      (.correlation (->double-array lhs) (->double-array rhs))))


(defn kendalls-correlation
  [lhs rhs]
  (-> (KendallsCorrelation.)
      (.correlation (->double-array lhs) (->double-array rhs))))


(defn quartiles
  "return [min, 25 50 75 max] of item"
  [item]
  (let [stats (DescriptiveStatistics. (->double-array item))]
    [(.getMin stats)
     (.getPercentile stats 25.0)
     (.getPercentile stats 50.0)
     (.getPercentile stats 75.0)
     (.getMax stats)]))


(defn quartile-outlier-fn
  "Create a function that, given floating point data, will return true or false
  if that data is an outlier.  Default range mult is 1.5:
  (or (< val (- q1 (* range-mult iqr)))
      (> val (+ q3 (* range-mult iqr)))"
  [item & [range-mult]]
  (let [[_ q1 _ q3 _] (quartiles item)
        q1 (double q1)
        q3 (double q3)
        iqr (- q3 q1)
        range-mult (double (or range-mult 1.5))]
    (boolean-op/make-boolean-unary-op
     :float64
     (or (< x (- q1 (* range-mult iqr)))
         (> x (+ q3 (* range-mult iqr)))))))
