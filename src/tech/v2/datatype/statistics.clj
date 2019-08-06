(ns tech.v2.datatype.statistics
  (:require [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.boolean-op :as boolean-op]
            [tech.v2.datatype.array]
            [kixi.stats.core :as kixi])
  (:import [org.apache.commons.math3.stat.correlation
            KendallsCorrelation PearsonsCorrelation SpearmansCorrelation]
           [org.apache.commons.math3.stat.descriptive DescriptiveStatistics]))


(defn- kixi-apply
  [kixi-fn item]
  (transduce identity kixi-fn (or (dtype-proto/as-reader item)
                                  (dtype-proto/as-iterable item))))

(defn- ->double-array
  ^doubles [item]
  (if (instance? (Class/forName "[D") item)
    item
    (dtype-base/make-container :java-array :float64 item)))



(defn mean
  [item]
  (kixi-apply kixi/mean item))


(defn median
  [item]
  (kixi-apply kixi/median item))


(defn geometric-mean
  [item]
  (kixi-apply kixi/geometric-mean item))


(defn harmonic-mean
  [item]
  (kixi-apply kixi/harmonic-mean item))


(defn variance
  [item]
  (kixi-apply kixi/variance item))


(defn variance-population
  [item]
  (kixi-apply kixi/variance-p item))


(defn standard-deviation
  [item]
  (kixi-apply kixi/standard-deviation item))


(defn standard-deviation-population
  [item]
  (kixi-apply kixi/standard-deviation-p item))


(defn standard-error
  [item]
  (kixi-apply kixi/standard-error item))


(defn skewness
  [item]
  (kixi-apply kixi/skewness item))


(defn skewness-population
  [item]
  (kixi-apply kixi/skewness-p item))


(defn kurtosis
  [item]
  (kixi-apply kixi/kurtosis item))


(defn kurtosis-population
  [item]
  (kixi-apply kixi/kurtosis-p item))


(defn- ->doubles
  ^"[D" [item]
  (if (instance? (Class/forName "[D") item)
    item
    (dtype-proto/make-container :java-array :float64 item {})))


(defn pearsons-correlation
  [lhs rhs]
  (-> (PearsonsCorrelation.)
      (.correlation (->doubles lhs) (->doubles rhs))))


(defn spearmans-correlation
  [lhs rhs]
  (-> (SpearmansCorrelation.)
      (.correlation (->doubles lhs) (->doubles rhs))))


(defn kendalls-correlation
  [lhs rhs]
  (-> (KendallsCorrelation.)
      (.correlation (->doubles lhs) (->doubles rhs))))


(defn percentile
  "Get the nth percentile.  Percent ranges from 0-100."
  [item percent]
  (-> (DescriptiveStatistics. (->double-array item))
      (.getPercentile (double percent))))


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
