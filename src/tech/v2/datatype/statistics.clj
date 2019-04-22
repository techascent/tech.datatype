(ns tech.v2.datatype.statistics
  (:require [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.array]
            [kixi.stats.core :as kixi])
  (:import [org.apache.commons.math3.stat.correlation
            KendallsCorrelation PearsonsCorrelation SpearmansCorrelation]))


(defn- kixi-apply
  [kixi-fn item]
  (transduce identity kixi-fn (typecast/->iterable item)))


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
