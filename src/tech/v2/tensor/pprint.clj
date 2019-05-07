(ns tech.v2.tensor.pprint
  "Taken from clojure.core.matrix pprint"
  (:require [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dtype-fn]
            [tech.v2.tensor.impl :as tens-impl]
            [tech.v2.datatype.reduce-op :as reduce-op])
  (:import [java.lang StringBuilder]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* true)

(def ^String NL (System/getProperty "line.separator"))

;; pretty-printing utilities for matrices
(def ^:dynamic *number-format* "%.3f")

(defn- format-num [x]
  (if (integer? x)
    (str x)
    (format *number-format* (double x))))

(defn- default-formatter [x]
  (if (number? x)
    (format-num x)
    (str x)))


(defn lazy-range-seq
  [shape]
  (mapcat (fn [item-idx]
            (if (seq (rest shape))
              (map (partial concat [item-idx])
                   (lazy-range-seq (rest shape)))
              [[item-idx]]))
       (range (first shape))))


(defn- column-lengths
  "Finds the longest string length of each column in an array of Strings."
  [m]
  (let [item-shape (dtype/shape m)
        n-dims (count item-shape)
        transpose-args (range n-dims)
        [row-dim-idx col-dim-idx] (take-last transpose-args 2)
        transpose-args (concat (drop-last transpose-args 2)
                               [col-dim-idx row-dim-idx])
        m (tens-impl/transpose m transpose-args)
        trans-shape (dtype/shape m)
        ;;Column seq
        columns (->> (lazy-range-seq (drop-last trans-shape))
                     (map #(apply tens-impl/select m (concat %
                                                             [:all]))))]
    (mapv
     (fn [col]
       (apply max (->> (dtype/->reader col)
                       (map #(.length ^String %)))))
     columns)))


(defn- append-elem
  "Appends an element, right-padding up to a given column length."
  [^StringBuilder sb ^String elem ^long clen]
  (let [c (long (count elem))
        ws (- clen c)]
    (dotimes [i ws]
      (.append sb \space))
    (.append sb elem)))

(defn- append-row
  "Appends a row of data."
  [^StringBuilder sb row clens] ;; the first element doesn't have a leading ws.
  (let [clens (typecast/datatype->reader :int32 clens)
        row (typecast/datatype->reader :object sb)
        cc (dtype/ecount clens)]
    (.append sb \[)
    (dotimes [i cc]
      (when (> i 0) (.append sb \space))
      (append-elem sb (.read row i) (.read clens i)))
    (.append sb \])))

;; (defn- rprint
;;   "Recursively joins each element with a leading
;;    line break and whitespace. If there are no
;;    elements left in the matrix it ends with a
;;    closing bracket."
;;   [^StringBuilder sb a pre clens]
;;   (let [dims (long (mp/dimensionality a))
;;         sc (long (mp/dimension-count a 0))
;;         pre2 (str pre " ")]
;;     (.append sb \[)
;;     (dotimes [i sc]
;;       (let [s (mp/get-major-slice a i)]
;;         (when (> i 0)
;;           (.append sb NL)
;;           (.append sb pre2))
;;         (if (== 2 dims)
;;           (append-row sb s clens)
;;           (rprint sb s pre2 clens))))
;;     (.append sb \])))

;; (defn pm
;;   "Pretty-prints an array. Returns a String containing the pretty-printed representation."
;;   ([a]
;;     (pm a nil))
;;   ([tens {:keys [prefix formatter]}]
;;    (if (tens-impl/tensor? tens)
;;      (let [formatter (or formatter default-formatter)
;;            n-dims (count (dtype/shape tens))
;;            tens (-> (dtype-fn/unary-reader-map :object formatter tens)
;;                     (tens-impl/tensor-force))
;;            prefix (or prefix "")
;;            sb (StringBuilder.)]
;;        (cond
;;          (== 1 n-dims)
;;          (append-row sb tens (column-lengths tens))
;;          :else
;;          (let [clens (column-lengths tens)] (rprint sb m prefix clens)))
;;        (.toString sb)))))
