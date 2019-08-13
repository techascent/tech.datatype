(ns tech.v2.tensor.pprint
  "Taken from clojure.core.matrix pprint"
  (:require [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dtype-fn]
            [tech.v2.datatype.unary-op :as unary]
            [tech.v2.tensor.impl :as tens-impl]
            [tech.v2.tensor.protocols :as tens-proto]
            [tech.v2.datatype.reduce-op :as reduce-op])
  (:import [java.lang StringBuilder]
           [java.io Writer]
           [tech.v2.tensor.impl Tensor]))


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

(defn- column-lengths
  "Finds the longest string length of each column in an array of Strings."
  [m]
  (let [item-shape (dtype/shape m)
        n-dims (count item-shape)]
    (if (> n-dims 1)
      (let [n-cols (last item-shape)]
        (->> (range n-cols)
             (mapv (fn [col-idx]
                     (->> (apply tens-impl/select m (concat (repeat (dec n-dims) :all)
                                                            [col-idx]))
                          dtype/->reader
                          (map #(.length ^String %))
                          (apply max))))))
      (mapv #(.length ^String %) (dtype/->reader m)))))


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
  [^StringBuilder sb row column-lengths] ;; the first element doesn't have a leading ws.
  (let [column-lengths (typecast/datatype->reader :int32 column-lengths)
        row (typecast/datatype->reader :object row)
        cc (dtype/ecount column-lengths)]
    (.append sb \[)
    (dotimes [i cc]
      (when (> i 0) (.append sb \space))
      (append-elem sb (.read row i) (.read column-lengths i)))
    (.append sb \])))


(defn- rprint
  "Recursively joins each element with a leading line break and whitespace. If there are
  no elements left in the matrix it ends with a closing bracket."
  [^StringBuilder sb tens prefix column-lengths]
  (let [tens-shape (dtype/shape tens)
        prefix (str prefix " ")
        n-dims (count tens-shape)]
    (if (= 1 n-dims)
      (append-row sb tens column-lengths)
      (do
        (.append sb \[)
        (dotimes [i (first tens-shape)]
          (when (> i 0)
            (.append sb NL)
            (.append sb prefix))
          (rprint sb (apply tens-impl/select tens i
                            (repeat (dec n-dims) :all))
                  prefix column-lengths))
        (.append sb \])))))


(defn base-tensor->string
  "Pretty-prints a tensor. Returns a String containing the pretty-printed representation."
  ([tens]
    (base-tensor->string tens nil))
  ([tens {:keys [prefix formatter]}]
   (let [formatter (or formatter default-formatter)]
     (if (number? tens)
       (formatter tens)
       (let [n-dims (count (dtype/shape tens))
             tens (-> (unary/unary-reader :object (formatter x) tens)
                      (tens-impl/tensor-force))
             prefix (or prefix "")
             sb (StringBuilder.)]
         (let [column-lengths (column-lengths tens)]
           (rprint sb tens prefix column-lengths))
         (.toString sb))))))



(defn tensor->string
  ^String [tens & {:keys [print-datatype]
                   :or {print-datatype :float64}}]
  (format "#tech.v2.tensor<%s>%s\n%s"
          (name (dtype/get-datatype tens))
          (dtype/shape tens)
          (base-tensor->string tens)))


(defmethod print-method Tensor
  [tens w]
  (.write ^Writer w (tensor->string tens)))


(extend-type Tensor
  tens-proto/PTensorPrinter
  (print-tensor [tensor]
    (tensor->string tensor)))
