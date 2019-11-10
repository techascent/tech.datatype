(ns tech.v2.tensor.protocols
  (:require [tech.v2.datatype.protocols :as dtype-proto]))


(defprotocol PTensor
  (is-tensor? [item])
  (dimensions [item])
  (buffer [item]))


(defprotocol PTensorPrinter
  (print-tensor [item]))


(defprotocol PToTensor
  (convertible-to-tensor? [item])
  (convert-to-tensor [item]))


(defn as-tensor
  [item]
  (when (convertible-to-tensor? item)
    (convert-to-tensor item)))


(defprotocol PToTensorReader
  (convertible-to-tensor-reader? [item])
  (->tensor-reader [item options]))


(defprotocol PToTensorWriter
  (convertible-to-tensor-writer? [item])
  (->tensor-writer [item options]))


(extend-type Object
  PTensor
  (is-tensor? [item] false)
  PToTensorReader
  (convertible-to-tensor-reader? [item] false))
