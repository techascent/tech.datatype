(ns tech.v2.tensor.protocols)


(defprotocol PTensor
  (dimensions [item])
  (buffer [item]))


(defprotocol PToTensor
  (tensor-convertible? [item])
  (convert-to-tensor [item]))


(extend-type Object
  PToTensor
  (tensor-convertible? [item] false))


(defn as-tensor
  [item]
  (when (tensor-convertible? item)
    (convert-to-tensor item)))


(defprotocol PToTensorReader
  (convertible-to-tensor-reader? [item])
  (->tensor-reader [item options]))


(extend-type Object
  PToTensorReader
  (convertible-to-tensor-reader? [item] false))
