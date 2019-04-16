(ns tech.tensor.protocols)


(defprotocol PToTensorReader
  (->tensor-reader-of-type [item datatype unchecked?]))
