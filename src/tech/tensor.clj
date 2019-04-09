(ns tech.tensor
  (:require [tech.datatype :as dtype]
            [tech.tensor.impl :as impl]
            [tech.tensor.dimensions :as dims]
            [tech.datatype.functional :as dtype-fn]))


(defn ->tensor
  [data & {:keys [datatype container-type]
           :or {container-type :typed-buffer}
           :as options}]
  (let [data-shape (dtype/shape data)
        datatype (impl/datatype datatype)
        n-elems (apply * 1 data-shape)]
    (impl/construct-tensor
     (first
      (dtype/copy-raw->item!
       data
       (dtype/make-container container-type datatype n-elems options)
       0 options))
     (dims/dimensions data-shape))))


(defn rotate
  [tens rotate-vec]
  (assoc tens :dimensions
         (dims/rotate (impl/tensor->dimensions tens)
                      rotate-vec)))
