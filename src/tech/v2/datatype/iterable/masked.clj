(ns tech.v2.datatype.iterable.masked
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.nio-access :as access]))


(defmacro make-masked-iterable-impl
  [datatype]
  `(fn [datatype# values# mask# unchecked?#]
     (reify
       dtype-proto/PDatatype
       (get-datatype [item#] datatype#)
       Iterable
       (iterator [item#]
         (let [values# (typecast/datatype->iter ~datatype values# unchecked?#)
               mask# (typecast/datatype->iter :boolean mask# true)]
           (while (and (.hasNext mask#)
                       (= false (.current mask#)))
             (.nextBoolean mask#)
             (typecast/datatype->iter-next-fn ~datatype values#))
           (reify ~(typecast/datatype->iter-type datatype)
             (hasNext [item#] (and (.hasNext mask#)
                                   (.current mask#)
                                   (.hasNext values#)))
             (~(typecast/datatype->iter-next-fn-name datatype)
              [item#]
              (let [retval# (.current values#)]
                (when (.hasNext mask#)
                  (.nextBoolean mask#)
                  (typecast/datatype->iter-next-fn ~datatype values#)
                  (while (and (.hasNext mask#)
                              (= false (.current mask#)))
                    (.nextBoolean mask#)
                    (typecast/datatype->iter-next-fn ~datatype values#)))
                retval#))
             (current [item#] (.current values#))))))))


(defmacro make-masked-iterable-table
  []
  `(->> [~@(for [dtype casting/base-marshal-types]
             [dtype `(make-masked-iterable-impl ~dtype)])]
        (into {})))


(def masked-iterable-table (make-masked-iterable-table))


(defn iterable-mask
  [{:keys [datatype unchecked?]} mask-iter values]
  (let [datatype (or datatype (dtype-proto/get-datatype values))
        mask-fn (get masked-iterable-table (casting/safe-flatten datatype))]
    (mask-fn datatype values mask-iter unchecked?)))
