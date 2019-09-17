(ns tech.v2.datatype.iterator
  (:require [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.argtypes :as argtypes]
            [tech.v2.datatype.iterable.const :as iter-const])
  (:import [tech.v2.datatype ObjectIter ByteIter ShortIter
            IntIter LongIter FloatIter DoubleIter
            BooleanIter IteratorObjectIter]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro make-marshal-iterator-macro
  [src-dtype dest-dtype]
  `(fn [iterator# datatype# unchecked?#]
     (let [src-iterator# (typecast/datatype->iter ~src-dtype iterator#
                                                  unchecked?#)]
       (if unchecked?#
         (reify ~(typecast/datatype->iter-type dest-dtype)
           (getDatatype [item#] datatype#)
           (hasNext [item#] (.hasNext src-iterator#))
           (~(typecast/datatype->iter-next-fn-name dest-dtype) [item#]
            (let [temp-val# (typecast/datatype->iter-next-fn ~src-dtype src-iterator#)]
              (casting/datatype->unchecked-cast-fn ~src-dtype ~dest-dtype temp-val#)))
           (current [item#]
             (let [temp-val# (.current src-iterator#)]
               (casting/datatype->unchecked-cast-fn ~src-dtype ~dest-dtype temp-val#))))
         (reify ~(typecast/datatype->iter-type dest-dtype)
           (getDatatype [item#] datatype#)
           (hasNext [item#] (.hasNext src-iterator#))
           (~(typecast/datatype->iter-next-fn-name dest-dtype) [item#]
            (let [temp-val# (typecast/datatype->iter-next-fn ~src-dtype src-iterator#)]
              (casting/datatype->cast-fn ~src-dtype ~dest-dtype temp-val#)))
           (current [item#]
             (let [temp-val# (.current src-iterator#)]
               (casting/datatype->cast-fn ~src-dtype ~dest-dtype temp-val#))))))))


(def marshal-iterator-table (casting/make-marshalling-item-table
                             make-marshal-iterator-macro))


(defn make-marshal-iterator
  [item datatype unchecked?]
  (let [item-dtype (dtype-proto/get-datatype item)]
    (if (= datatype item-dtype)
      item
      (let [iter-fn (get marshal-iterator-table [(casting/safe-flatten item-dtype)
                                                 (casting/safe-flatten datatype)])]
        (iter-fn item datatype unchecked?)))))


(extend-type Iterable
  dtype-proto/PToIterable
  (convertible-to-iterable? [item] true)
  (->iterable [item {:keys [datatype unchecked?] :as options}]
    (if-let [src-reader (dtype-proto/as-reader item options)]
      src-reader
      (reify
        Iterable
        (iterator [iter-item]
          (-> (IteratorObjectIter. (.iterator item) :object)
              (make-marshal-iterator datatype unchecked?)))
        dtype-proto/PDatatype
        (get-datatype [item] datatype)))))


(defn ->iterable
  [item]
  (if (= :scalar (argtypes/arg->arg-type item))
    (iter-const/make-const-iterable item (dtype-proto/get-datatype item))
    item))
