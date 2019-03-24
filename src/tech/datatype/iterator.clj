(ns tech.datatype.iterator
  (:require [tech.datatype.typecast :as typecast]
            [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.nio-access
             :refer [unchecked-full-cast
                     checked-full-write-cast]])
  (:import [tech.datatype ObjectIter ByteIter ShortIter
            IntIter LongIter FloatIter DoubleIter
            BooleanIter]))



(defmacro make-marshal-iterator
  [src-dtype intermediate-dtype dest-dtype src-iterator unchecked?]
  `(if ~unchecked?
     (reify ~(typecast/datatype->iter-type intermediate-dtype)
       (getDatatype [item#] ~intermediate-dtype)
       (hasNext [item#] (.hasNext ~src-iterator))
       (~(typecast/datatype->iter-next-fn-name intermediate-dtype) [item#]
        (-> (typecast/datatype->iter-next-fn ~src-dtype ~src-iterator)
            (unchecked-full-cast ~src-dtype
                                 ~intermediate-dtype
                                 ~dest-dtype)))
       (current [item#]
         (-> (.current ~src-iterator)
             (unchecked-full-cast ~src-dtype
                                  ~intermediate-dtype
                                  ~dest-dtype))))
     (reify ~(typecast/datatype->iter-type intermediate-dtype)
       (getDatatype [item#] ~intermediate-dtype)
       (hasNext [item#] (.hasNext ~src-iterator))
       (~(typecast/datatype->iter-next-fn-name intermediate-dtype) [item#]
        (-> (typecast/datatype->iter-next-fn ~src-dtype ~src-iterator)
            (checked-full-write-cast ~src-dtype
                                     ~intermediate-dtype
                                     ~dest-dtype)))
       (current [item#]
         (-> (.current ~src-iterator)
             (checked-full-write-cast ~src-dtype
                                      ~intermediate-dtype
                                      ~dest-dtype))))))

(defmacro make-marshal-iterator-table
  []
  `(->> [~@(for [src-dtype casting/all-host-datatypes
                 dst-dtype casting/base-datatypes]
             [[src-dtype dst-dtype]
              `(fn [iterator# unchecked?#]
                 (let [iterator# (typecast/datatype->iter ~src-dtype iterator#
                                                          unchecked?#)]
                   (make-marshal-iterator ~src-dtype ~dst-dtype
                                          ~(casting/datatype->safe-host-type
                                            dst-dtype)
                                          iterator# unchecked?#)))])]
        (into {})))


(def marshal-iterator-table (make-marshal-iterator-table))


(defmacro extend-iter-type
  [src-type datatype]
  `(clojure.core/extend ~src-type
     dtype-proto/PToIterator
     {:->iterator-of-type
      (fn [item# datatype# unchecked?#]
        (if-let [iter-fn#
                 (get marshal-iterator-table [~datatype datatype#])]
          (iter-fn# item# unchecked?#)
          (throw (ex-info (format "Failed to find iterator conversion %s->%s"
                                  ~datatype datatype#)))))}))


(extend-iter-type ByteIter :int8)
(extend-iter-type ShortIter :int16)
(extend-iter-type IntIter :int32)
(extend-iter-type LongIter :int64)
(extend-iter-type FloatIter :float32)
(extend-iter-type DoubleIter :float64)
(extend-iter-type BooleanIter :boolean)
(extend-iter-type ObjectIter :object)
