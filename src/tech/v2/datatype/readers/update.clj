(ns tech.v2.datatype.readers.update
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.bitmap :as bitmap])
  (:import [java.util List Map]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defmacro make-reader-impl
  [datatype]
  `(fn [src-reader# update-map#]
     (let [dtype# (dtype-proto/get-datatype src-reader#)
           src-reader# (typecast/datatype->reader ~datatype src-reader#)
           n-elems# (.lsize src-reader#)
           ^Map update-map# (typecast/->java-map update-map#)
           bitmap# (bitmap/->bitmap (keys update-map#))]
       (reify ~(typecast/datatype->reader-type datatype)
         (getDatatype [item#] dtype#)
         (lsize [item#] n-elems#)
         (read [item# idx#]
           ;;The assumption here is that when there are very few sparse values the
           ;;bitmap lookup is both a lot faster and doesn't box the index.
           (if (.contains bitmap# idx#)
             (casting/datatype->unchecked-cast-fn :unknown ~datatype
                                                  (.get update-map# idx#))
             (.read src-reader# idx#)))))))


(def reader-table (casting/make-base-datatype-table make-reader-impl))


(defn update-reader
  "Create a new reader that uses values from the update-map else uses values
  from the src-reader if the update map values do not exist."
  [src-reader update-map]
  (let [reader-fn (get reader-table (casting/safe-flatten
                                     (dtype-proto/get-datatype src-reader)))]
    (reader-fn src-reader update-map)))
