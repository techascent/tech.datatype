(ns tech.v2.datatype.readers.reverse
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.reader :as reader]))


(defmacro make-reverse-reader
  [datatype]
  `(fn [src-reader#]
     (let [src-reader# (typecast/datatype->reader ~datatype src-reader#)
           n-elems# (.lsize src-reader#)
           n-elems-m1# (- n-elems# 1)
           src-dtype# (dtype-proto/get-datatype src-reader#)]
       (reader/make-derived-reader ~datatype src-dtype# {:unchecked? true} src-reader#
                                   (.read src-reader#
                                          (- n-elems-m1# ~'idx))
                                   dtype-proto/->reader
                                   n-elems#))))


(def reverse-reader-table (casting/make-base-datatype-table make-reverse-reader))


(defn reverse-reader
  ([src-reader {:keys [datatype]}]
   (let [datatype (or datatype (dtype-proto/get-datatype src-reader))
         create-fn (get reverse-reader-table (casting/safe-flatten datatype))]
     (create-fn src-reader)))
  ([src-reader]
   (reverse-reader src-reader {})))
