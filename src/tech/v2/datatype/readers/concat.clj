(ns tech.v2.datatype.readers.concat
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.typecast :as typecast])
  (:import [java.util List]))


(set! *warn-on-reflection* true)


(defmacro make-dual-reader-impl
  [datatype]
  `(fn [datatype# concat-args#]
     (let [^List reader-args# (mapv #(dtype-proto/->reader % {:datatype datatype#})
                                    concat-args#)
           total-size# (long (apply + (map #(dtype-base/ecount %) reader-args#)))
           first-arg# (first concat-args#)
           second-arg# (second concat-args#)
           first-reader# (typecast/datatype->reader ~datatype (.get reader-args# 0))
           second-reader# (typecast/datatype->reader ~datatype (.get reader-args# 1))
           initial-ecount# (dtype-base/ecount first-reader#)
           has-min-max?# (and (dtype-proto/has-constant-time-min-max? first-arg#)
                              (dtype-proto/has-constant-time-min-max? second-arg#))
           [cmin# cmax#] (when has-min-max?#
                           [(min (dtype-proto/constant-time-min first-arg#)
                                 (dtype-proto/constant-time-min second-arg#))
                            (max (dtype-proto/constant-time-max first-arg#)
                                 (dtype-proto/constant-time-max second-arg#))])]
       (reify
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [item#] has-min-max?#)
         (constant-time-min [item#] cmin#)
         (constant-time-max [item#] cmax#)
         ~(typecast/datatype->reader-type datatype)
         (getDatatype [rdr#] datatype#)
         (lsize [rdr#] total-size#)
         (read [rdr# idx#]
           (if (< idx# initial-ecount#)
             (.read first-reader# idx#)
             (.read second-reader# (- idx# initial-ecount#))))))))


(def dual-reader-table (casting/make-base-datatype-table make-dual-reader-impl))


(defmacro make-same-len-concat-reader-impl
  [datatype]
  `(fn [datatype# concat-args#]
     (let [^List reader-args# (mapv #(dtype-proto/->reader % {:datatype datatype#})
                                    concat-args#)
           n-readers# (count reader-args#)
           reader-ecount# (dtype-base/ecount (first reader-args#))
           total-size# (* n-readers# reader-ecount#)
           has-min-max?# (every? dtype-proto/has-constant-time-min-max? concat-args#)
           first-arg# (first concat-args#)
           [cmin# cmax#] (when has-min-max?#
                           (reduce (fn [[cmin# cmax#] next-reader#]
                                     [(min cmin# (dtype-proto/constant-time-min
                                                  next-reader#))
                                      (max cmax# (dtype-proto/constant-time-max
                                                  next-reader#))])
                                   [(dtype-proto/constant-time-min first-arg#)
                                    (dtype-proto/constant-time-max first-arg#)]
                                   (rest concat-args#)))]
       (reify
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [item#] has-min-max?#)
         (constant-time-min [item#] cmin#)
         (constant-time-max [item#] cmax#)
         ~(typecast/datatype->reader-type datatype)
         (getDatatype [rdr#] datatype#)
         (lsize [rdr#] total-size#)
         (read [rdr# idx#]
           (let [rdr-idx# (quot idx# reader-ecount#)
                 local-idx# (rem idx# reader-ecount#)]
             (.read (typecast/datatype->reader
                     ~datatype
                     (.get reader-args# rdr-idx#))
                    local-idx#)))))))


(def same-len-reader-table
  (casting/make-base-datatype-table make-same-len-concat-reader-impl))



(defmacro make-concat-reader-impl
  [datatype]
  `(fn [datatype# concat-args#]
     (let [^List reader-args# (mapv #(dtype-proto/->reader % {:datatype datatype#})
                                    concat-args#)
           total-size# (long (apply + (map #(dtype-base/ecount %) reader-args#)))
           has-min-max?# (every? dtype-proto/has-constant-time-min-max? concat-args#)
           first-arg# (first concat-args#)
           [cmin# cmax#] (when has-min-max?#
                           (reduce (fn [[cmin# cmax#] next-reader#]
                                     [(min cmin# (dtype-proto/constant-time-min
                                                  next-reader#))
                                      (max cmax# (dtype-proto/constant-time-max
                                                  next-reader#))])
                                   [(dtype-proto/constant-time-min first-arg#)
                                    (dtype-proto/constant-time-max first-arg#)]
                                   (rest concat-args#)))]
       (reify
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [item#] has-min-max?#)
         (constant-time-min [item#] cmin#)
         (constant-time-max [item#] cmax#)
         ~(typecast/datatype->reader-type datatype)
         (getDatatype [rdr#] datatype#)
         (lsize [rdr#] total-size#)
         (read [rdr# idx#]
           (loop [rdr-idx# 0
                  idx# idx#]
             (let [cur-rdr# (typecast/datatype->reader ~datatype
                                                       (.get reader-args# rdr-idx#))
                   cur-lsize# (.lsize cur-rdr#)]
               (if (< idx# cur-lsize#)
                 (.read cur-rdr# idx#)
                 (recur (inc rdr-idx#) (- idx# cur-lsize#))))))))))


(def concat-reader-table (casting/make-base-datatype-table make-concat-reader-impl))


(defn concat-readers
  ([options readers]
   (let [n-readers (count readers)
         datatype (or (:datatype options) (dtype-base/get-datatype (first readers)))
         first-reader-len (dtype-base/ecount (first readers))]
     (cond
       (== 1 n-readers) (first readers)
       (== 2 n-readers)
       (let [reader-fn (get dual-reader-table (casting/safe-flatten datatype))]
         (reader-fn datatype readers))
       (every? #(= first-reader-len
                   (dtype-base/ecount %))
               (rest readers))
       (let [reader-fn (get same-len-reader-table (casting/safe-flatten datatype))]
         (reader-fn datatype readers))
       :else
       (let [reader-fn (get concat-reader-table (casting/safe-flatten datatype))]
         (reader-fn datatype readers)))))
  ([readers]
   (concat-readers {} readers)))
