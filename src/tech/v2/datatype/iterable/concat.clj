(ns tech.v2.datatype.iterable.concat
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]))


(defmacro make-concat-iterable-impl
  [datatype]
  `(fn [datatype# concat-args#]
     (reify
       dtype-proto/PDatatype
       (get-datatype [item#] datatype#)
       Iterable
       (iterator [item#]
         (let [concat-args# (map #(typecast/datatype->iter ~datatype %) concat-args#)
               algo-data# (object-array [(first concat-args#)
                                         (rest concat-args#)])]
           (reify ~(typecast/datatype->iter-type datatype)
             (hasNext [item#]
               (boolean (not= nil (aget algo-data# 0))))
             (~(typecast/datatype->iter-next-fn-name datatype)
              [item#]
              (let [src-iter# (typecast/datatype->fast-iter ~datatype
                                                            (aget algo-data# 0))
                    retval# (typecast/datatype->iter-next-fn ~datatype src-iter#)]
                (when-not (.hasNext src-iter#)
                  (aset algo-data# 0 (first (aget algo-data# 1)))
                  (aset algo-data# 1 (rest (aget algo-data# 1))))
                retval#))
             (current [item#]
               (let [src-iter# (typecast/datatype->fast-iter
                                ~datatype (aget algo-data# 0))]
                 (.current src-iter#)))))))))


(defmacro make-concat-iterable-table
  []
  `(->> [~@(for [dtype casting/base-marshal-types]
             [dtype `(make-concat-iterable-impl ~dtype)])]
        (into {})))


(def concat-iterable-table (make-concat-iterable-table))


(defn iterable-concat
  [{:keys [datatype]} concat-iters]
  (let [datatype (or datatype
                     (when-let [first-arg (first concat-iters)]
                       (dtype-proto/get-datatype first-arg))
                     :float64)
        create-fn (get concat-iterable-table (casting/safe-flatten datatype))]
    (create-fn datatype concat-iters)))
