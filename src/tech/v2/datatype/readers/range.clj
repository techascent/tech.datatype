(ns tech.v2.datatype.readers.range
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]))


(defmacro make-range-reader
  [datatype]
  (when-not (casting/numeric-type? datatype)
    (throw (ex-info (format "Datatype (%s) is not a numeric type"
                            datatype) {})))
  `(fn [start# end# increment#]
     (let [start# (casting/datatype->cast-fn :unknown ~datatype start#)
           end# (casting/datatype->cast-fn :unknown ~datatype end#)
           increment# (casting/datatype->cast-fn :unkown ~datatype increment#)
           n-elems# (Math/round (double (/ (- end# start#)
                                           increment#)))]

       (reify ~(typecast/datatype->reader-type datatype)
         (getDatatype [item#] ~datatype)
         (lsize [item#] n-elems#)
         (read [item# idx#]
           (when-not (< idx# n-elems#)
             (throw (ex-info (format "Index out of range: %s >= %s" idx# n-elems#))))
           (casting/datatype->unchecked-cast-fn
            :unknown ~datatype
            (+ (* increment# idx#)
               start#)))))))


(defmacro make-range-reader-table
  []
  `(->> [~@(for [dtype (->> casting/base-marshal-types
                            (filter casting/numeric-type?))]
             [dtype `(make-range-reader ~dtype)])]
        (into {})))


(def range-reader-table (make-range-reader-table))

(defn reader-range
  [datatype start end & [increment]]
  (if-let [reader-fn (get range-reader-table (casting/safe-flatten datatype))]
    (reader-fn start end (or increment 1))
    (throw (ex-info (format "Failed to find reader fn for datatype %s" datatype)
                    {}))))
