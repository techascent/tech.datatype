(ns tech.v2.datatype.vector-of
  "Bindings to the vectors produced by vector-of"
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.casting :as casting])
  (:import [clojure.core Vec ArrayManager]))

(set! *warn-on-reflection* true)


(defmacro ^:private specific-list-reader
  [datatype n-elems list-data]
  `(reify ~(typecast/datatype->reader-type datatype)
     (lsize [item#] ~n-elems)
     (read [item# idx#]
       (casting/datatype->unchecked-cast-fn
        :unknown
        ~datatype
        (.get ~list-data idx#)))))

(defmacro ^:private list-reader
  ([datatype item]
   `(let [item# (typecast/object-list-cast ~item)
          n-elems# (long (.size item#))]
      (case ~datatype
        :boolean (specific-list-reader :boolean n-elems# item#)
        :int8 (specific-list-reader :int8 n-elems# item#)
        :int16 (specific-list-reader :int16 n-elems# item#)
        :int32 (specific-list-reader :int32 n-elems# item#)
        :int64 (specific-list-reader :int64 n-elems# item#)
        :float32 (specific-list-reader :float32 n-elems# item#)
        :float64 (specific-list-reader :float64 n-elems# item#))))
  ([item]
   `(list-reader (dtype-proto/get-datatype ~item) ~item)))


(extend-type Vec
  dtype-proto/PDatatype
  (get-datatype [item]
    (dtype-proto/get-datatype
     (.array ^ArrayManager (.am item) 1)))
  dtype-proto/PClone
  (clone [item] item)
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (let [item-dtype (dtype-proto/get-datatype item)
          new-reader (list-reader item-dtype item)
          final-dtype (or (:datatype options) item-dtype)]
      (if (= final-dtype item-dtype)
        new-reader
        (dtype-proto/->reader new-reader options))))
  dtype-proto/PBuffer
  (sub-buffer [item offset length]
    (dtype-proto/sub-buffer (dtype-proto/->reader item nil) offset length)))
