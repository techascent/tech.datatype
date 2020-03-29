(ns tech.v2.datatype.clj-range
  "Datatype bindings for clojure ranges."
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.base :as base]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.casting :as casting])
  (:import [clojure.lang LongRange Range]
           [tech.v2.datatype LongReader]
           [java.lang.reflect Field]))


(set! *warn-on-reflection* true)


(def lr-step-field (doto (.getDeclaredField ^Class LongRange "step")
                  (.setAccessible true)))


(extend-type LongRange
  dtype-proto/PDatatype
  (get-datatype [rng] :int64)
  dtype-proto/PCountable
  (ecount [rng] (.count rng))
  dtype-proto/PClone
  (clone [rng] rng)
  dtype-proto/PToReader
  (convertible-to-reader? [rng] true)
  (->reader [rng options]
    (let [start (long (first rng))
          step (long (.get ^Field lr-step-field rng))
          n-elems (.count rng)]
      (-> (reify
            LongReader
            (lsize [rdr] n-elems)
            (read [rdr idx]
              (-> (* step idx)
                  (+ start))))
          (dtype-proto/->reader options)))))


(def r-step-field (doto (.getDeclaredField ^Class Range "step")
                    (.setAccessible true)))


(defmacro range-reader-macro
  [datatype rng options]
  `(let [rng# ~rng
         start# (casting/datatype->cast-fn :unknown ~datatype (first rng#))
         step# (casting/datatype->cast-fn :unknown ~datatype
                                          (.get ^Field r-step-field rng#))
         n-elems# (.count rng#)]
      (-> (reify ~(typecast/datatype->reader-type datatype)
            (lsize [rdr#] n-elems#)
            (read [rdr# idx#]
              (casting/datatype->cast-fn :uknown ~datatype
                                         (-> (* step# idx#)
                                             (+ start#)))))
          (dtype-proto/->reader ~options))))


(extend-type Range
  dtype-proto/PDatatype
  (get-datatype [rng] (base/get-datatype (first rng)))
  dtype-proto/PCountable
  (ecount [rng] (.count rng))
  dtype-proto/PClone
  (clone [rng] rng)
  dtype-proto/PToReader
  (convertible-to-reader? [rng] (contains?
                                 #{:int8 :int16 :int32 :int64
                                   :float32 :float64}
                                 (base/get-datatype rng)))
  (->reader [rng options]
    (case (base/get-datatype (first rng))
      :int8 (range-reader-macro :int8 rng options)
      :int16 (range-reader-macro :int16 rng options)
      :int32 (range-reader-macro :int32 rng options)
      :int64 (range-reader-macro :int64 rng options)
      :float32 (range-reader-macro :float32 rng options)
      :float64 (range-reader-macro :float64 rng options))))
