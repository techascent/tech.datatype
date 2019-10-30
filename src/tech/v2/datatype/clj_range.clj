(ns tech.v2.datatype.clj-range
  "Datatype bindings for clojure ranges."
  (:require [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.protocols :as dtype-proto])
  (:import [clojure.lang LongRange]
           [tech.v2.datatype LongReader]))


(set! *warn-on-reflection* true)


(extend-type LongRange
  dtype-proto/PDatatype
  (get-datatype [rng] :int64)
  dtype-proto/PCountable
  (ecount [rng] (.count rng))
  dtype-proto/PToReader
  (convertible-to-reader? [rng] true)
  (->reader [rng options]
    (let [step-field (doto (.getDeclaredField ^Class (type rng) "step")
                       (.setAccessible true))
          start (long (first rng))
          step (long (.get step-field rng))
          n-elems (.count rng)]
      (-> (reify LongReader
            (lsize [rdr] n-elems)
            (read [rdr idx]
              (-> (* step idx)
                  (+ start))))
          (dtype-proto/->reader options)))))
