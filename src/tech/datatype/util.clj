(ns tech.datatype.util)


;;Test utility, keep at end of file.
(defn- cartesian-list
  [coll]
  (when-let [rest-coll (seq (rest coll))]
    (cons [(first coll) rest-coll]
          (lazy-seq (cartesian-list rest-coll)))))


(defn all-pairs
  [item-list]
  (let [forward-pairs (mapcat (fn [[item coll]]
                                (partition 2 (interleave (repeat item) coll)))
                              (cartesian-list item-list))
        backward-pairs (map reverse forward-pairs)]
    (concat forward-pairs backward-pairs)))
