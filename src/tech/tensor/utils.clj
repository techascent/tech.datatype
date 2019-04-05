(ns tech.tensor.utils)


(defmacro when-not-error
  [expr error-msg extra-data]
  `(when-not ~expr
     (throw (ex-info ~error-msg ~extra-data))))


(defn reversev
  [item-seq]
  (if (vector? item-seq)
    (let [len (count item-seq)
          retval (transient [])]
      (loop [idx 0]
        (if (< idx len)
          (do
            (conj! retval (item-seq (- len idx 1)))
            (recur (inc idx)))
          (persistent! retval))))
    (vec (reverse item-seq))))


(defn map-reversev
  [map-fn item-seq]
  (if (vector? item-seq)
    (let [len (count item-seq)
          retval (transient [])]
      (loop [idx 0]
        (if (< idx len)
          (do
            (conj! retval (map-fn (item-seq (- len idx 1))))
            (recur (inc idx)))
          (persistent! retval))))
    (vec (reverse item-seq))))


(defn all-combinations
  [item-seq]
  (let [item (first item-seq)
        rest-items (rest item-seq)]
    (if (seq rest-items)
      (concat (map vector (repeat item) rest-items)
              (lazy-seq (all-combinations rest-items)))
      nil)))
