(ns tech.compute.tensor.dimensions.shape
  "A shape vector entry can be a number of things.  We want to be precise
  with handling them and abstract that handling so new things have a clear
  path."
  (:require [tech.compute.tensor.utils
             :refer [when-not-error reversev map-reversev]]
            [tech.datatype :as dtype]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(def monotonic-operators
  {:+ <
   :- >})


(defn classify-sequence
  [item-seq]
  ;;Normalize this to account for single digit numbers
  (if (number? item-seq)
    {:type :+
     :min-item item-seq
     :max-item item-seq
     :scalar? true}
    (do
      (when-not (seq item-seq)
        (throw (ex-info "Nil sequence in check monotonic" {})))
      (let [n-elems (count item-seq)
            first-item (long (first item-seq))
            last-item (long (last item-seq))
            min-item (min first-item last-item)
            max-item (max first-item last-item)
            retval {:min-item min-item
                    :max-item max-item}]
        (if (= n-elems 1)
          (assoc retval :type :+)
          (let [mon-op (->> monotonic-operators
                            (map (fn [[op-name op]]
                                   (when (apply op item-seq)
                                     op-name)))
                            (remove nil?)
                            first)]
            (if (and (= n-elems
                        (+ 1
                           (- max-item
                              min-item)))
                     mon-op)
              (assoc retval :type mon-op)
              (assoc retval :sequence (vec item-seq)))))))))


(def classified-sequence-keys #{:type :min-item :max-item :sequence})


(defn classified-sequence?
  [item]
  (and (map? item)
       (= 3 (->> (keys item)
                 (filter classified-sequence-keys)
                 count))))


(defn classified-sequence->count
  ^long [{:keys [type min-item max-item sequence]}]
  (if type
    (+ 1 (- (long max-item) (long min-item)))
    (count sequence)))


(defn classified-sequence->sequence
  [{:keys [min-item max-item type sequence]}]
  (->> (if sequence
         sequence
         (case type
           :+ (range min-item (inc (long max-item)))
           :- (range max-item (dec (long min-item)) -1)))
       ;;Ensure dtype/get-value works on result.
       vec))


(defn combine-classified-sequences
  "Room for optimization here.  But simplest way is easiest to get correct."
  [source-sequence select-sequence]
  (let [last-valid-index (if (:type source-sequence)
                           (- (long (:max-item source-sequence))
                              (long (:min-item source-sequence)))
                           (- (count (:sequence source-sequence))
                              1))]
    (when (> (long (:max-item select-sequence))
             last-valid-index)
      (throw (ex-info "Select argument out of range" {:dimension source-sequence
                                                      :select-arg select-sequence}))))
  (let [source-sequence (classified-sequence->sequence source-sequence)]
    (->> (classified-sequence->sequence select-sequence)
         (map #(get source-sequence (long %)))
         classify-sequence)))


(defn reverse-classified-sequence
  [{:keys [type sequence] :as item}]
  (if sequence
    (assoc item :sequence
           (reversev sequence))
    (assoc item :type
           (if (= type :+)
             :-
             :+))))


(defn classified-sequence->elem-idx
  ^long [{:keys [type min-item max-item sequence] :as dim} ^long shape-idx]
  (let [min-item (long min-item)
        max-item (long max-item)
        last-idx (- max-item min-item)]
    (when (> shape-idx last-idx)
      (throw (ex-info "Element access out of range"
                      {:shape-idx shape-idx
                       :dimension dim})))
    (if (= :+ type)
      (+ min-item shape-idx)
      (- max-item shape-idx))))


(defn shape-entry->count
  "Return a vector of counts of each shape."
  ^long [shape-entry]
  (cond
    (number? shape-entry)
    (long shape-entry)
    (classified-sequence? shape-entry)
    (classified-sequence->count shape-entry)
    :else
    (long (dtype/ecount shape-entry))))


(defn shape->count-vec
  [shape-vec]
  (mapv shape-entry->count shape-vec))


(defn direct-shape?
  [shape]
  (every? number? shape))

(defn indirect-shape?
  [shape]
  (not (direct-shape? shape)))

(defn reverse-shape
  [shape-vec]
  (map-reversev shape-entry->count shape-vec))

(defn ecount
  "Return the element count indicated by the dimension map"
  ^long [shape]
  (long (apply * (shape->count-vec shape))))


(defn- ensure-direct
  [shape-seq]
  (when-not (direct-shape? shape-seq)
    (throw (ex-info "Index buffers not supported for this operation." {})))
  shape-seq)


(defn ->2d
  "Given dimensions, return new dimensions with the lowest (fastest-changing) dimension
  unchanged and the rest of the dimensions multiplied into the higher dimension."
  [shape]
  (when-not-error (seq shape)
    "Invalid shape in dimension map"
    {:shape shape})
  (if (= 1 (count shape))
    [1 (first shape)]
    [(apply * (ensure-direct (drop-last shape))) (last shape)]))


(defn ->batch
  "Given dimensions, return new dimensions with the lowest (fastest-changing) dimension
  unchanged and the rest of the dimensions multiplied into the higher dimension."
  [shape]
  (when-not-error (seq shape)
    "Invalid shape in dimension map"
    {:shape shape})
  (if (= 1 (count shape))
    [1 (first shape)]
    [(first shape) (apply * (ensure-direct (drop 1 shape)))]))
