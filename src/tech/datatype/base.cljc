(ns tech.datatype.base
  "Datatype library primitives shared between clojurescript and clojure.
Contains:
 - base protocols
  - functions that operate purely at the protocol level."
  (:require [clojure.core.matrix :as m])
  #?(:clj (:require [tech.datatype.base-macros :as base-macros]
                    [clojure.core.matrix.macros :refer [c-for]]
                    [tech.datatype.shared-macros :as shared-macros])
     :cljs (:require-macros [tech.datatype.base-macros :as base-macros]
                            [clojure.core.matrix.macros :refer [c-for]]
                            [tech.datatype.shared-macros :as shared-macros])))


(def datatypes
  [:int8
   :int16
   :int32
   :int64
   :float32
   :float64])

(def datatypes-set (set datatypes))

(def unsigned-datatypes
  [:uint8
   :uint16
   :uint32
   :uint64])

(def unsigned-datatype-set (set unsigned-datatypes))


(defn signed? [dtype] (boolean (unsigned-datatype-set dtype)))


(def datatype-sizes
  [1
   2
   4
   8
   4
   8])

(def unsigned-datatype-sizes
  [1 2 4 8])

(def datatype-size-map
  (into {} (map vec (partition 2 (interleave (concat datatypes unsigned-datatypes)
                                             (concat datatype-sizes unsigned-datatype-sizes))))))

(defn datatype->byte-size
  ^long [datatype] (get datatype-size-map datatype))

(defprotocol PDatatype
  (get-datatype [item]))

(defn ecount
  ^long [item]
  (m/ecount item))


#?(:cljs
   (do
    (defn- alloc-buffer
      [datatype elem-count]
      (new js/ArrayBuffer (* (datatype->byte-size datatype)
                             elem-count)))

    (defn- normalize-elem-count-or-seq
      "returns [elem-count data-seq]
  where data-seq may be nil."
      [elem-count-or-seq]
      (if (number? elem-count-or-seq)
        [(long elem-count-or-seq) nil]
        (let [array-data (new js/Array)]
          (doseq [item elem-count-or-seq]
            (.push array-data item))
          [(count array-data) array-data])))

    (defn- setup-array
      [elem-count-or-seq constructor dtype]
      (let [[data-len data-buf] (normalize-elem-count-or-seq elem-count-or-seq)
            buffer (alloc-buffer dtype data-len)
            retval (constructor buffer)]
        (when data-buf
          (loop [idx 0]
            (when (< idx data-len)
              (aset retval idx (aget data-buf idx))
              (recur (inc idx)))))
        retval))

    (defn byte-array
      [elem-count-or-seq]
      (setup-array elem-count-or-seq #(new js/Int8Array %) :int8))

    (defn short-array
      [elem-count-or-seq]
      (setup-array elem-count-or-seq #(new js/Int16Array %) :int16))

    (defn int-array
      [elem-count-or-seq]
      (setup-array elem-count-or-seq #(new js/Int32Array %) :int32))

    (defn long-array
      [elem-count-or-seq]
      (throw (ex-info "No int64 support in js" {})))

    (defn float-array
      [elem-count-or-seq]
      (setup-array elem-count-or-seq #(new js/Float32Array %) :float32))

    (defn double-array
      [elem-count-or-seq]
      (setup-array elem-count-or-seq #(new js/Float64Array %) :float64))))


(defn make-array-of-type
  [datatype elem-count-or-seq]
  (base-macros/try-catch-any
    (cond
      (= datatype :int8) (byte-array elem-count-or-seq)
      (= datatype :int16) (short-array elem-count-or-seq)
      (= datatype :int32) (int-array elem-count-or-seq)
      (= datatype :int64) (long-array elem-count-or-seq)
      (= datatype :float32) (float-array elem-count-or-seq)
      (= datatype :float64) (double-array elem-count-or-seq)
      :else
      (throw (ex-info "Unknown datatype in make-array-of-type"
                      {:datatype datatype})))

    e (ex-info "make-array-of-type failed"
                {:datatype datatype
                 :elem-count-or-seq elem-count-or-seq
                 :error e})))


(defprotocol PAccess
  (set-value! [item offset value])
  (set-constant! [item offset value elem-count])
  (get-value [item offset]))


(defprotocol PCopyQuery
  "Copy protocol when the types do not match"
  (get-copy-fn [dest destoffset]))


(defn copy!
  "copy elem-count src items to dest items"
  ([src src-offset dest dest-offset elem-count options]
   (let [src-dtype (get-datatype src)
         src-offset (long src-offset)
         dest-dtype (get-datatype dest)
         dest-offset (long dest-offset)
         elem-count (long elem-count)]
     (shared-macros/check-range src src-offset elem-count)
     (shared-macros/check-range dest dest-offset elem-count)
     ((get-copy-fn dest dest-offset) src src-offset elem-count options)
     dest))
  ([src src-offset dst dst-offset elem-count]
   (copy! src src-offset dst dst-offset elem-count {:unchecked? false}))
  ([src dest]
   (copy! src 0 dest 0 (min (ecount dest) (ecount src)))))


(def ^:dynamic *error-on-slow-path* false)


(defn generic-copy!
  [item item-offset dest dest-offset elem-count options]
  (when *error-on-slow-path*
    (throw (ex-info "should not hit slow path"
                    {:src-type (type item)
                     :dest-type (type dest)})))
  (let [item-offset (long item-offset)
        dest-offset (long dest-offset)
        elem-count (long elem-count)]
    (c-for [idx 0 (< idx elem-count) (inc idx)]
           (set-value! dest (+ dest-offset idx)
                       (get-value item (+ item-offset idx))))
    dest))


(defprotocol PCopyRawData
  "Given a sequence of data copy it as fast as possible into a target item."
  (copy-raw->item! [raw-data ary-target target-offset]))


(defn copy-raw-seq->item!
  [raw-data-seq ary-target target-offset]
  (reduce (fn [[ary-target target-offset] new-raw-data]
            (copy-raw->item! new-raw-data ary-target target-offset))
          [ary-target target-offset]
          raw-data-seq))


(extend-protocol PCopyRawData
  Number
  (copy-raw->item! [raw-data ary-target ^long target-offset]
    (set-value! ary-target target-offset raw-data)
    [ary-target (+ target-offset 1)])

  clojure.lang.PersistentVector
  (copy-raw->item! [raw-data ary-target ^long target-offset]
    (let [num-elems (count raw-data)]
     (if (= 0 num-elems)
       [ary-target target-offset]
       (if (number? (raw-data 0))
         (do
          (c-for [idx 0 (< idx num-elems) (inc idx)]
                 (set-value! ary-target (+ idx target-offset) (raw-data idx)))
          [ary-target (+ target-offset num-elems)])
         (copy-raw-seq->item! raw-data ary-target target-offset)))))

  clojure.lang.ISeq
  (copy-raw->item! [raw-data ary-target target-offset]
    (copy-raw-seq->item! raw-data ary-target target-offset)))


(defn raw-dtype-copy!
  [raw-data ary-target ^long target-offset]
  (copy! raw-data 0 ary-target target-offset (ecount raw-data))
  [ary-target (+ target-offset ^long (ecount raw-data))])
