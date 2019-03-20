(ns tech.datatype.base
  "Datatype library primitives shared between clojurescript and clojure.  The datatype
  system is an extensible system to provide understanding of and access to an undefined
  set of datatypes and containers that hold contiguous sections of those datatypes."
  (:refer-clojure :exclude [cast])
  (:require [tech.datatype.base-macros :as base-macros]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting :as casting]
            [tech.datatype.io :as dtype-io]
            [clojure.core.matrix.macros :refer [c-for]]
            [clojure.core.matrix :as m])
  (:import [tech.datatype ObjectReader ObjectWriter]))


(set! *warn-on-reflection* true)

(defn shape->ecount
  ^long [shape-or-num]
  (if (number? shape-or-num)
    (long shape-or-num)
    (do
      (when-not (seq shape-or-num)
        (throw (ex-info "Shape appears to not be a shape"
                        {:shape shape-or-num})))
      (when-not (> (count shape-or-num) 0)
        (throw (ex-info "Empty shape is meaningless" {:shape shape-or-num})))
      (long (apply * shape-or-num)))))


(defn datatype->byte-size
  ^long [datatype]
  (casting/numeric-byte-width datatype))


(defn set-value! [item offset value]
  (.write ^ObjectWriter (dtype-proto/->writer-of-type item :object false)
          offset value))

(defn set-constant! [item offset value elem-count]
  (.writeConstant ^ObjectWriter (dtype-proto/->writer-of-type item :object false)
                  offset value elem-count))

(defn get-value [item offset]
  (.read ^ObjectReader (dtype-proto/->reader-of-type item :object false) offset))


(defn ->vector
  [item]
  (dtype-proto/->vector item))


(defn ecount
  "Type hinted ecount."
  ^long [item]
  (m/ecount item))


(defn shape
  [item]
  (if (nil? item)
    nil
    (or (m/shape item) [(ecount item)])))


(defn get-datatype
  [item]
  (dtype-proto/get-datatype item))


(defn make-container
  ([container-type datatype elem-seq options]
   (dtype-proto/make-container container-type datatype
                               elem-seq options))
  ([container-type datatype elem-seq]
   (dtype-proto/make-container container-type datatype elem-seq {})))


(defn copy!
  "copy elem-count src items to dest items.  Options may contain unchecked in which you
  get unchecked operations."
  ([src src-offset dest dest-offset elem-count options]
   (base-macros/check-range src src-offset elem-count)
   (base-macros/check-range dest dest-offset elem-count)
   (let [src (dtype-proto/sub-buffer src src-offset elem-count)
         dest (dtype-proto/sub-buffer dest dest-offset elem-count)]
     (dtype-io/dense-copy! dest src elem-count
                           (:unchecked? options)
                           (:parallel? options)))
   dest)
  ([src src-offset dst dst-offset elem-count]
   (copy! src src-offset dst dst-offset elem-count {:unchecked? false}))
  ([src dest]
   (copy! src 0 dest 0 (min (ecount dest) (ecount src)))))


(defn copy-raw-seq->item!
  [raw-data-seq ary-target target-offset options]
  (reduce (fn [[ary-target target-offset] new-raw-data]
            (dtype-proto/copy-raw->item! new-raw-data ary-target target-offset options))
          [ary-target target-offset]
          raw-data-seq))


(defn raw-dtype-copy!
  [raw-data ary-target target-offset options]
  (let [elem-count (ecount raw-data)]
    (copy! raw-data 0 ary-target target-offset elem-count options)
    [ary-target (+ (long target-offset) elem-count)]))


(extend-protocol dtype-proto/PCopyRawData
  Number
  (copy-raw->item! [raw-data ary-target ^long target-offset options]
    (set-value! ary-target target-offset raw-data)
    [ary-target (+ target-offset 1)])

  clojure.lang.PersistentVector
  (copy-raw->item! [raw-data ary-target ^long target-offset options]
    (let [num-elems (count raw-data)]
     (if (= 0 num-elems)
       [ary-target target-offset]
       (if (number? (raw-data 0))
         (do
          (c-for [idx 0 (< idx num-elems) (inc idx)]
                 (set-value! ary-target (+ idx target-offset) (raw-data idx)))
          [ary-target (+ target-offset num-elems)])
         (copy-raw-seq->item! raw-data ary-target target-offset options)))))

  clojure.lang.ISeq
  (copy-raw->item! [raw-data ary-target target-offset options]
    (copy-raw-seq->item! raw-data ary-target target-offset options))
  java.lang.Iterable
  (copy-raw->item! [raw-data ary-target target-offset options]
    (copy-raw-seq->item! (seq raw-data) ary-target target-offset options)))


(extend-type Object
  dtype-proto/PCopyRawData
  (copy-raw->item!
   [src-data dst-data offset options]
    (dtype-proto/copy-raw->item! (seq src-data) dst-data offset options))
  dtype-proto/PPersistentVector
  (->vector [src]
    (if (satisfies? dtype-proto/PToArray src)
      (vec (or (dtype-proto/->array src)
               (dtype-proto/->array-copy src)))))
  dtype-proto/PToNioBuffer
  (->buffer-backing-store [src]
    (when (satisfies? dtype-proto/PToArray src)
      (when-let [ary-data (dtype-proto/->array src)]
        (dtype-proto/->buffer-backing-store src))))
  dtype-proto/PToReader
  (->object-reader [item]
    (reify ObjectReader
      (read [item-reader idx]
        (cond
          (or (map? item)
              (vector? item))
          (do
            (when-not (contains? item idx)
              (throw (ex-info "Item has no idx entry"
                              {:item item
                               :idx idx})))
            (item idx))
          (fn? item)
          (item idx)
          :else
          (do
            (when-not (= 0 idx)
              (throw (ex-info "Generic index access must be 0"
                              {:item item
                               :idx idx})))
            item)))
      (readBlock [item-reader off dest]
        (let [n-elems (.size dest)]
          (c-for [idx (int 0) (< idx n-elems) (inc idx)]
                 (.set dest idx (.read item-reader (+ off idx))))
          dest))
      (readIndexes [item-reader idx-buf dest]
        (let [n-elems (ecount idx-buf)
              buf-pos (.position idx-buf)]
          (c-for [idx (int 0) (< idx n-elems) (inc idx)]
                 (.set dest idx (.read item-reader
                                       (.get idx-buf
                                             (+ idx buf-pos))))))
        dest)))
  (->reader-of-type [item datatype unchecked?]
    (-> (dtype-proto/->reader-of-type item :object unchecked?)
        (dtype-proto/->reader-of-type datatype unchecked?)))
  dtype-proto/PClone
  (clone [item datatype]
    (copy! item (dtype-proto/from-prototype item datatype
                                            (shape item)))))
