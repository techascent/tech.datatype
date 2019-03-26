(ns tech.datatype.base
  "Datatype library primitives shared between clojurescript and clojure.  The datatype
  system is an extensible system to provide understanding of and access to an undefined
  set of datatypes and containers that hold contiguous sections of those datatypes."
  (:refer-clojure :exclude [cast])
  (:require [tech.datatype.base-macros :as base-macros]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting :as casting]
            [tech.datatype.typecast :as typecast]
            [tech.datatype.io :as dtype-io]
            [clojure.core.matrix.macros :refer [c-for]]
            [clojure.core.matrix :as m]
            [clojure.core.matrix.protocols :as mp])
  (:import [tech.datatype ObjectReader ObjectWriter
            ByteReader ShortReader IntReader LongReader
            FloatReader DoubleReader BooleanReader]
           [java.util List]))


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
  (dtype-proto/set-constant! item offset value elem-count))

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


(defn write-block!
  [item offset values & [options]]
  (dtype-proto/write-block! item offset values options))


(defn read-block!
  [item offset values & [options]]
  (dtype-proto/read-block! item offset values options))


(defn write-indexes!
  [item indexes values & [options]]
  (dtype-proto/write-indexes! item indexes values options))


(defn read-indexes!
  [item indexes values & [options]]
  (dtype-proto/read-indexes! item indexes values options))


(defn copy!
  "copy elem-count src items to dest items.  Options may contain unchecked in which you
  get unchecked operations."
  ([src src-offset dest dest-offset elem-count options]
   (base-macros/check-range src src-offset elem-count)
   (base-macros/check-range dest dest-offset elem-count)
   (let [src (dtype-proto/sub-buffer src src-offset elem-count)
         dest (dtype-proto/sub-buffer dest dest-offset elem-count)]
     (dtype-io/dense-copy! dest src (:unchecked? options)))
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

  List
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


(extend-protocol dtype-proto/PToReader
  List
  (->reader-of-type [item datatype unchecked?]
    (let [item-count (count item)]
      (-> (reify ObjectReader
            (getDatatype [_] :object)
            (size [_] (int (or
                            (count item)
                            0)))
            (read [item-reader idx]
              (item idx))
            (invoke [item-reader idx]
              (item idx)))
          (dtype-proto/->reader-of-type datatype unchecked?)))))


(defmacro extend-reader-type
  [reader-type]
  `(clojure.core/extend
       ~reader-type
     dtype-proto/PCopyRawData
     {:copy-raw->item!
      (fn [raw-data# ary-target# target-offset# options#]
        (raw-dtype-copy! raw-data# ary-target# target-offset# options#))}))


(extend-reader-type ByteReader)
(extend-reader-type ShortReader)
(extend-reader-type IntReader)
(extend-reader-type LongReader)
(extend-reader-type FloatReader)
(extend-reader-type DoubleReader)
(extend-reader-type BooleanReader)


(extend-type Object
  dtype-proto/PCopyRawData
  (copy-raw->item!
   [src-data dst-data offset options]
    (dtype-proto/copy-raw->item! (seq src-data) dst-data offset options))
  dtype-proto/PPersistentVector
  (->vector [src]
    (if (satisfies? dtype-proto/PToReader src)
      ;;Readers implement iterable which gives them seq and friends
      (vec (dtype-proto/->reader-of-type
            src (dtype-proto/get-datatype src) true))))

  dtype-proto/PSetConstant
  (set-constant! [item offset value n-elems]
    (let [n-elems (int n-elems)
          offset (int offset)
          item-dtype (dtype-proto/get-datatype item)
          value (casting/cast value item-dtype)
          ^ObjectWriter writer (dtype-proto/->writer-of-type item :object false)]
      (c-for [idx (int 0) (< idx n-elems) (inc idx)]
             (.write writer idx value))))


  dtype-proto/PWriteBlock
  (write-block! [item offset values options]
    (copy! values 0 item offset (ecount values) options))

  dtype-proto/PReadBlock
  (read-block! [item offset values options]
    (copy! item offset values 0 (ecount values) options))

  dtype-proto/PWriteIndexes
  (write-indexes! [item indexes values options]
    (dtype-io/write-indexes! (dtype-proto/get-datatype item)
                             item indexes values options))

  dtype-proto/PReadIndexes
  (write-indexes! [item indexes values options]
    (dtype-io/read-indexes! (dtype-proto/get-datatype item)
                            item indexes values options))


  dtype-proto/PClone
  (clone [item datatype]
    (copy! item (dtype-proto/from-prototype item datatype
                                            (shape item)))))
