(ns tech.v2.datatype.base
  "Datatype library primitives shared between clojurescript and clojure.  The datatype
  system is an extensible system to provide understanding of and access to an undefined
  set of datatypes and containers that hold contiguous sections of those datatypes."
  (:refer-clojure :exclude [cast])
  (:require [tech.v2.datatype.base-macros :as base-macros]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.shape :as dtype-shape]
            [tech.v2.datatype.io :as dtype-io]
            [tech.parallel.for :as parallel-for])
  (:import [tech.v2.datatype ObjectReader ObjectWriter
            ByteReader ShortReader IntReader LongReader
            FloatReader DoubleReader BooleanReader
            ObjectMutable]
           [java.util List RandomAccess]))


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
  ((dtype-proto/as-writer item {:datatype :object}) offset value)
  item)

(defn set-constant! [item offset value elem-count]
  (dtype-proto/set-constant! item offset value elem-count)
  item)

(defn get-value [item offset]
  (cond
    (instance? RandomAccess item)
    (.get ^List item (int offset))
    (dtype-proto/convertible-to-reader? item)
    ((dtype-proto/->reader item {}) offset)
    (map? item)
    (item offset)
    (= offset 0)
    item
    :else
    (throw (ex-info "Cannot get value of item at offset" {:item item :offset offset}))))


(defn ->vector
  [item]
  (if (vector? item)
    item
    (-> (or (dtype-proto/as-reader item)
            (dtype-proto/as-iterable item))
        vec)))


(defn ecount
  "Type hinted ecount."
  ^long [item]
  (dtype-shape/ecount item))


(defn shape
  [item]
  (dtype-shape/shape item))


(defn get-datatype
  [item]
  (dtype-proto/get-datatype item))


(defn make-container
  ([container-type datatype elem-seq options]
   (dtype-proto/make-container container-type datatype
                               elem-seq options))
  ([container-type datatype elem-seq]
   (dtype-proto/make-container container-type datatype elem-seq {})))


(defn sub-buffer
  [item off len]
  (when-not (<= (+ (int off) (int len))
                (ecount item))
    (throw (ex-info "Sub buffer out of range." {})))
  (if (and (= (int off) 0)
           (= (int len) (ecount item)))
    item
    (dtype-proto/sub-buffer item off len)))

(defn- requires-sub-buffer
  [item off len]
  (not (and (= (int off) 0)
            (= (int len) (ecount item)))))

(defn copy!
  "copy elem-count src items to dest items.  Options may contain unchecked in which you
  get unchecked operations."
  ([src src-offset dest dest-offset elem-count options]
   (base-macros/check-range src src-offset elem-count)
   (base-macros/check-range dest dest-offset elem-count)
   ;;Src may be iterable in which case there is no sub buffer
   ;;option available.
   (let [src (if (requires-sub-buffer src src-offset elem-count)
               (sub-buffer (dtype-proto/->reader src {})
                           src-offset elem-count)
               src)
         dest (if (requires-sub-buffer dest dest-offset elem-count)
                (sub-buffer (dtype-proto/->writer dest {})
                            dest-offset elem-count)
                dest)]
     (dtype-proto/copy! dest src options))
   dest)
  ([src src-offset dst dst-offset elem-count]
   (copy! src src-offset dst dst-offset elem-count {:unchecked? false}))
  ([src dest]
   (copy! src 0 dest 0 (min (ecount dest) (ecount src)))))


(defn write-block!
  [item offset values & [options]]
  (copy! values 0 item offset (ecount values) options))


(defn read-block!
  [item offset values & [options]]
  (copy! item offset values 0 (ecount values) options))


(defn write-indexes!
  [item indexes values & [options]]
  (dtype-proto/write-indexes! item indexes values options))


(defn read-indexes!
  [item indexes values & [options]]
  (dtype-proto/read-indexes! item indexes values options))


(defn remove-range!
  [item idx count]
  (dtype-proto/remove-range! item idx count))


(defn insert-block!
  [item idx values & [options]]
  (dtype-proto/insert-block! item idx values options))


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


(defn op-name
  [operator]
  (if (satisfies? dtype-proto/POperator operator)
    (dtype-proto/op-name operator)
    :unnamed))


(defn buffer-type
  [item]
  (dtype-proto/safe-buffer-type item))


(extend-protocol dtype-proto/PCopyRawData
  Number
  (copy-raw->item! [raw-data ary-target ^long target-offset options]
    (set-value! ary-target target-offset raw-data)
    [ary-target (+ target-offset 1)])

  RandomAccess
  (copy-raw->item! [raw-data ary-target ^long target-offset options]
    (let [^List raw-data raw-data
          num-elems (.size raw-data)]
      (if (= 0 num-elems)
        [ary-target target-offset]
        (if (number? (.get raw-data 0))
          (do
            (parallel-for/parallel-for
             idx num-elems
             (set-value! ary-target (+ idx target-offset) (.get raw-data idx)))
            [ary-target (+ target-offset num-elems)])
          (copy-raw-seq->item! raw-data ary-target target-offset options)))))

  java.lang.Iterable
  (copy-raw->item! [raw-data ary-target target-offset options]
    (copy-raw-seq->item! (seq raw-data) ary-target target-offset options))

  Object
  (copy-raw->item! [raw-data ary-target offset options]
    (if-let [reader (dtype-proto/as-reader raw-data)]
      (raw-dtype-copy! reader ary-target offset options)
      (if-let [base-type (dtype-proto/as-base-type raw-data)]
        (raw-dtype-copy! base-type ary-target offset options)
        (throw (ex-info "Raw copy not supported on object" {}))))))


(extend-protocol dtype-proto/PBuffer
  List
  (sub-buffer [item offset length]
    (let [^List item item
          offset (int offset)
          length (int length)]
      (.subList item offset (+ offset length)))))


(extend-protocol dtype-proto/PToReader
  RandomAccess
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (let [^List item item
          item-count (.size item)]
      (-> (reify ObjectReader
            (getDatatype [_] :object)
            (lsize [_] item-count)
            (read [_ idx]
              (.get item idx)))
          (dtype-proto/->reader options)))))


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
  (copy-raw->item! [raw-data ary-target target-offset options]
    (copy-raw-seq->item! (seq raw-data) ary-target target-offset options))


  dtype-proto/PSetConstant
  (set-constant! [item offset value n-elems]
    (let [n-elems (int n-elems)
          offset (int offset)
          item-dtype (dtype-proto/get-datatype item)
          value (casting/cast value item-dtype)
          ^ObjectWriter writer (dtype-proto/->writer item {:datatype :object})]
      (parallel-for/parallel-for
       idx n-elems
       (.write writer idx value))))


  dtype-proto/PWriteIndexes
  (write-indexes! [item indexes values options]
    (dtype-io/write-indexes! item indexes values options))

  dtype-proto/PReadIndexes
  (write-indexes! [item indexes values options]
    (dtype-io/read-indexes! item indexes values options))

  dtype-proto/PClone
  (clone [item datatype]
    (copy! item (dtype-proto/from-prototype item datatype
                                            (shape item)))))


(defn item-inclusive-range
  [item-reader]
  (let [item-ecount (ecount item-reader)]
    (if (= 0 item-ecount)
      [0 0]
      [(get-value item-reader 0)
       (get-value item-reader (- item-ecount 1))])))
