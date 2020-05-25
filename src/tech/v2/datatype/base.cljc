(ns tech.v2.datatype.base
  "Datatype library primitives shared between clojurescript and clojure.  The datatype
  system is an extensible system to provide understanding of and access to an undefined
  set of datatypes and containers that hold contiguous sections of those datatypes."
  (:refer-clojure :exclude [cast])
  (:require [tech.v2.datatype.base-macros :as base-macros]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.shape :as dtype-shape]
            [tech.v2.datatype.argtypes :as argtypes]
            [tech.v2.datatype.io :as dtype-io]
            [tech.parallel.for :as parallel-for])
  (:import [tech.v2.datatype ObjectReader ObjectWriter
            ByteReader ShortReader IntReader LongReader
            FloatReader DoubleReader BooleanReader
            ObjectMutable]
           [java.lang.reflect Method]
           [clojure.lang IPersistentVector]
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


(defn operation-type
  [item]
  (when item
    (dtype-proto/operation-type item)))


(defn make-container
  ([container-type datatype elem-seq options]
   (dtype-proto/make-container container-type datatype
                               elem-seq options))
  ([container-type datatype elem-seq]
   (dtype-proto/make-container container-type datatype elem-seq {})))


(defn sub-buffer
  ([item off len]
   (when-not (<= (+ (int off) (int len))
                 (ecount item))
     (throw (ex-info "Sub buffer out of range." {})))
   (if (and (= (int off) 0)
            (= (int len) (ecount item)))
     item
     (dtype-proto/sub-buffer item off len)))
  ([item off]
   (sub-buffer item off (- (ecount item) (long off)))))

(defn- requires-sub-buffer
  [item off len]
  (not (and (= (int off) 0)
            (= (int len) (ecount item)))))


(defn ->byte-array
  ^bytes [item]
  (let [item (or (dtype-proto/->array item) item)]
    (if (instance? (Class/forName "[B") item)
      item
      (make-container :java-array :int8 item))))


(defn ->short-array
  ^shorts [item]
  (let [item (or (dtype-proto/->array item) item)]
    (if (instance? (Class/forName "[S") item)
      item
      (make-container :java-array :int16 item))))


(defn ->int-array
  ^ints [item]
  (let [item (or (dtype-proto/->array item) item)]
    (if (instance? (Class/forName "[I") item)
      item
      (make-container :java-array :int32 item))))


(defn ->long-array
  ^longs [item]
  (let [item (or (dtype-proto/->array item) item)]
    (if (instance? (Class/forName "[J") item)
      item
      (make-container :java-array :int64 item))))


(defn ->float-array
  ^floats [item]
  (let [item (or (dtype-proto/->array item) item)]
    (if (instance? (Class/forName "[F") item)
      item
      (make-container :java-array :float32 item))))


(defn ->double-array
  ^doubles [item]
  (let [item (or (dtype-proto/->array item) item)]
    (if (instance? (Class/forName "[D") item)
      item
      (make-container :java-array :float64 item))))


(defn ->reader
  "Create a reader of a specific type."
  [src-item & [datatype options]]
  (dtype-proto/->reader src-item
                        (assoc options
                               :datatype
                               (or datatype (get-datatype src-item)))))


(defn ->writer
  "Create a writer of a specific type."
  [src-item & [datatype options]]
  (dtype-proto/->writer src-item
                        (assoc options
                               :datatype
                               (or datatype (get-datatype src-item)))))


(defn ->iterable
  "Create an object that when .iterator is called it returns an iterator that
  derives from tech.v2.datatype.{dtype}Iter.  This iterator class adds 'current'
  to the fastutil typed iterator of the same type.  Current makes implementing
  a few algorithms far easier as they no longer require local state outside of
  the iterator."
  [src-item & [datatype options]]
  (if (and (not datatype)
           (instance? Iterable src-item))
    src-item
    (dtype-proto/->iterable src-item
                            (assoc options
                                   :datatype
                                   (or datatype (get-datatype src-item))))))


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
  (let [writer (dtype-proto/->writer ary-target options)]
    (reduce (fn [[ary-target target-offset] new-raw-data]
              ;;Fastpath for sequences of numbers.  Avoids more protocol pathways.
              (if (number? new-raw-data)
                (do
                  (writer target-offset new-raw-data)
                  [ary-target (inc target-offset)])
                ;;slow path if we didn't recognize the thing.
                (dtype-proto/copy-raw->item! new-raw-data ary-target
                                             target-offset options)))
            [ary-target target-offset]
            raw-data-seq)))


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

  Object
  (copy-raw->item! [raw-data ary-target ^long target-offset options]
    (cond
      (dtype-proto/convertible-to-reader? raw-data)
      (let [src-reader (dtype-proto/as-reader raw-data)]
        (if (or (not= :object (casting/flatten-datatype
                               (get-datatype src-reader)))
                (= :object (casting/flatten-datatype (get-datatype ary-target))))
          (raw-dtype-copy! src-reader ary-target target-offset options)
          (copy-raw-seq->item! (seq raw-data) ary-target target-offset options)))
      (instance? RandomAccess raw-data)
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
            (copy-raw-seq->item! raw-data ary-target target-offset options))))
      (instance? java.lang.Iterable raw-data)
      (copy-raw-seq->item! (seq raw-data) ary-target
                           target-offset options)

      :else
      (do
        (set-value! ary-target target-offset raw-data)
        [ary-target (+ target-offset 1)]))))


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
          item-count (.size item)
          item-dtype (get-datatype item)]
      (-> (reify ObjectReader
            (getDatatype [_] item-dtype)
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


(extend-protocol dtype-proto/PToWriter
  RandomAccess
  (convertible-to-writer? [item]
    (not (instance? IPersistentVector item)))
  (->writer [item options]
    (let [^List item item
          item-count (.size item)
          item-dtype (get-datatype item)]
      (-> (reify ObjectWriter
            (getDatatype [_] item-dtype)
            (lsize [_] item-count)
            (write [_ idx val]
              (.set item idx val)))
          (dtype-proto/->writer options)))))


(extend-type Object
  dtype-proto/PToArray
  (->sub-array [item] nil)
  (->array-copy [item]
    (let [retval
          (make-container :java-array (get-datatype item) (ecount item) {})]
      (copy! item retval)))


  dtype-proto/PSetConstant
  (set-constant! [item offset value n-elems]
    (let [n-elems (int n-elems)
          offset (int offset)
          item-dtype (dtype-proto/get-datatype item)
          value (casting/cast value item-dtype)
          ^ObjectWriter writer (dtype-proto/->writer item {:datatype :object})]
      (parallel-for/parallel-for
       idx n-elems
       (.write writer (+ idx offset) value))))


  dtype-proto/PWriteIndexes
  (write-indexes! [item indexes values options]
    (dtype-io/write-indexes! item indexes values options))

  dtype-proto/PReadIndexes
  (write-indexes! [item indexes values options]
    (dtype-io/read-indexes! item indexes values options))

  dtype-proto/PClone
  (clone [item]
    (if (and (instance? java.lang.Cloneable item)
             (not (.isArray (.getClass ^Object item))))
      (let [^Class item-cls (class item)
            ^Method method
            (.getMethod item-cls
                        "clone"
                        ^"[Ljava.lang.Class;" (into-array Class []))]
        (.invoke method item (object-array 0)))
      (copy! item (dtype-proto/from-prototype item
                                              (get-datatype item)
                                              (shape item)))))
  dtype-proto/PRichDatatype
  (get-rich-datatype [item]
    (let [item-dtype (dtype-proto/get-datatype item)]
      (case (argtypes/arg->arg-type item)
        :scalar item-dtype
        :iterable {:container-type :iterable
                   :datatype item-dtype}
        :reader {:container-type :reader
                 :shape (shape item)
                 :datatype item-dtype}))))


(defn item-inclusive-range
  [item-reader]
  (let [item-ecount (ecount item-reader)]
    (if (= 0 item-ecount)
      [0 0]
      [(get-value item-reader 0)
       (get-value item-reader (- item-ecount 1))])))


(extend-type clojure.lang.PersistentVector
  dtype-proto/PClone
  (clone [item] item))
