(ns tech.v2.datatype.fast-copy
  (:require [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.jna :as jna]
            [tech.v2.datatype.casting :as casting]
            [tech.parallel.for :as parallel-for]
            [tech.v2.datatype.nio-access :refer [buf-put buf-get
                                              datatype->list-read-fn]])
  (:import  [com.sun.jna Pointer]
            [it.unimi.dsi.fastutil.bytes ByteList ByteArrayList]
            [it.unimi.dsi.fastutil.shorts ShortList ShortArrayList]
            [it.unimi.dsi.fastutil.ints IntList IntArrayList]
            [it.unimi.dsi.fastutil.longs LongList LongArrayList]
            [it.unimi.dsi.fastutil.floats FloatList FloatArrayList]
            [it.unimi.dsi.fastutil.doubles DoubleList DoubleArrayList]
            [it.unimi.dsi.fastutil.booleans BooleanList BooleanArrayList]
            [it.unimi.dsi.fastutil.objects ObjectList ObjectArrayList]
            [tech.v2.datatype
             ObjectReader ObjectWriter
             ByteReader ByteWriter
             ShortReader ShortWriter
             IntReader IntWriter
             LongReader LongWriter
             FloatReader FloatWriter
             DoubleReader DoubleWriter
             BooleanReader BooleanWriter]
            [tech.v2.datatype.protocols PToReader PToWriter]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(jna/def-jna-fn (jna/c-library-name) memcpy
  "Copy bytes from one object to another"
  Pointer
  [dst typecast/ensure-ptr-like]
  [src typecast/ensure-ptr-like]
  [n-bytes int])


(defmacro parallel-slow-copy
  [datatype dst src unchecked?]
  (let [datatype (casting/safe-flatten datatype)]
    `(do
       (when-not (or (instance? PToWriter ~dst)
                     (dtype-proto/convertible-to-writer? ~dst))
         (throw (Exception.
                 (format "Destination object of type %s is not convertible to writer"
                         (type ~dst)))))
       (if (and (or (instance? PToReader ~src)
                    (dtype-proto/convertible-to-reader? ~src)))
         (let [src-reader# (typecast/datatype->reader ~datatype ~src ~unchecked?)
               dst-writer# (typecast/datatype->writer ~datatype ~dst ~unchecked?)
               n-elems# (.lsize dst-writer#)]
           (parallel-for/parallel-for
            idx# n-elems#
            (.write dst-writer# idx# (.read src-reader# idx#))))
         ;;Go the *much* slower iterator pathway
         (let [src-iter# (typecast/datatype->iter ~datatype ~src ~unchecked?)
               dst-writer# (typecast/datatype->writer ~datatype ~dst ~unchecked?)]
           (->> (range (.lsize dst-writer#))
                (map (fn [idx#]
                       [idx# (typecast/datatype->iter-next-fn ~datatype src-iter#)]))
                ;;Attempt to get at least little bit of parallelism.  Given iterators
                ;;implicitly make every item dependent upon the one before we can't
                ;;really do much here aside from potentially
                (pmap #(dst-writer# (first %) (second %)))
                dorun))))))


(defn parallel-slow-copy!
  [dst src & [unchecked?]]
  (case (casting/un-alias-datatype (dtype-proto/get-datatype dst))
    :int8 (parallel-slow-copy :int8 dst src unchecked?)
    :uint8 (parallel-slow-copy :uint8 dst src unchecked?)
    :int16 (parallel-slow-copy :int16 dst src unchecked?)
    :uint16 (parallel-slow-copy :uint16 dst src unchecked?)
    :int32 (parallel-slow-copy :int32 dst src unchecked?)
    :uint32 (parallel-slow-copy :uint32 dst src unchecked?)
    :int64 (parallel-slow-copy :int64 dst src unchecked?)
    :uint64 (parallel-slow-copy :uint64 dst src unchecked?)
    :float32 (parallel-slow-copy :float32 dst src unchecked?)
    :float64 (parallel-slow-copy :float64 dst src unchecked?)
    :boolean (parallel-slow-copy :boolean dst src unchecked?)
    (parallel-slow-copy :object dst src unchecked?))
  dst)


(defmacro impl-nio-write
  [datatype dst src unchecked?]
  `(let [src-reader# (typecast/datatype->reader ~(casting/safe-flatten datatype)
                                                ~src ~unchecked?)
         dst-buf# (typecast/datatype->buffer-cast-fn ~datatype ~dst)
         dst-pos# (.position dst-buf#)
         n-elems# (int (dtype-proto/ecount ~dst))]
     (parallel-for/parallel-for
      idx# n-elems#
      (buf-put dst-buf# idx# dst-pos#
               (casting/datatype->cast-fn
                ~(casting/safe-flatten datatype)
                ~datatype
                (.read src-reader# idx#))))))


(defn parallel-nio-write!
  [dst src & [unchecked?]]
  (case (dtype-proto/get-datatype dst)
    :int8 (impl-nio-write :int8 dst src unchecked?)
    :int16 (impl-nio-write :int16 dst src unchecked?)
    :int32 (impl-nio-write :int32 dst src unchecked?)
    :int64 (impl-nio-write :int64 dst src unchecked?)
    :float32 (impl-nio-write :float32 dst src unchecked?)
    :float64 (impl-nio-write :float64 dst src unchecked?)))


(defmacro impl-list-write
  [datatype dst src unchecked?]
  `(let [src-reader# (typecast/datatype->reader ~(casting/safe-flatten datatype)
                                                ~src ~unchecked?)
         dst-buf# (typecast/datatype->list-cast-fn ~datatype ~dst)
         n-elems# (int (dtype-proto/ecount ~dst))]
     (parallel-for/parallel-for
      idx# n-elems#
      (.set dst-buf# idx# (casting/datatype->cast-fn
                           ~(casting/safe-flatten datatype)
                           ~datatype
                           (.read src-reader# idx#))))))


(defn parallel-list-write!
  [dst src & [unchecked?]]
  (case (dtype-proto/get-datatype dst)
    :int8 (impl-list-write :int8 dst src unchecked?)
    :int16 (impl-list-write :int16 dst src unchecked?)
    :int32 (impl-list-write :int32 dst src unchecked?)
    :int64 (impl-list-write :int64 dst src unchecked?)
    :float32 (impl-list-write :float32 dst src unchecked?)
    :float64 (impl-list-write :float64 dst src unchecked?)
    :boolean (impl-list-write :boolean dst src unchecked?)
    (impl-list-write :object dst src unchecked?)))


(defn parallel-write!
  [item src & [unchecked?]]
  (let [item-dtype (cond-> (dtype-proto/get-datatype item)
                     unchecked?
                     casting/datatype->host-datatype)
        item-buf (typecast/as-nio-buffer item)
        item-list (typecast/as-list item)]
    (cond
      (and item-buf (= item-dtype (dtype-proto/get-datatype item-buf)))
      (parallel-nio-write! item src unchecked?)
      (and item-list (= item-dtype (dtype-proto/get-datatype item-list)))
      (parallel-list-write! item src unchecked?)
      :else
      (parallel-slow-copy! item src unchecked?))))


(defmacro impl-nio-read
  [datatype dst src unchecked?]
  `(let [src-buf# (typecast/datatype->buffer-cast-fn ~datatype ~src)
         dst-writer# (typecast/datatype->writer ~(casting/safe-flatten datatype)
                                                ~dst ~unchecked?)
         src-pos# (.position src-buf#)
         n-elems# (int (dtype-proto/ecount ~src))]
     (parallel-for/parallel-for
      idx# n-elems#
      (.write dst-writer# idx# (buf-get src-buf# idx# src-pos#)))))


(defn parallel-nio-read!
  [dst src & [unchecked?]]
  (case (dtype-proto/get-datatype src)
    :int8 (impl-nio-read :int8 dst src unchecked?)
    :int16 (impl-nio-read :int16 dst src unchecked?)
    :int32 (impl-nio-read :int32 dst src unchecked?)
    :int64 (impl-nio-read :int64 dst src unchecked?)
    :float32 (impl-nio-read :float32 dst src unchecked?)
    :float64 (impl-nio-read :float64 dst src unchecked?)))


(defmacro impl-list-read
  [datatype dst src unchecked?]
  `(let [dst-writer# (typecast/datatype->writer ~(casting/safe-flatten datatype)
                                                ~dst ~unchecked?)
         src-buf# (typecast/datatype->list-cast-fn ~datatype ~src)
         n-elems# (int (dtype-proto/ecount ~src))]
     (parallel-for/parallel-for
      idx# n-elems#
      (.write dst-writer# idx# (datatype->list-read-fn ~datatype src-buf# idx#)))))


(defn parallel-list-read!
  [dst src & [unchecked?]]
  (case (dtype-proto/get-datatype src)
    :int8 (impl-list-read :int8 dst src unchecked?)
    :int16 (impl-list-read :int16 dst src unchecked?)
    :int32 (impl-list-read :int32 dst src unchecked?)
    :int64 (impl-list-read :int64 dst src unchecked?)
    :float32 (impl-list-read :float32 dst src unchecked?)
    :float64 (impl-list-read :float64 dst src unchecked?)
    :boolean (impl-list-read :boolean dst src unchecked?)
    (impl-list-read :object dst src unchecked?)))


(defn parallel-read!
  [item src & [unchecked?]]
  (let [src-dtype (dtype-proto/get-datatype src)
        src-buf (typecast/as-nio-buffer src)
        src-list (typecast/as-list src)]
    (cond
      (and src-buf (= src-dtype (dtype-proto/get-datatype src-buf)))
      (parallel-nio-read! item src unchecked?)
      (and src-list (= src-dtype (dtype-proto/get-datatype src-list)))
      (parallel-list-read! item src unchecked?)
      :else
      (parallel-slow-copy! item src unchecked?))))


(defn copy!
  "Copy defined when both things are convertible to concrete types, and the types
  of those concrete types exactly match."
  [dst src]
  (let [dst-ptr (typecast/as-ptr dst)
        src-ptr (typecast/as-ptr src)
        src-buf (typecast/as-nio-buffer src)
        dst-buf (typecast/as-nio-buffer dst)
        src-list (typecast/as-list src)
        dst-list (typecast/as-list dst)
        _ (when-not (and (or src-buf src-list)
                         (or dst-buf dst-list))
            (throw (ex-info "convertible to list or nio"
                            {})))
        src-dtype (dtype-proto/get-datatype (or src-buf src-list))
        dst-dtype (dtype-proto/get-datatype (or dst-buf dst-list))
        src-ary (when src-buf (typecast/as-array src-buf))
        dst-ary (when dst-buf (typecast/as-array dst-buf))
        n-elems (long (dtype-proto/ecount dst))]
    (when-not (= src-dtype dst-dtype)
      (throw (ex-info "Fast copy called inappropriately; datatypes do not match"
                      {:src-datatype src-dtype
                       :dst-datatype dst-dtype})))
    (cond
      ;;Very fast path
      (and dst-ptr src-ary)
      (let [{:keys [java-array offset]} src-ary
            array-offset (int offset)]
        (case src-dtype
          :int8 (.write dst-ptr 0 ^bytes java-array array-offset n-elems)
          :int16 (.write dst-ptr 0 ^shorts java-array array-offset n-elems)
          :int32 (.write dst-ptr 0 ^ints java-array array-offset n-elems)
          :int64 (.write dst-ptr 0 ^longs java-array array-offset n-elems)
          :float32 (.write dst-ptr 0 ^floats java-array array-offset n-elems)
          :float64 (.write dst-ptr 0 ^doubles java-array array-offset n-elems)))
      (and dst-ary src-ptr)
      (let [{:keys [java-array offset]} dst-ary
            array-offset (int offset)]
        (case src-dtype
          :int8 (.read src-ptr 0 ^bytes java-array array-offset n-elems)
          :int16 (.read src-ptr 0 ^shorts java-array array-offset n-elems)
          :int32 (.read src-ptr 0 ^ints java-array array-offset n-elems)
          :int64 (.read src-ptr 0 ^longs java-array array-offset n-elems)
          :float32 (.read src-ptr 0 ^floats java-array array-offset n-elems)
          :float64 (.read src-ptr 0 ^doubles java-array array-offset n-elems)))
      ;;Turns out that system/arraycopy is *really* damn fast.
      (and dst-ary src-ary)
      (let [{src-array :java-array
             src-offset :offset} src-ary
            {dst-array :java-array
             dst-offset :offset} dst-ary]
        (System/arraycopy src-array src-offset dst-array dst-offset n-elems))
      (and src-buf dst-buf)
      (memcpy dst-buf src-buf (* n-elems (casting/numeric-byte-width src-dtype)))
      (and src-list dst-ary)
      (let [{:keys [java-array offset]} dst-ary
            array-offset (int offset)]
        (case src-dtype
          :int8 (.getElements ^ByteList src-list 0
                              ^bytes java-array array-offset n-elems)
          :int16 (.getElements ^ShortList src-list 0
                               ^shorts java-array array-offset n-elems)
          :int32 (.getElements ^IntList src-list 0
                               ^ints java-array array-offset n-elems)
          :int64 (.getElements ^LongList src-list 0
                               ^longs java-array array-offset n-elems)
          :float32 (.getElements ^FloatList src-list 0
                                 ^floats java-array array-offset n-elems)
          :float64 (.getElements ^DoubleList src-list 0
                                 ^doubles java-array array-offset n-elems)
          :boolean (.getElements ^BooleanList src-list 0
                                 ^booleans java-array array-offset n-elems)
          (.getElements ^ObjectList src-list 0
                        (typecast/as-object-array java-array) array-offset n-elems)))
      dst-buf
      (parallel-nio-write! dst-buf src true)
      dst-list
      (parallel-list-write! dst-list src true)
      src-buf
      (parallel-nio-read! dst-buf src true)
      src-list
      (parallel-list-read! dst-buf src true)
      :else
      (parallel-slow-copy! dst src true)))
  dst)
