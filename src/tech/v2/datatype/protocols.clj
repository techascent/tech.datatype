(ns tech.v2.datatype.protocols
  (:require [tech.v2.datatype.casting :as casting]
            [tech.jna :as jna])
  (:import [tech.v2.datatype Datatype Countable
            ObjectIter IteratorObjectIter
            ObjectReader ObjectWriter]
           [com.sun.jna Pointer]
           [java.lang.reflect Method]
           [java.util List]
           [java.nio ByteOrder
            ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer CharBuffer]))


(set! *warn-on-reflection* true)


(defprotocol PDatatype
  (get-datatype [item]))


(defprotocol PRichDatatype
  (get-rich-datatype [item]))


(defprotocol PSetDatatype
  (set-datatype [item new-dtype]))


(extend-type Datatype
  PDatatype
  (get-datatype [item] (.getDatatype item)))


(defprotocol POperationType
  (operation-type [item]))


(defprotocol PCountable
  (ecount [item]))


(extend-type Countable
  PCountable
  (ecount [item] (.lsize item)))


(defprotocol PShape
  (shape [item]))


(defprotocol PCopyRawData
  "Given a sequence of data copy it as fast as possible into a target item."
  (copy-raw->item! [raw-data ary-target target-offset options]))


(defprotocol PPrototype
  (from-prototype [item datatype shape]))


(defprotocol PClone
  "Clone an object.  Implemented generically for all objects."
  (clone [item]))


(defprotocol PBufferType ;;:sparse or :dense, :tensor, :array-of-structs
  (buffer-type [item]))


(defn safe-buffer-type
  [item]
  (buffer-type item))


(defprotocol PSetConstant
  (set-constant! [item offset value elem-count]))

(defprotocol PWriteIndexes
  (write-indexes! [item indexes values options]))

(defprotocol PReadIndexes
  (read-indexes! [item indexes values options]))

(defprotocol PRemoveRange
  (remove-range! [item idx n-elems]))

(defprotocol PInsertBlock
  (insert-block! [item idx values options]))


(defprotocol PToBackingStore
  "Necessary only for checking that things aren't reading/writing to same backing store
  object."
  (->backing-store-seq [item]))


(defprotocol PToNioBuffer
  "Take a 'thing' and convert it to a nio buffer.  Only valid if the thing
  shares the backing store with the buffer.  Result may not exactly
  represent the value of the item itself as the backing store may require
  element-by-element conversion to represent the value of the item."
  (convertible-to-nio-buffer? [item])
  (->buffer-backing-store [item]))


(defprotocol PToNativeBuffer
  (convertible-to-native-buffer? [buf])
  (->native-buffer [buf]))


(defprotocol PToJNAPointer
  (convertible-to-data-ptr? [item])
  (->jna-ptr [item]))


(defn as-jna-ptr
  ^Pointer [item]
  (when (and item (convertible-to-data-ptr? item))
    (->jna-ptr item)))


(defn nio-convertible?
  [item]
  (convertible-to-nio-buffer? item))


(defn as-nio-buffer
  [item]
  (when (nio-convertible? item)
    (->buffer-backing-store item)))


(defprotocol PNioBuffer
  (position [item])
  (limit [item])
  (array-backed? [item]))


(defprotocol PBuffer
  "Interface to create sub-buffers out of larger contiguous buffers."
  (sub-buffer [buffer offset length]
    "Create a sub buffer that shares the backing store with the main buffer."))


(defprotocol PToArray
  "Take a'thing' and convert it to an array that exactly represents the value
  of the data."
  (->sub-array [item]
    "Noncopying convert to a map of {:java-array :offset :length} or nil if impossible")
  (->array-copy [item]
    "Convert to an array containing a copy of the data"))

(defn ->array [item]
  (when-let [ary-data (->sub-array item)]
    (let [{:keys [java-array offset length]} ary-data]
      (when (and (= (int offset) 0)
                 (= (int (ecount java-array))
                    (int length)))
        java-array))))


(defprotocol PToList
  "Generically implemented for anything that implements ->array"
  (convertible-to-fastutil-list? [item])
  (->list-backing-store [item]))


(defn list-convertible?
  [item]
  (when (and item (convertible-to-fastutil-list? item))
    (convertible-to-fastutil-list? item)))


(defn as-list [item]
  (when (list-convertible? item)
    (->list-backing-store item)))


(defprotocol PToBufferDesc
  "Conversion to a buffer descriptor for consuming by an external C library."
  (convertible-to-buffer-desc? [item])
  (->buffer-descriptor [item]
    "Buffer descriptors are maps such that:
{:ptr com.sun.jna.Pointer that keeps reference back to original buffer.
 :datatype datatype of data that ptr points to.
 :device-type (optional) - one of #{:cpu :opencl :cuda}
 :shape -  vector of integers.
 :stride - vector of byte lengths for each dimension.
}
Note that this makes no mention of indianness; buffers are in the format of the host."))




;; Various other type conversions.  These happen quite a lot and we have found that
;; avoiding 'satisfies' is wise.  In all of these cases, options may contain at least
;; :datatype and :unchecked?
(defprotocol PToWriter
  (convertible-to-writer? [item])
  (->writer [item options]))

(defn as-writer
  [item & [options]]
  (when (convertible-to-writer? item)
    (->writer item options)))

(defprotocol PToReader
  (convertible-to-reader? [item])
  (->reader [item options]))


(defprotocol PReaderSparseUpdate
  (reader-sparse-update [src-reader update-bitmap update-values]))

(defn as-reader
  [item & [options]]
  (when (convertible-to-reader? item)
    (->reader item options)))

(defprotocol PToMutable
  (convertible-to-mutable? [item])
  (->mutable [item options]))

(defn as-mutable
  [item & [options]]
  (when (convertible-to-mutable? item)
    (->mutable item options)))

(defprotocol PToIterable
  (convertible-to-iterable? [item])
  (->iterable [item options]))

(defn as-iterable
  [item & [options]]
  (when (convertible-to-iterable? item)
    (->iterable item options)))

(defprotocol POperator
  (op-name [item]))

(defprotocol PToUnaryOp
  (convertible-to-unary-op? [item])
  (->unary-op [item options]))

(defn as-unary-op
  [item & [options]]
  (when (convertible-to-unary-op? item)
    (->unary-op item options)))

(defprotocol PToUnaryBooleanOp
  (convertible-to-unary-boolean-op? [item])
  (->unary-boolean-op [item options]))

(defn as-unary-boolean-op
  [item & [options]]
  (when (convertible-to-unary-boolean-op? item)
    (->unary-boolean-op item options)))

(defprotocol PToBinaryOp
  (convertible-to-binary-op? [item])
  (->binary-op [item options]))

(defn as-binary-op
  [item & [options]]
  (when (convertible-to-binary-op? item)
    (->binary-op item options)))

(defprotocol PToBinaryBooleanOp
  (convertible-to-binary-boolean-op? [item])
  (->binary-boolean-op [item options]))

(defn as-binary-boolean-op
  [item & [options]]
  (when (convertible-to-binary-boolean-op? item)
    (->binary-boolean-op item options)))


(defn base-type-convertible?
  [item]
  (and (casting/base-host-datatypes (casting/flatten-datatype (get-datatype item)))
       (or (convertible-to-nio-buffer? item)
           (convertible-to-fastutil-list? item))))


(defprotocol PToBitmap
  (convertible-to-bitmap? [item])
  (as-roaring-bitmap [item]))

(defprotocol PBitmapSet
  (set-and [lhs rhs])
  (set-and-not [lhs rhs])
  (set-or [lhs rhs])
  (set-xor [lhs rhs])
  (set-offset [item offset]
    "Offset a set by an amount")
  (set-add-range! [item start end])
  (set-add-block! [item data])
  (set-remove-range! [item start end])
  (set-remove-block! [item data]))


(defn as-base-type
  [item]
  (if (instance? java.util.RandomAccess item)
    (or (as-list item)
        (as-nio-buffer item))
    (as-nio-buffer item)))


(defprotocol PConstantTimeMinMax
  (has-constant-time-min-max? [item])
  (constant-time-min [item])
  (constant-time-max [item]))


(defprotocol PRangeConvertible
  (convertible-to-range? [item])
  (->range [item options]
    "Convert to something that implements the PRange protocols"))


(defprotocol PRange
  (range-select [lhs rhs]
    "Select the lhs range using the rhs range as an indexer.  Returns
  a new range as if the elements of lhs were indexed by rhs.")
  (range-start [item])
  (range-increment [item])
  (range-min [item])
  (range-max [item])
  (range-offset [item offset]
    "Offset this range by this offset.  Returns")
  (range->reverse-map [item]
    "Return a map whose keys are the values of the range
and whose values are the indexes that produce those values in the reader."))


(defprotocol PEndianness
  (endianness [item]
    "Either :little-endian or :big-endian"))


(extend-protocol PEndianness
  ByteBuffer
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian))
  ShortBuffer
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian))
  IntBuffer
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian))
  LongBuffer
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian))
  FloatBuffer
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian))
  DoubleBuffer
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian))
  CharBuffer
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian)))


(defn default-endianness
  [item]
  (or item :little-endian))


(defprotocol PConvertibleToBinaryReader
  (convertible-to-binary-reader? [item])
  (->binary-reader [item options]))

(defprotocol PConvertibleToBinaryWriter
  (convertible-to-binary-writer? [item])
  (->binary-writer [item options]))


(declare make-container)


(extend-type Object
  PDatatype
  (get-datatype [item] :object)
  PSetDatatype
  (set-datatype [item new-dtype]
    (cond
      (convertible-to-reader? item)
      (->reader item {:datatype new-dtype})
      (convertible-to-iterable? item)
      (->iterable item {:datatype new-dtype})
      :else
      (casting/cast item new-dtype)))
  PBufferType
  (buffer-type [item] :dense)
  PToBackingStore
  (->backing-store-seq [item] [item])
  PToNioBuffer
  (convertible-to-nio-buffer? [item] false)
  PToJNAPointer
  (convertible-to-data-ptr? [item] (jna/is-jna-ptr-convertible? item))
  (->jna-ptr [item] (jna/as-ptr item))
  PToList
  (convertible-to-fastutil-list? [item] false)
  PToBufferDesc
  (convertible-to-buffer-desc? [item] false)
  (->buffer-descriptor [item] (throw (Exception. "item is not convertible")))
  POperationType
  (operation-type [item]
    (cond
      (or (number? item) (nil? item)) :scalar
      (convertible-to-reader? item)
      :reader
      (convertible-to-iterable? item)
      :iterable
      :else
      :scalar))
  PToWriter
  (convertible-to-writer? [item]
    (or (base-type-convertible? item)
        (.isArray ^Class (type item))))
  (->writer [item options]
    (cond
      (convertible-to-nio-buffer? item)
      (-> (->buffer-backing-store item)
          (->writer (assoc options
                           :datatype (get-datatype item)))
          (->writer options))
      (convertible-to-fastutil-list? item)
      (-> (->list-backing-store item)
          (->writer (assoc options
                           :datatype (get-datatype item)))
          (->writer options))
      (.isArray ^Class (type item))
      (let [^"[Ljava.lang.Object;" obj-ary item
            n-elems (alength obj-ary)]
        (reify
          ObjectWriter
          (lsize [item] n-elems)
          (write [item idx value] (aset obj-ary idx value))))))

  PToReader
  (convertible-to-reader? [item]
    (or (base-type-convertible? item)
        (.isArray ^Class (type item))))
  (->reader [item options]
    (cond
      (convertible-to-nio-buffer? item)
      (-> (->buffer-backing-store item)
          (->reader (assoc options
                           :datatype (get-datatype item)))
          (->reader options))
      (convertible-to-fastutil-list? item)
      (-> (->list-backing-store item)
          (->reader (assoc options
                           :datatype (get-datatype item)))
          (->reader options))
      (.isArray ^Class (type item))
      (let [^"[Ljava.lang.Object;" obj-ary item
            n-elems (alength obj-ary)]
        (-> (reify
              ObjectReader
              (lsize [item] n-elems)
              (read [item idx] (aget obj-ary idx)))
            (->reader options)))
      :else
      nil
      ))
  PToIterable
  (convertible-to-iterable? [item]
    (convertible-to-reader? item))
  (->iterable [item options]
    (->reader item options))

  PToMutable
  (convertible-to-mutable? [item]
    (convertible-to-fastutil-list? item))
  (->mutable [item options]
    (-> (->list-backing-store item)
        (->mutable (assoc options
                         :datatype (get-datatype item)))
        (->mutable options)))

  POperator
  (op-name [item] :unnamed)

  PToUnaryOp
  (convertible-to-unary-op? [item] (fn? item))

  PToUnaryBooleanOp
  (convertible-to-unary-boolean-op? [item] false)

  PToBinaryOp
  (convertible-to-binary-op? [item] false)

  PToBinaryBooleanOp
  (convertible-to-binary-boolean-op? [item] false)

  PRangeConvertible
  (convertible-to-range? [item] false)

  PToBitmap
  (convertible-to-bitmap? [item] false)

  PConstantTimeMinMax
  (has-constant-time-min-max? [item] (convertible-to-range? item))
  (constant-time-min [item] (constant-time-min (->range item {})))
  (constant-time-max [item] (constant-time-max (->range item {})))

  PEndianness
  (endianness [item]
    (if (.. (ByteOrder/nativeOrder) (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian))
  PConvertibleToBinaryReader
  (convertible-to-binary-reader? [item] false)

  PConvertibleToBinaryWriter
  (convertible-to-binary-writer? [item] false))

(defmulti make-container
  (fn [container-type _datatype _elem-seq-or-count _options]
    container-type))


(defmulti copy!
  (fn [dst src _options]
    [(safe-buffer-type src)
     (safe-buffer-type dst)]))
