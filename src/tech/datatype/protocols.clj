(ns tech.datatype.protocols)


(defprotocol PDatatype
  (get-datatype [item]))

(defprotocol PAccess
  (set-value! [item offset value])
  (set-constant! [item offset value elem-count])
  (get-value [item offset]))

(defprotocol PContainerType
  (container-type [item]))

(defprotocol PCopyRawData
  "Given a sequence of data copy it as fast as possible into a target item."
  (copy-raw->item! [raw-data ary-target target-offset options]))

(defprotocol PPersistentVector
  "Conversion to a persistent vector of numbers."
  (->vector [item]))

(defprotocol PPrototype
  (from-prototype [item datatype shape]))

(defprotocol PClone
  (clone [item datatype]))

(defprotocol PToNioBuffer
  "Take a 'thing' and convert it to a nio buffer.  Only valid if the thing
  shares the backing store with the buffer.  Result may not exactly
  represent the value of the item itself as the backing store may require
  element-by-element conversion to represent the value of the item."
  (->buffer-backing-store [item]))


(defprotocol PNioBuffer
  (position [item])
  (limit [item]))


(defprotocol PBuffer
  "Interface to create sub-buffers out of larger contiguous buffers."
  (sub-buffer [buffer offset length]
    "Create a sub buffer that shares the backing store with the main buffer.")
  (alias? [lhs-buffer rhs-buffer]
    "Do these two buffers alias each other?  Meaning do they start at the same address
and overlap completely?")
  (partially-alias? [lhs-buffer rhs-buffer]
    "Do these two buffers partially alias each other?  Does some sub-range of their
data overlap?"))


(defprotocol PToArray
  "Take a'thing' and convert it to an array that exactly represents the value
  of the data."
  (->array [item]
    "Convert to an array; both objects must share backing store")
  (->sub-array [item]
    "Convert to a map of {:array-data :offset :length}")
  (->array-copy [item]
    "Convert to an array containing a copy of the data"))


(defprotocol PFastutilConvertible
  (->fastutil-list-backing-store [item]))


(defprotocol PToWriter
  (->object-writer [item])
  (->writer-of-type [item datatype])
  (->writer [item]))


(defprotocol PToReader
  (->object-reader [item])
  (->reader-of-type [item datatype unchecked?])
  (->reader [item]))


(defprotocol PToMutable
  (->object-mutable [item])
  (->mutable-of-type [item datatype])
  (->mutable [item]))
