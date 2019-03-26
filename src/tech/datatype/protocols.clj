(ns tech.datatype.protocols
  (:require [clojure.core.matrix.protocols :as mp])
  (:import [tech.datatype Datatype Countable
            ObjectIter IteratorObjectIter]))

(set! *warn-on-reflection* true)

(defprotocol PDatatype
  (get-datatype [item]))

(extend-type Datatype
  PDatatype
  (get-datatype [item] (.getDatatype item)))

(extend-type Countable
  mp/PElementCount
  (element-count [item] (.size item)))

(defprotocol PCopyRawData
  "Given a sequence of data copy it as fast as possible into a target item."
  (copy-raw->item! [raw-data ary-target target-offset options]))

(defprotocol PPrototype
  (from-prototype [item datatype shape]))

(defprotocol PClone
  "Clone an object.  Implemented generically for all objects."
  (clone [item datatype]))


(defprotocol PSetConstant
  (set-constant! [item offset value elem-count]))


(defprotocol PWriteBlock
  (write-block! [item offset values options]))

(defprotocol PWriteIndexes
  (write-indexes! [item indexes values options]))

(defprotocol PReadBlock
  (read-block! [item offset values options]))

(defprotocol PReadIndexes
  (read-indexes! [item indexes values options]))

(defprotocol PInsertBlock
  (insert-block! [item idx values options]))


(defprotocol PToNioBuffer
  "Take a 'thing' and convert it to a nio buffer.  Only valid if the thing
  shares the backing store with the buffer.  Result may not exactly
  represent the value of the item itself as the backing store may require
  element-by-element conversion to represent the value of the item."
  (->buffer-backing-store [item]))


(defn as-nio-buffer
  [item]
  (when (and item (satisfies? PToNioBuffer item))
    (->buffer-backing-store item)))

(defprotocol PNioBuffer
  (position [item])
  (limit [item])
  (array-backed? [item]))


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
  (->sub-array [item]
    "Noncopying convert to a map of {:array-data :offset :length} or nil if impossible")
  (->array-copy [item]
    "Convert to an array containing a copy of the data"))

(defn ->array [item]
  (if (satisfies? PToArray item)
    (when-let [ary-data (->sub-array item)]
      (let [{:keys [array-data offset length]} ary-data]
        (when (and (= (int offset) 0)
                   (= (int (mp/element-count array-data))
                      (int length)))
          array-data)))))


(defprotocol PToList
  "Generically implemented for anything that implements ->array"
  (->list-backing-store [item]))


(defn as-list [item]
  (when (and item (satisfies? PToList item))
    (->list-backing-store item)))


(defprotocol PToTypedBuffer
  (->typed-buffer [item datatype]))


(defprotocol PToWriter
  (->writer-of-type [item datatype unchecked?]))

(defprotocol PToReader
  (->reader-of-type [item datatype unchecked?]))

(defprotocol PToMutable
  (->mutable-of-type [item datatype unchecked?]))

(defprotocol PToIterable
  (->iterable-of-type [item datatype unchecked?]))

(defprotocol PToUnaryOp
  (->unary-op [item datatype unchecked?]))

(defprotocol PToBinaryOp
  (->binary-op [item datatype unchecked?]))

(defmulti make-container
  (fn [container-type datatype elem-seq-or-count options]
    container-type))
