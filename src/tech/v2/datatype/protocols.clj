(ns tech.v2.datatype.protocols
  (:require [clojure.core.matrix.protocols :as mp])
  (:import [tech.v2.datatype Datatype Countable
            ObjectIter IteratorObjectIter]))

(set! *warn-on-reflection* true)

(defprotocol PDatatype
  (get-datatype [item]))

(extend-type Datatype
  PDatatype
  (get-datatype [item] (.getDatatype item)))


(extend-type Object
  PDatatype
  (get-datatype [item] :object))


(extend-type Countable
  mp/PElementCount
  (element-count [item] (.lsize item)))

(defprotocol PCopyRawData
  "Given a sequence of data copy it as fast as possible into a target item."
  (copy-raw->item! [raw-data ary-target target-offset options]))

(defprotocol PPrototype
  (from-prototype [item datatype shape]))

(defprotocol PClone
  "Clone an object.  Implemented generically for all objects."
  (clone [item datatype]))

(defprotocol PBufferType ;;:sparse or :dense
  (buffer-type [item]))

(extend-type Object
  PBufferType
  (buffer-type [item] :dense))

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


(extend-type Object
  PToBackingStore
  (->backing-store-seq [item] [item]))


(defprotocol PToNioBuffer
  "Take a 'thing' and convert it to a nio buffer.  Only valid if the thing
  shares the backing store with the buffer.  Result may not exactly
  represent the value of the item itself as the backing store may require
  element-by-element conversion to represent the value of the item."
  (convertible-to-nio-buffer? [item])
  (->buffer-backing-store [item]))


(extend-type Object
  PToNioBuffer
  (convertible-to-nio-buffer? [item] false))


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
  (if (satisfies? PToArray item)
    (when-let [ary-data (->sub-array item)]
      (let [{:keys [java-array offset length]} ary-data]
        (when (and (= (int offset) 0)
                   (= (int (mp/element-count java-array))
                      (int length)))
          java-array)))))


(defprotocol PToList
  "Generically implemented for anything that implements ->array"
  (convertible-to-fastutil-list? [item])
  (->list-backing-store [item]))


(extend-type Object
  PToList
  (convertible-to-fastutil-list? [item] false))


(defn list-convertible?
  [item]
  (when (and item (satisfies? PToList item))
    (convertible-to-fastutil-list? item)))


(defn as-list [item]
  (when (list-convertible? item)
    (->list-backing-store item)))



(defprotocol PToWriter
  (->writer-of-type [item datatype unchecked?]))

(defprotocol PToReader
  (->reader-of-type [item datatype unchecked?]))

(defprotocol PToMutable
  (->mutable-of-type [item datatype unchecked?]))

(defprotocol PToIterable
  (->iterable-of-type [item datatype unchecked?]))

(defprotocol POperator
  (op-name [item]))

(defprotocol PToUnaryOp
  (->unary-op [item datatype unchecked?]))

(defprotocol PToUnaryBooleanOp
  (->unary-boolean-op [item datatype unchecked?]))

(defprotocol PToBinaryOp
  (->binary-op [item datatype unchecked?]))


(defprotocol PToBinaryBooleanOp
  (->binary-boolean-op [item datatype unchecked?]))


(defmulti make-container
  (fn [container-type datatype elem-seq-or-count options]
    container-type))


(defmulti copy!
  (fn [dst src options]
    [(safe-buffer-type src)
     (safe-buffer-type dst)]))
