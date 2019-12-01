(ns tech.v2.datatype
  "Generalized efficient manipulations of sequences of primitive datatype.  Includes
  specializations for java arrays and nio buffers, fastutil lists, and native buffers.
  There are specializations to allow implementations to provide efficient full typed
  copy functions when the types can be ascertained.

  Generic operations include:
  1. datatype of this sequence.
  2. Writing to, reading from.
  3. Construction.
  4. Efficient mutable copy from one sequence to another.
  5. Mutation of underlying storage amount assuming list based.

  Lists furthermore support add/removing elements at arbitrary indexes.

  Base datatypes are:
  :int8 :uint8 :int16 :uint16 :int32 :uint32 :int64 :uint64
  :float32 :float64 :boolean :string :object"
  (:require [tech.jna :as jna]
            [tech.v2.datatype.base :as base]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto]
            ;;Support for base container types
            [tech.v2.datatype.array]
            [tech.v2.datatype.nio-buffer]
            [tech.v2.datatype.typed-buffer]
            [tech.v2.datatype.jna]
            [tech.v2.datatype.list]
            [tech.v2.datatype.clj-range]
            [tech.v2.datatype.sparse.protocols :as sparse-proto]
            [tech.v2.datatype.sparse.sparse-buffer]
            [tech.v2.datatype.readers.indexed :as indexed-rdr]
            [tech.v2.datatype.object-datatypes])
  (:import [tech.v2.datatype MutableRemove ObjectMutable ObjectReader]
           [java.util Iterator List RandomAccess])
  (:refer-clojure :exclude [cast]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn get-datatype
  [item]
  (base/get-datatype item))


(defn make-container
  "Generically make a container of the given type.  Built in types are:

  :java-array - Support for host primitive types, boolean, and all object types:

  :nio-buffer - Support for host primitive types.

  :native-buffer - Support for all (including unsigned) numeric types.  Native backed.

  :typed-buffer - Support all known datatypes, java array backend

  :list - Support all known datatypes and allow mutation.  fastutil array-list backed"
  [container-type datatype elem-count-or-seq & [options]]
  (dtype-proto/make-container container-type datatype
                              elem-count-or-seq options))


(defn make-jvm-container
  "Jvm container of fixed element count."
  [datatype elem-count-or-seq & [options]]
  (make-container :typed-buffer datatype elem-count-or-seq options))


(defn make-array-of-type
  "Make a jvm array."
  [datatype elem-count-or-seq & [options]]
  (dtype-proto/make-container :java-array datatype
                              elem-count-or-seq options))


(defn make-buffer-of-type
  "Make an array backed nio buffer."
  [datatype elem-count-or-seq & [options]]
  (dtype-proto/make-container :nio-buffer datatype
                              elem-count-or-seq (or options {})))


(defn make-native-container
  "Native container of fixed element count."
  [datatype elem-count-or-seq & [options]]
  (make-container :native-buffer datatype elem-count-or-seq options))


(defn make-jvm-list
  "Jvm container that allows adding/removing of elements which
  means is supports the ->mutable protocol."
  [datatype elem-count-or-seq & [options]]
  (make-container :list datatype elem-count-or-seq options))


(defn make-sparse-container
  "Make a container of fixed element count that stores data sparsely"
  [datatype elem-count-or-seq & [options]]
  (make-container :sparse datatype elem-count-or-seq options))


(defn buffer-type
  "Return the type of buffer.  Current options are :dense or :sparse"
  [item]
  (base/buffer-type item))


(defn container-type
  "sparse, typed-buffer, list, or native-buffer.  nil means
  unknown."
  [item]
  (cond
    (dtype-proto/list-convertible? item)
    :list
    (jna/ptr-convertible? item)
    :native-buffer
    (dtype-proto/nio-convertible? item)
    :typed-buffer
    (sparse-proto/sparse-convertible? item)
    :sparse))


(defn set-value!
  "Set a value on a container."
  [item offset value]
  (base/set-value! item offset value))


(defn set-constant!
  "Set a constant value on a container."
  ([item offset value elem-count]
   (base/set-constant! item offset value elem-count))
  ([item value]
   (base/set-constant! item 0 value (base/ecount item))))


(defn get-value
  "Get a value from a container."
  [item offset]
  (base/get-value item offset))


(defn ecount
  "Type hinted ecount so numeric expressions run faster.
Calls clojure.core.matrix/ecount."
  ^long [item]
  (if (nil? item)
    0
    (base/ecount item)))


(defn shape
  "m/shape with fallback to m/ecount if m/shape is not available."
  [item]
  (base/shape item))


(defn shape->ecount
  "Get an ecount from a shape."
  ^long [shape-or-num]
  (if (nil? shape-or-num)
    0
    (base/shape->ecount shape-or-num)))


(defn datatype->byte-size
  "Get the byte size of a given (numeric only) datatype."
  ^long [datatype]
  (base/datatype->byte-size datatype))


(defn cast
  "Cast a value to a datatype.  Throw exception if out or range."
  [value datatype]
  (casting/cast value datatype))


(defn unchecked-cast
  "Cast a value to a datatype ignoring errors."
  [value datatype]
  (casting/unchecked-cast value datatype))


(defn ->vector
  "Conversion to persistent vector"
  [item]
  (base/->vector item))


(defn from-prototype
  "Create a new thing like the old thing.  Does not require copying
  data into the new thing."
  [item & {:keys [datatype shape]}]
  (dtype-proto/from-prototype item
                       (or datatype (get-datatype item))
                       (or shape (base/shape item))))


(defn clone
  "Create a new thing from the old thing and copy data into new thing."
  [item & {:keys [datatype]}]
  (dtype-proto/clone item (or datatype (get-datatype item))))


(defn ->array
  "Returns nil of item does not share a backing store with an array or if
  item has been non-trivially sub-buffered."
  [item]
  (dtype-proto/->array item))


(defn ->byte-array
  ^bytes [item]
  (base/->byte-array item))


(defn ->short-array
  ^shorts [item]
  (base/->short-array item))


(defn ->int-array
  ^ints [item]
  (base/->int-array item))


(defn ->long-array
  ^longs [item]
  (base/->long-array item))


(defn ->float-array
  ^floats [item]
  (base/->float-array item))


(defn ->double-array
  ^doubles [item]
  (base/->double-array item))


(defn ->sub-array
  "Returns map of the backing array plus a length and offset
  of nil of this item is not array backed.  The sub array may not match
  the container in datatype.
  {:java-array :offset :length}"
  [item]
  (dtype-proto/->sub-array item))


(defn ->array-copy
  "Copy the data into an array that can correctly hold the datatype.  This
  array may not have the same datatype as the source item"
  [item]
  (dtype-proto/->array-copy item))


(defn copy!
  "Copy a block of data from one container to another"
  ([src src-offset dst dst-offset n-elems options]
   (base/copy! src src-offset dst dst-offset n-elems options))
  ([src src-offset dst dst-offset n-elems]
   (base/copy! src src-offset dst dst-offset n-elems {}))
  ([src dst]
   (base/copy! src 0 dst 0 (ecount src) {})))


(defn copy-raw->item!
  "Copy a non-datatype sequence of data into a datatype library container.
  [dtype-container result-offset-after-copy]"
  ([raw-data container container-offset options]
   (dtype-proto/copy-raw->item! raw-data container
                                container-offset options))
  ([raw-data container container-offset]
   (copy-raw->item! raw-data container container-offset {}))
  ([raw-data container]
   (copy-raw->item! raw-data container 0)))


(defn write-block!
  "Write a block of data to an object.  Values must support ->reader,
  get-datatype, and core.matrix.protocols/element-count."
  [item offset values & [options]]
  (base/write-block! item offset values options))


(defn read-block!
  "Read a block from an object.  Values must support ->writer,
  get-datatype, and core.matrix.protocols/element-count."
  [item offset values & [options]]
  (base/read-block! item offset values options))


(defn write-indexes!
  "Write a block of data to specific indexes in the object.
  indexes, values must support same protocols as write and read block."
  [item indexes values & [options]]
  (dtype-proto/write-indexes! item indexes values options)
  item)


(defn read-indexes!
  "Write a block of data to specific indexes in the object.
  indexes, values must support same protocols as write and read block."
  [item indexes values & [options]]
  (dtype-proto/read-indexes! item indexes values options)
  values)


(defn insert!
  [item idx value]
  (.insert ^ObjectMutable (dtype-proto/->mutable item {:datatype :object})
           idx value)
  item)


(defn remove!
  [item idx]
  (let [mut-item ^MutableRemove (dtype-proto/->mutable item {})]
    (.mremove mut-item (int idx))
    item))


(defn remove-range!
  [item idx count]
  (base/remove-range! item idx count)
  item)


(defn insert-block!
  [item idx values & [options]]
  (dtype-proto/insert-block! item idx values options)
  item)



(defn ->buffer-backing-store
  "Convert to nio buffer that stores the data for the object.  This may have
  a different datatype than the object, so for instance the backing store for
  the uint8 datatype is a nio buffer of type int8."
  [src-ary]
  (dtype-proto/->buffer-backing-store src-ary))


(defn as-nio-buffer
  [src-item]
  (dtype-proto/as-nio-buffer src-item))


(defn as-list
  [src-item]
  (dtype-proto/as-list src-item))


(defn as-array
  [src-item]
  (dtype-proto/->sub-array src-item))


(defn as-buffer-descriptor
  [src-item]
  (when (dtype-proto/convertible-to-buffer-desc? src-item)
    (dtype-proto/->buffer-descriptor src-item)))


(defn ->list-backing-store
  "Convert to a list that stores data for the object.  This may have a different
  datatype than the object."
  [src-item]
  (dtype-proto/->list-backing-store src-item))


(defn sub-buffer
  "Create a sub buffer that shares the backing store with the main buffer."
  [buffer offset & [length]]
  (let [length (or length (max 0 (- (ecount buffer)
                                    (long offset))))]
    (base/sub-buffer buffer offset length)))


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


(defn reader?
  [item]
  (dtype-proto/convertible-to-reader? item))


(defn ->reader
  "Create a reader of a specific type."
  [src-item & [datatype options]]
  (if (map? datatype)
    (dtype-proto/->reader src-item datatype)
    (dtype-proto/->reader src-item
                          (assoc options
                                 :datatype
                                 (or datatype (get-datatype src-item))))))


(defn ->>reader
  "Create a reader of a specific type."
  ([options src-item]
   (dtype-proto/->reader src-item options))
  ([src-item]
   (->>reader src-item {})))


(defn object-reader
  "Create an object reader from an elem count and a function from index to object."
  [n-elems idx->item-fn & [datatype]]
  (let [n-elems (long n-elems)]
    (reify
      ObjectReader
      (getDatatype [_] (or datatype :object))
      (lsize [_] (long n-elems))
      (read [_ elem-idx]
        (idx->item-fn elem-idx)))))


(defn reader-map
  "Map a function over several readers returning a new reader."
  [map-fn reader & readers]
  (let [args (map #(->reader % :object) (concat [reader] readers))
        n-elems (->> args
                     (map ecount)
                     (apply min)
                     long)
        n-readers (count args)]
    (case n-readers
      1 (let [^ObjectReader reader (first args)]
          (object-reader n-elems #(map-fn (.read reader %))))
      2 (let [^ObjectReader reader1 (first args)
              ^ObjectReader reader2 (second args)]
          (object-reader n-elems
                         #(map-fn (.read reader1 %)
                                  (.read reader2 %))))
      (object-reader n-elems
                     #(apply map-fn (map (fn [^ObjectReader reader]
                                           (.read reader %))))))))


(defn indexed-reader
  "Create an indexed reader that readers values from specific indexes.  The options
  map can contain at least :datatype and :unchecked? to control the datatype
  and casting correctness checks of the resulting reader."
  [indexes values & [options]]
  (indexed-rdr/make-indexed-reader indexes values options))


(defn ->sparse
  "Return an object that implements the sparse protocols."
  [item]
  (when (= :sparse (buffer-type item))
    (sparse-proto/->sparse item)))


(defn ->writer
  "Create a writer of a specific type."
  [src-item & [datatype options]]
  (dtype-proto/->writer src-item
                        (assoc options
                               :datatype
                               (or datatype (get-datatype src-item)))))


(defn ->mutable
  "Create an object capable of mutating the underlying structure of the data storage.
  Only works for list-backed types."
  [src-item & [datatype options]]
  (dtype-proto/->mutable src-item
                         (assoc options
                                :datatype
                                (or datatype (get-datatype src-item)))))
