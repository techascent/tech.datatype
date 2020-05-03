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
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.protocols :as dtype-proto]
            ;;Support for base container types
            [tech.v2.datatype.bitmap :as bitmap]
            [tech.v2.datatype.readers.indexed :as indexed-rdr]
            [tech.v2.datatype.readers.const :as const-rdr]
            [tech.v2.datatype.index-algebra :as idx-alg])
  (:import [tech.v2.datatype MutableRemove ObjectMutable
            ListPersistentVector
            ObjectReader BooleanReader ByteReader ShortReader
            IntReader LongReader FloatReader DoubleReader]
           [clojure.lang IPersistentVector]
           [java.util Iterator List RandomAccess]
           [org.roaringbitmap RoaringBitmap]
           [tech.v2.datatype.bitmap BitmapSet])
  (:refer-clojure :exclude [cast]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn get-datatype
  [item]
  (base/get-datatype item))


(defn set-datatype
  [item dtype]
  (when item
    (dtype-proto/set-datatype item dtype)))


(defn operation-type
  [item]
  (base/operation-type item))


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
    ;;(sparse-proto/sparse-convertible? item)
    ;;:sparse
    )
  )


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
  ([item]
   (dtype-proto/clone item))
  ([item datatype]
   (when-not (= datatype (get-datatype item))
     (throw (Exception. (format "Cloning to different datatypes is no longer supported."))))
   (clone item)))


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


(defn copy-opt!
  "Pass in just src, dst, options"
  [src dst options]
  (copy! src 0 dst 0 (ecount src) options))


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


(defn as-jna-ptr
  [src-item]
  (dtype-proto/as-jna-ptr src-item))


(defn ->jna-ptr
  [src-item]
  (dtype-proto/->jna-ptr src-item))


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
  (when item
    (dtype-proto/convertible-to-reader? item)))


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


(defmacro make-reader
  "Make a reader.  Datatype must be a compile time visible object.
  read-op has 'idx' in scope which is the index to read from.  Returns a
  reader of the appropriate type for the passed in datatype.  Results are unchecked
  casted to the appropriate datatype.  It is up to *you* to ensure this is the result
  you want or throw an exception.

user> (dtype/make-reader :float32 5 idx)
[0.0 1.0 2.0 3.0 4.0]
user> (dtype/make-reader :boolean 5 idx)
[true true true true true]
user> (dtype/make-reader :boolean 5 (== idx 0))
[true false false false false]
user> (dtype/make-reader :float32 5 (* idx 2))
 [0.0 2.0 4.0 6.0 8.0]
user> (dtype/make-reader :any-datatype-you-wish 5 (* idx 2))
[0 2 4 6 8]
user> (dtype/get-datatype *1)
:any-datatype-you-wish
user> (dtype/make-reader [:a :b] 5 (* idx 2))
[0 2 4 6 8]
user> (dtype/get-datatype *1)
[:a :b]"
  [datatype n-elems read-op]
  `(let [~'n-elems (long ~n-elems)]
     ~(case (casting/safe-flatten datatype)
        :boolean `(reify BooleanReader
                    (getDatatype [rdr#] ~datatype)
                    (lsize [rdr#] ~'n-elems)
                    (read [rdr# ~'idx]
                      (casting/datatype->unchecked-cast
                       :unknown :boolean ~read-op)))
        :int8 `(reify ByteReader
                 (getDatatype [rdr#] ~datatype)
                 (lsize [rdr#] ~'n-elems)
                 (read [rdr# ~'idx] (unchecked-byte ~read-op)))
        :int16 `(reify ShortReader
                  (getDatatype [rdr#] ~datatype)
                  (lsize [rdr#] ~'n-elems)
                  (read [rdr# ~'idx] (unchecked-short ~read-op)))
        :int32 `(reify IntReader
                  (getDatatype [rdr#] ~datatype)
                  (lsize [rdr#] ~'n-elems)
                  (read [rdr# ~'idx] (unchecked-int ~read-op)))
        :int64 `(reify LongReader
                  (getDatatype [rdr#] ~datatype)
                  (lsize [rdr#] ~'n-elems)
                  (read [rdr# ~'idx] (unchecked-long ~read-op)))
        :float32 `(reify FloatReader
                    (getDatatype [rdr#] ~datatype)
                    (lsize [rdr#] ~'n-elems)
                    (read [rdr# ~'idx] (unchecked-float ~read-op)))
        :float64 `(reify DoubleReader
                    (getDatatype [rdr#] ~datatype)
                    (lsize [rdr#] ~'n-elems)
                    (read [rdr# ~'idx] (unchecked-double ~read-op)))
        :object `(reify ObjectReader
                   (getDatatype [rdr#] ~datatype)
                   (lsize [rdr#] ~'n-elems)
                   (read [rdr# ~'idx] ~read-op)))))


(defn object-reader
  "Create an object reader from an elem count and a function from index to object."
  ([n-elems idx->item-fn datatype]
   (let [n-elems (long n-elems)]
     (reify
       ObjectReader
       (getDatatype [_] (or datatype :object))
       (lsize [_] (long n-elems))
       (read [_ elem-idx]
         (idx->item-fn elem-idx)))))
  ([n-elems idx->item-fn]
   (object-reader n-elems idx->item-fn :object)))


(defn reader-map
  "Map a function over several readers returning a new reader.  Reader will always
  have :object datatype."
  [map-fn reader & readers]
  (let [args (map #(->reader %) (concat [reader] readers))
        n-elems (->> args
                     (map ecount)
                     (apply min)
                     long)
        n-readers (count args)]
    (case n-readers
      1 (let [reader (first args)]
          (object-reader n-elems #(map-fn (reader %))))
      2 (let [reader1 (first args)
              reader2 (second args)]
          (object-reader n-elems
                         #(map-fn (reader1 %)
                                  (reader2 %))))
      (object-reader n-elems
                     #(apply map-fn (map (fn [reader] (reader %))))))))


(defmacro ->typed-reader
  "Do a typecast to transform obj into a reader with a defined type.  Object must
  have an implementation of tech.v2.datatype.protocols/PToReader or be an
  implementation of one of the existing readers.  This allows typed .read calls
  on the appropriate reader type.

  Use unchecked? if you do not want the reader conversion to check the data.  This
  only applies if you are changing datatypes from the base object datatype."
  ([obj datatype unchecked?]
   (case (casting/safe-flatten datatype)
     :boolean `(typecast/datatype->reader :boolean ~obj ~unchecked?)
     :int8 `(typecast/datatype->reader :int8 ~obj ~unchecked?)
     :int16 `(typecast/datatype->reader :int16 ~obj ~unchecked?)
     :int32 `(typecast/datatype->reader :int32 ~obj ~unchecked?)
     :int64 `(typecast/datatype->reader :int64 ~obj ~unchecked?)
     :float32 `(typecast/datatype->reader :float32 ~obj ~unchecked?)
     :float64 `(typecast/datatype->reader :float64 ~obj ~unchecked?)
     :object `(typecast/datatype->reader :object ~obj ~unchecked?)))
  ([obj datatype]
   `(->typed-reader ~obj ~datatype false)))


(defn indexed-reader
  "Create an indexed reader that readers values from specific indexes.  The options
  map can contain at least :datatype and :unchecked? to control the datatype
  and casting correctness checks of the resulting reader."
  ([indexes values options]
   (indexed-rdr/make-indexed-reader indexes values options))
  ([indexed values]
   (indexed-reader indexed values {})))


(defn const-reader
  "Create an indexed reader that readers values from specific indexes.  The options
  map can contain at least :datatype and :unchecked? to control the datatype
  and casting correctness checks of the resulting reader."
  ([value n-elems options]
   (const-rdr/make-const-reader value (:datatype options) n-elems))
  ([value n-elems]
   (const-reader value n-elems {})))


(defn reader-select
  "Select indexes out of a reader.  This applies the selection to the existing reader
  in an opaque fashion by using an indexed reader.  A much more efficient version of
  select when doing compositions of select operations is provided for tensors."
  [reader select-arg]
  (-> (idx-alg/select (ecount reader) select-arg)
      (indexed-reader reader)))


(defn reader-as-persistent-vector
  "In-place conversion of a reader to a persistent list that gives you Clojure
  semantics (which are excellent) for equality and hashcode implementations.
  Use this if you intend to use readers as keys in hashmaps.  Data is not copied
  and modification is inefficient."
  ^IPersistentVector [reader]
  (when-not (reader? reader)
    (throw (Exception. "Input has to be convertible to a reader.")))
  (ListPersistentVector. (->reader reader)))


;;Sparse is gone for a bit.
#_(defn ->sparse
  "Return an object that implements the sparse protocols."
  [item]
  (when (= :sparse (buffer-type item))
    (sparse-proto/->sparse item)))


(defn writer?
  [item]
  (when item
    (dtype-proto/convertible-to-writer? item)))

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


(defn ->bitmap-set
  "Create a bitmap capable of storing unsigned integers up to about 4 billion.
  Set operations are expected to be highly optimized.  Random reads potentially
  less so (although still not bad).
  Bitmaps are convertible to readers and the bitmap ops all apply to them."
  ([]
   (bitmap/->bitmap))
  ([item-seq]
   (bitmap/->bitmap item-seq)))


(defn ->unique-bitmap-set
  "Create a bitmap capable of storing unsigned integers up to about 4 billion.
  Set operations are expected to be highly optimized.  Random reads potentially
  less so (although still not bad).
  Bitmaps are convertible to readers and the bitmap ops all apply to them."
  ([]
   (bitmap/->unique-bitmap))
  ([item-seq]
   (bitmap/->unique-bitmap item-seq)))


;; bitmap Set Operations
(defn set-and
  "bitmap op"
  [lhs rhs]
  (dtype-proto/set-and lhs rhs))

(defn set-and-not
  "bitmap op"
  [lhs rhs]
  (dtype-proto/set-and-not lhs rhs))

(defn set-or
  "bitmap op"
  [lhs rhs]
  (dtype-proto/set-or lhs rhs))

(defn set-xor
  "bitmap op"
  [lhs rhs]
  (dtype-proto/set-xor lhs rhs))

(defn set-offset
  "bitmap op"
  [item offset]
  (dtype-proto/set-offset item offset))

(defn set-add-range!
  "bitmap op"
  [item start end]
  (dtype-proto/set-add-range! item start end))

(defn set-add-block!
  "bitmap op"
  [item data]
  (dtype-proto/set-add-block! item data))

(defn set-remove-range!
  "bitmap op"
  [item start end]
  (dtype-proto/set-remove-range! item start end))

(defn set-remove-block!
  "bitmap op"
  [item data]
  (dtype-proto/set-remove-block! item data))

(defn bitmap->typed-buffer
  [bitmap]
  (bitmap/bitmap->typed-buffer bitmap))

(defn bitmap->set
  [bitmap]
  (BitmapSet. bitmap))

(defn as-roaring-bitmap
  [item]
  (when (dtype-proto/convertible-to-bitmap? item)
    (dtype-proto/as-roaring-bitmap item)))
