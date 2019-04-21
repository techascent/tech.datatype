(ns tech.v2.datatype.typecast
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.protocols.impl :as dtype-proto-impl]
            [tech.jna :as jna]
            [tech.v2.datatype.casting :as casting]
            [clojure.core.matrix.protocols :as mp])
  (:import [tech.v2.datatype
            ObjectWriter ObjectReader ObjectMutable ObjectReaderIter ObjectIter
            ByteWriter ByteReader ByteMutable ByteReaderIter ByteIter
            ShortWriter ShortReader ShortMutable ShortReaderIter ShortIter
            IntWriter IntReader IntMutable IntReaderIter IntIter
            LongWriter LongReader LongMutable LongReaderIter LongIter
            FloatWriter FloatReader FloatMutable FloatReaderIter FloatIter
            DoubleWriter DoubleReader DoubleMutable DoubleReaderIter DoubleIter
            BooleanWriter BooleanReader BooleanMutable BooleanReaderIter BooleanIter]
           [java.util Iterator]
           [com.sun.jna Pointer]
           [java.nio Buffer ByteBuffer ShortBuffer
            IntBuffer LongBuffer FloatBuffer DoubleBuffer]
           [it.unimi.dsi.fastutil.bytes ByteList ByteArrayList]
           [it.unimi.dsi.fastutil.shorts ShortList ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntList IntArrayList]
           [it.unimi.dsi.fastutil.longs LongList LongArrayList]
           [it.unimi.dsi.fastutil.floats FloatList FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleList DoubleArrayList]
           [it.unimi.dsi.fastutil.booleans BooleanList BooleanArrayList]
           [it.unimi.dsi.fastutil.objects ObjectList ObjectArrayList]))



(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro writer-type->datatype
  [writer-type]
  (case writer-type
    ObjectWriter `:object
    ByteWriter `:int8
    ShortWriter `:int16
    IntWriter `:int32
    LongWriter `:int64
    FloatWriter `:float32
    DoubleWriter `:float64
    BooleanWriter `:boolean))


(defn datatype->writer-type
  [datatype]
  (case datatype
    :int8 'tech.v2.datatype.ByteWriter
    :uint8 'tech.v2.datatype.ShortWriter
    :int16 'tech.v2.datatype.ShortWriter
    :uint16 'tech.v2.datatype.IntWriter
    :int32 'tech.v2.datatype.IntWriter
    :uint32 'tech.v2.datatype.LongWriter
    :int64 'tech.v2.datatype.LongWriter
    :uint64 'tech.v2.datatype.LongWriter
    :float32 'tech.v2.datatype.FloatWriter
    :float64 'tech.v2.datatype.DoubleWriter
    :boolean 'tech.v2.datatype.BooleanWriter
    :object 'tech.v2.datatype.ObjectWriter))



(defmacro implement-writer-cast
  [datatype]
  `(if (instance? ~(resolve (datatype->writer-type datatype)) ~'item)
     ~'item
     (dtype-proto/->writer-of-type ~'item ~datatype ~'unchecked?)))


(defn ->int8-writer ^ByteWriter [item unchecked?] (implement-writer-cast :int8))
(defn ->uint8-writer ^ShortWriter [item unchecked?] (implement-writer-cast :uint8))
(defn ->int16-writer ^ShortWriter [item unchecked?] (implement-writer-cast :int16))
(defn ->uint16-writer ^IntWriter [item unchecked?] (implement-writer-cast :uint16))
(defn ->int32-writer ^IntWriter [item unchecked?] (implement-writer-cast :int32))
(defn ->uint32-writer ^LongWriter [item unchecked?] (implement-writer-cast :uint32))
(defn ->int64-writer ^LongWriter [item unchecked?] (implement-writer-cast :int64))
(defn ->uint64-writer ^LongWriter [item unchecked?] (implement-writer-cast :uint64))
(defn ->float32-writer ^FloatWriter [item unchecked?] (implement-writer-cast :float32))
(defn ->float64-writer ^DoubleWriter [item unchecked?] (implement-writer-cast :float64))
(defn ->boolean-writer ^BooleanWriter [item unchecked?] (implement-writer-cast :boolean))
(defn ->object-writer ^ObjectWriter [item unchecked?] (implement-writer-cast :object))


(defmacro datatype->writer
  [datatype writer & [unchecked?]]
  (case datatype
    :int8 `(->int8-writer ~writer ~unchecked?)
    :uint8 `(->uint8-writer ~writer ~unchecked?)
    :int16 `(->int16-writer ~writer ~unchecked?)
    :uint16 `(->uint16-writer ~writer ~unchecked?)
    :int32 `(->int32-writer ~writer ~unchecked?)
    :uint32 `(->uint32-writer ~writer ~unchecked?)
    :int64 `(->int64-writer ~writer ~unchecked?)
    :uint64 `(->uint64-writer ~writer ~unchecked?)
    :float32 `(->float32-writer ~writer ~unchecked?)
    :float64 `(->float64-writer ~writer ~unchecked?)
    :boolean `(->boolean-writer ~writer ~unchecked?)
    :object `(->object-writer ~writer ~unchecked?)))


(defmacro reader-type->datatype
  [reader-type]
  (case reader-type
    ObjectReader `:object
    ByteReader `:int8
    ShortReader `:int16
    IntReader `:int32
    LongReader `:int64
    FloatReader `:float32
    DoubleReader `:float64
    BooleanReader `:boolean))


(defn datatype->reader-type
  [datatype]
  (case datatype
    :int8 'tech.v2.datatype.ByteReader
    :uint8 'tech.v2.datatype.ShortReader
    :int16 'tech.v2.datatype.ShortReader
    :uint16 'tech.v2.datatype.IntReader
    :int32 'tech.v2.datatype.IntReader
    :uint32 'tech.v2.datatype.LongReader
    :int64 'tech.v2.datatype.LongReader
    :uint64 'tech.v2.datatype.LongReader
    :float32 'tech.v2.datatype.FloatReader
    :float64 'tech.v2.datatype.DoubleReader
    :boolean 'tech.v2.datatype.BooleanReader
    :object 'tech.v2.datatype.ObjectReader))


(defmacro implement-reader-cast
  [datatype]
  `(if (instance? ~(resolve (datatype->reader-type datatype)) ~'item)
     ~'item
     (dtype-proto/->reader-of-type ~'item ~datatype ~'unchecked?)))


(defn ->int8-reader ^ByteReader [item unchecked?] (implement-reader-cast :int8))
(defn ->uint8-reader ^ShortReader [item unchecked?] (implement-reader-cast :uint8))
(defn ->int16-reader ^ShortReader [item unchecked?] (implement-reader-cast :int16))
(defn ->uint16-reader ^IntReader [item unchecked?] (implement-reader-cast :uint16))
(defn ->int32-reader ^IntReader [item unchecked?] (implement-reader-cast :int32))
(defn ->uint32-reader ^LongReader [item unchecked?] (implement-reader-cast :uint32))
(defn ->int64-reader ^LongReader [item unchecked?] (implement-reader-cast :int64))
(defn ->uint64-reader ^LongReader [item unchecked?] (implement-reader-cast :uint64))
(defn ->float32-reader ^FloatReader [item unchecked?] (implement-reader-cast :float32))
(defn ->float64-reader ^DoubleReader [item unchecked?] (implement-reader-cast :float64))
(defn ->boolean-reader ^BooleanReader [item unchecked?] (implement-reader-cast :boolean))
(defn ->object-reader ^ObjectReader [item unchecked?] (implement-reader-cast :object))


(defmacro datatype->reader
  [datatype reader & [unchecked?]]
  (case datatype
    :int8 `(->int8-reader ~reader ~unchecked?)
    :uint8 `(->uint8-reader ~reader ~unchecked?)
    :int16 `(->int16-reader ~reader ~unchecked?)
    :uint16 `(->uint16-reader ~reader ~unchecked?)
    :int32 `(->int32-reader ~reader ~unchecked?)
    :uint32 `(->uint32-reader ~reader ~unchecked?)
    :int64 `(->int64-reader ~reader ~unchecked?)
    :uint64 `(->uint64-reader ~reader ~unchecked?)
    :float32 `(->float32-reader ~reader ~unchecked?)
    :float64 `(->float64-reader ~reader ~unchecked?)
    :boolean `(->boolean-reader ~reader ~unchecked?)
    :object `(->object-reader ~reader ~unchecked?)))


(defn datatype->iter-type
  [datatype]
  (case datatype
    :int8 'tech.v2.datatype.ByteIter
    :uint8 'tech.v2.datatype.ShortIter
    :int16 'tech.v2.datatype.ShortIter
    :uint16 'tech.v2.datatype.IntIter
    :int32 'tech.v2.datatype.IntIter
    :uint32 'tech.v2.datatype.LongIter
    :int64 'tech.v2.datatype.LongIter
    :uint64 'tech.v2.datatype.LongIter
    :float32 'tech.v2.datatype.FloatIter
    :float64 'tech.v2.datatype.DoubleIter
    :boolean 'tech.v2.datatype.BooleanIter
    :object 'tech.v2.datatype.ObjectIter))


(defmacro implement-iter-cast
  [datatype]
  `(when ~'item
     (if (instance? ~(resolve (datatype->iter-type datatype)) ~'item)
       ~'item
       (.iterator ^Iterable (dtype-proto/->iterable-of-type
                             ~'item ~datatype ~'unchecked?)))))


(defn ->int8-iter ^ByteIter [item unchecked?] (implement-iter-cast :int8))
(defn ->uint8-iter ^ShortIter [item unchecked?] (implement-iter-cast :uint8))
(defn ->int16-iter ^ShortIter [item unchecked?] (implement-iter-cast :int16))
(defn ->uint16-iter ^IntIter [item unchecked?] (implement-iter-cast :uint16))
(defn ->int32-iter ^IntIter [item unchecked?] (implement-iter-cast :int32))
(defn ->uint32-iter ^LongIter [item unchecked?] (implement-iter-cast :uint32))
(defn ->int64-iter ^LongIter [item unchecked?] (implement-iter-cast :int64))
(defn ->uint64-iter ^LongIter [item unchecked?] (implement-iter-cast :uint64))
(defn ->float32-iter ^FloatIter [item unchecked?] (implement-iter-cast :float32))
(defn ->float64-iter ^DoubleIter [item unchecked?] (implement-iter-cast :float64))
(defn ->boolean-iter ^BooleanIter [item unchecked?] (implement-iter-cast :boolean))
(defn ->object-iter ^ObjectIter [item unchecked?] (implement-iter-cast :object))


(defn ->iterable
  "Whatever it is, make it iterable."
  ^Iterable [src-item]
  (if (instance? Iterable src-item)
    src-item
    (dtype-proto/->iterable-of-type
     src-item (dtype-proto-impl/safe-get-datatype src-item) false)))


(defn ->iter
  ^Iterator [item]
  (.iterator (->iterable item)))


(defmacro datatype->iter
  [datatype reader & [unchecked?]]
  (case datatype
    :int8 `(->int8-iter ~reader ~unchecked?)
    :uint8 `(->uint8-iter ~reader ~unchecked?)
    :int16 `(->int16-iter ~reader ~unchecked?)
    :uint16 `(->uint16-iter ~reader ~unchecked?)
    :int32 `(->int32-iter ~reader ~unchecked?)
    :uint32 `(->uint32-iter ~reader ~unchecked?)
    :int64 `(->int64-iter ~reader ~unchecked?)
    :uint64 `(->uint64-iter ~reader ~unchecked?)
    :float32 `(->float32-iter ~reader ~unchecked?)
    :float64 `(->float64-iter ~reader ~unchecked?)
    :boolean `(->boolean-iter ~reader ~unchecked?)
    :object `(->object-iter ~reader ~unchecked?)))



(defn ->int8-fast-iter ^ByteIter [item] item)
(defn ->uint8-fast-iter ^ShortIter [item] item)
(defn ->int16-fast-iter ^ShortIter [item] item)
(defn ->uint16-fast-iter ^IntIter [item] item)
(defn ->int32-fast-iter ^IntIter [item] item)
(defn ->uint32-fast-iter ^LongIter [item] item)
(defn ->int64-fast-iter ^LongIter [item] item)
(defn ->uint64-fast-iter ^LongIter [item] item)
(defn ->float32-fast-iter ^FloatIter [item] item)
(defn ->float64-fast-iter ^DoubleIter [item] item)
(defn ->boolean-fast-iter ^BooleanIter [item] item)
(defn ->object-fast-iter ^ObjectIter [item] item)


(defmacro datatype->fast-iter
  [datatype reader]
  (case datatype
    :int8 `(->int8-fast-iter ~reader)
    :uint8 `(->uint8-fast-iter ~reader)
    :int16 `(->int16-fast-iter ~reader)
    :uint16 `(->uint16-fast-iter ~reader)
    :int32 `(->int32-fast-iter ~reader)
    :uint32 `(->uint32-fast-iter ~reader)
    :int64 `(->int64-fast-iter ~reader)
    :uint64 `(->uint64-fast-iter ~reader)
    :float32 `(->float32-fast-iter ~reader)
    :float64 `(->float64-fast-iter ~reader)
    :boolean `(->boolean-fast-iter ~reader)
    :object `(->object-fast-iter ~reader)))


(defn reader->iterator
  [reader-item]
  (cond
    (instance? ByteReader reader-item) (ByteReaderIter. reader-item)
    (instance? ShortReader reader-item) (ShortReaderIter. reader-item)
    (instance? IntReader reader-item) (IntReaderIter. reader-item)
    (instance? LongReader reader-item) (LongReaderIter. reader-item)
    (instance? FloatReader reader-item) (FloatReaderIter. reader-item)
    (instance? DoubleReader reader-item) (DoubleReaderIter. reader-item)
    (instance? BooleanReader reader-item) (BooleanReaderIter. reader-item)
    (instance? ObjectReader reader-item) (ObjectReaderIter. reader-item)))


(defn datatype->iter-next-fn-name
  [datatype]
  (case datatype
    :int8 'nextByte
    :uint8 'nextShort
    :int16 'nextShort
    :uint16 'nextInt
    :int32 'nextInt
    :uint32 'nextLong
    :int64 'nextLong
    :uint64 'nextLong
    :float32 'nextFloat
    :float64 'nextDouble
    :boolean 'nextBoolean
    :object 'next))

(defmacro datatype->iter-next-fn
  [datatype reader-iter]
  (case datatype
    :int8 `(.nextByte ~reader-iter)
    :uint8 `(.nextShort ~reader-iter)
    :int16 `(.nextShort ~reader-iter)
    :uint16 `(.nextInt ~reader-iter)
    :int32 `(.nextInt ~reader-iter)
    :uint32 `(.nextLong ~reader-iter)
    :int64 `(.nextLong ~reader-iter)
    :uint64 `(.nextLong ~reader-iter)
    :float32 `(.nextFloat ~reader-iter)
    :float64 `(.nextDouble ~reader-iter)
    :boolean `(.nextBoolean ~reader-iter)
    :object `(.next ~reader-iter)))


(defn datatype->mutable-type
  [datatype]
  (case datatype
    :int8 'ByteMutable
    :uint8 'ShortMutable
    :int16 'ShortMutable
    :uint16 'IntMutable
    :int32 'IntMutable
    :uint32 'LongMutable
    :int64 'LongMutable
    :uint64 'LongMutable
    :float32 'FloatMutable
    :float64 'DoubleMutable
    :boolean 'BooleanMutable
    :object 'ObjectMutable))


(defmacro implement-mutable-cast
  [datatype]
  `(if (instance? ~(resolve (datatype->mutable-type datatype)) ~'item)
     ~'item
     (dtype-proto/->mutable-of-type ~'item ~datatype ~'unchecked?)))


(defn ->int8-mutable
  ^ByteMutable [item unchecked?] (implement-mutable-cast :int8))
(defn ->uint8-mutable
  ^ShortMutable [item unchecked?] (implement-mutable-cast :uint8))
(defn ->int16-mutable
  ^ShortMutable [item unchecked?] (implement-mutable-cast :int16))
(defn ->uint16-mutable
  ^IntMutable [item unchecked?] (implement-mutable-cast :uint16))
(defn ->int32-mutable
  ^IntMutable [item unchecked?] (implement-mutable-cast :int32))
(defn ->uint32-mutable
  ^LongMutable [item unchecked?] (implement-mutable-cast :uint32))
(defn ->int64-mutable
  ^LongMutable [item unchecked?] (implement-mutable-cast :int64))
(defn ->uint64-mutable
  ^LongMutable [item unchecked?] (implement-mutable-cast :uint64))
(defn ->float32-mutable
  ^FloatMutable [item unchecked?] (implement-mutable-cast :float32))
(defn ->float64-mutable
  ^DoubleMutable [item unchecked?] (implement-mutable-cast :float64))
(defn ->boolean-mutable
  ^BooleanMutable [item unchecked?] (implement-mutable-cast :boolean))
(defn ->object-mutable
  ^ObjectMutable [item unchecked?] (implement-mutable-cast :object))


(defmacro datatype->mutable
  [datatype mutable & [unchecked?]]
  (case datatype
    :int8 `(->int8-mutable ~mutable ~unchecked?)
    :uint8 `(->uint8-mutable ~mutable ~unchecked?)
    :int16 `(->int16-mutable ~mutable ~unchecked?)
    :uint16 `(->uint16-mutable ~mutable ~unchecked?)
    :int32 `(->int32-mutable ~mutable ~unchecked?)
    :uint32 `(->uint32-mutable ~mutable ~unchecked?)
    :int64 `(->int64-mutable ~mutable ~unchecked?)
    :uint64 `(->uint64-mutable ~mutable ~unchecked?)
    :float32 `(->float32-mutable ~mutable ~unchecked?)
    :float64 `(->float64-mutable ~mutable ~unchecked?)
    :boolean `(->boolean-mutable ~mutable ~unchecked?)
    :object `(->object-mutable ~mutable ~unchecked?)))


(defn as-byte-array
  ^bytes [obj] obj)

(defn as-short-array
  ^shorts [obj] obj)

(defn as-int-array
  ^ints [obj] obj)

(defn as-long-array
  ^longs [obj] obj)

(defn as-float-array
  ^floats [obj] obj)

(defn as-double-array
  ^doubles [obj] obj)

(defn as-boolean-array
  ^"[Z" [obj] obj)

(defn as-object-array
  ^"[Ljava.lang.Object;"
  [obj] obj)



(defmacro datatype->array-cast-fn
  [dtype buf]
  (case dtype
    :int8 `(as-byte-array ~buf)
    :int16 `(as-short-array ~buf)
    :int32 `(as-int-array ~buf)
    :int64 `(as-long-array ~buf)
    :float32 `(as-float-array ~buf)
    :float64 `(as-double-array ~buf)
    :boolean `(as-boolean-array ~buf)
    `(as-object-array ~buf)))


(defn ensure-ptr-like
  "JNA is extremely flexible in what it can take as an argument.  Anything convertible
  to a nio buffer, be it direct or array backend is fine."
  [item]
  (cond
    (and (satisfies? jna/PToPtr item)
         (jna/->ptr-backing-store item))
    (jna/->ptr-backing-store item)
    :else
    (if-let [retval (dtype-proto/->buffer-backing-store item)]
      retval
      (throw (ex-info "Object is not convertible to a pointer" {:item item})))))


(defn as-ptr
  ^Pointer [item]
  (jna/as-ptr item))


(defn as-array
  [item]
  (when (satisfies? dtype-proto/PToArray item)
    (dtype-proto/->sub-array item)))


(defn as-nio-buffer
  [item]
  (dtype-proto/as-nio-buffer item))


(defn as-list
  [item]
  (dtype-proto/as-list item))


(defn as-byte-buffer
  ^ByteBuffer [obj] (dtype-proto/->buffer-backing-store obj))

(defn as-short-buffer
  ^ShortBuffer [obj] (dtype-proto/->buffer-backing-store obj))

(defn as-int-buffer
  ^IntBuffer [obj] (dtype-proto/->buffer-backing-store obj))

(defn as-long-buffer
  ^LongBuffer [obj] (dtype-proto/->buffer-backing-store obj))

(defn as-float-buffer
  ^FloatBuffer [obj] (dtype-proto/->buffer-backing-store obj))

(defn as-double-buffer
  ^DoubleBuffer [obj] (dtype-proto/->buffer-backing-store obj))

(defn as-boolean-buffer
  ^BooleanList [obj] (dtype-proto/->list-backing-store obj))

(defn as-object-buffer
  ^ObjectList [obj] (dtype-proto/->list-backing-store obj))



(defmacro datatype->buffer-cast-fn
  [dtype buf]
  (case dtype
    :int8 `(as-byte-buffer ~buf)
    :int16 `(as-short-buffer ~buf)
    :int32 `(as-int-buffer ~buf)
    :int64 `(as-long-buffer ~buf)
    :float32 `(as-float-buffer ~buf)
    :float64 `(as-double-buffer ~buf)
    :boolean `(as-boolean-buffer ~buf)
    :object `(as-object-buffer ~buf)))


(defn datatype->buffer-type
  [dtype]
  (case dtype
    :int8 'ByteBuffer
    :int16 'ShortBuffer
    :int32 'IntBuffer
    :int64 'LongBuffer
    :float32 'FloatBuffer
    :float64 'DoubleBuffer
    :boolean 'BooleanList
    :object 'ObjectList))


(defn make-interface-buffer-type
  [dtype elem-count-or-seq & [options]]
  ((case dtype
     :int8 (partial dtype-proto/make-container :nio-buffer :int8)
     :uint8 (partial dtype-proto/make-container :typed-buffer :uint8)
     :int16 (partial dtype-proto/make-container :nio-buffer :int16)
     :uint16 (partial dtype-proto/make-container :typed-buffer :uint16)
     :int32 (partial dtype-proto/make-container :nio-buffer :int32)
     :uint32 (partial dtype-proto/make-container :typed-buffer :uint32)
     :int64 (partial dtype-proto/make-container :nio-buffer :int64)
     :uint64 (partial dtype-proto/make-container :typed-buffer :uint64)
     :float32 (partial dtype-proto/make-container :nio-buffer :float32)
     :float64 (partial dtype-proto/make-container :nio-buffer :float64)
     :boolean (partial dtype-proto/make-container :list :boolean)
     :object (partial dtype-proto/make-container :list :object))
   elem-count-or-seq options))


(defn byte-list-cast ^ByteList [item] item)
(defn short-list-cast ^ShortList [item] item)
(defn int-list-cast ^IntList [item] item)
(defn long-list-cast ^LongList [item] item)
(defn float-list-cast ^FloatList [item] item)
(defn double-list-cast ^DoubleList [item] item)
(defn boolean-list-cast ^BooleanList [item] item)
(defn object-list-cast ^ObjectList [item] item)


(defmacro datatype->list-cast-fn
  [datatype item]
  (case datatype
    :int8 `(byte-list-cast ~item)
    :int16 `(short-list-cast ~item)
    :int32 `(int-list-cast ~item)
    :int64 `(long-list-cast ~item)
    :float32 `(float-list-cast ~item)
    :float64 `(double-list-cast ~item)
    :boolean `(boolean-list-cast ~item)
    :object `(object-list-cast ~item)))


(defn datatype->list-type
  [datatype]
  (case datatype
    :int8 'ByteList
    :int16 'ShortList
    :int32 'IntList
    :int64 'LongList
    :float32 'FloatList
    :float64 'DoubleList
    :boolean 'BooleanList
    :object 'ObjectList))

(defn datatype->array-list-type
  [datatype]
  (case datatype
    :int8 'ByteArrayList
    :int16 'ShortArrayList
    :int32 'IntArrayList
    :int64 'LongArrayList
    :float32 'FloatArrayList
    :float64 'DoubleArrayList
    :boolean 'BooleanArrayList
    :object 'ObjectArrayList))


(defmacro wrap-array-fastpath
  [datatype src-data]
  (case datatype
    :int8 `(ByteArrayList/wrap ^bytes ~src-data)
    :int16 `(ShortArrayList/wrap ^shorts ~src-data)
    :int32 `(IntArrayList/wrap ^ints ~src-data)
    :int64 `(LongArrayList/wrap ^longs ~src-data)
    :float32 `(FloatArrayList/wrap ^floats ~src-data)
    :float64 `(DoubleArrayList/wrap ^doubles ~src-data)
    :boolean `(BooleanArrayList/wrap ^booleans ~src-data)
    `(ObjectArrayList/wrap (as-object-array ~src-data))))


(defn wrap-array-with-list
  [src-data & [datatype]]
  (let [datatype (or datatype
                     (dtype-proto-impl/safe-get-datatype src-data))]
    (case datatype
      :int8 (ByteArrayList/wrap ^bytes src-data)
      :int16 (ShortArrayList/wrap ^shorts src-data)
      :int32 (IntArrayList/wrap ^ints src-data)
      :int64 (LongArrayList/wrap ^longs src-data)
      :float32 (FloatArrayList/wrap ^floats src-data)
      :float64 (DoubleArrayList/wrap ^doubles src-data)
      :boolean (BooleanArrayList/wrap ^booleans src-data)
      (ObjectArrayList/wrap (as-object-array src-data)))))


(defn datatype->mutable-type
  [datatype]
  (case datatype
    :int8 'ByteMutable
    :uint8 'ShortMutable
    :int16 'ShortMutable
    :uint16 'IntMutable
    :int32 'IntMutable
    :uint32 'LongMutable
    :int64 'LongMutable
    :uint64 'LongMutable
    :float32 'FloatMutable
    :float64 'DoubleMutable
    :boolean 'BooleanMutable
    :object 'ObjectMutable))
