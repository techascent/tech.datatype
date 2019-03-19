(ns tech.datatype.jna
  (:require [tech.datatype.base :as dtype-base]
            [tech.datatype.nio-buffer]
            [tech.datatype.typed-buffer :as typed-buf]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting :as casting]
            [tech.jna :as jna]
            [tech.resource :as resource]
            [clojure.core.matrix.protocols :as mp])
  (:import [com.sun.jna Pointer Native Function NativeLibrary]
           [com.sun.jna.ptr PointerByReference]
           [java.nio ByteBuffer Buffer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn offset-pointer
  ^Pointer [^Pointer ptr, ^long offset]
  (Pointer. (+ offset (Pointer/nativeValue ptr))))


(defn make-jna-pointer
  "Use with care..."
  ^Pointer [^long address]
  (Pointer. address))


(defn pointer->address
  ^long [^Pointer ptr]
  (Pointer/nativeValue ptr))


(declare make-typed-pointer)


(defrecord TypedPointer [^Pointer ptr ^long byte-len datatype]
  jna/PToPtr
  (->ptr-backing-store [item] ptr)

  dtype-proto/PDatatype
  (get-datatype [item] datatype)

  mp/PElementCount
  (element-count [_] (quot byte-len
                           (dtype-base/datatype->byte-size datatype)))

  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (dtype-proto/copy-raw->item! (typed-buf/->typed-buffer raw-data) ary-target
                                 target-offset options))

  dtype-proto/PPrototype
  (from-prototype [item datatype shape]
    (make-typed-pointer datatype (dtype-base/shape->ecount shape)))

  dtype-proto/PToNioBuffer
  (->buffer-backing-store [item]
    (let [jvm-type (casting/datatype->host-datatype datatype)
          buffer (.getByteBuffer ptr 0 byte-len)]
      (case jvm-type
        :int8 buffer
        :int16 (.asShortBuffer buffer)
        :int32 (.asIntBuffer buffer)
        :int64 (.asLongBuffer buffer)
        :float32 (.asFloatBuffer buffer)
        :float64 (.asDoubleBuffer buffer))))


  dtype-proto/PToArray
  (->array [item] nil)
  (->sub-array [item] nil)
  (->array-copy [item]
    (dtype-proto/->array-copy (typed-buf/->typed-buffer item)))

  dtype-proto/PBuffer
  (sub-buffer [buffer offset length]
    (dtype-proto/sub-buffer (typed-buf/->typed-buffer buffer) offset length))
  (alias? [lhs-buffer rhs-buffer]
    (dtype-proto/alias? (typed-buf/->typed-buffer lhs-buffer) rhs-buffer))
  (partially-alias? [lhs-buffer rhs-buffer]
    (dtype-proto/partially-alias? (typed-buf/->typed-buffer lhs-buffer) rhs-buffer))

  dtype-proto/PToWriter
  (->object-writer [item]
    (dtype-proto/->object-writer (typed-buf/->typed-buffer item)))
  (->writer-of-type [item datatype unchecked?]
    (dtype-proto/->writer-of-type (typed-buf/->typed-buffer item) datatype unchecked?))

  dtype-proto/PToReader
  (->object-reader [item]
    (dtype-proto/->object-reader (typed-buf/->typed-buffer item)))
  (->reader-of-type [item datatype unchecked?]
    (dtype-proto/->reader-of-type (typed-buf/->typed-buffer item) datatype unchecked?)))


(defn typed-pointer?
  "True if you are can be substituted for typed pointers directly."
  [item]
  (and (typed-buf/typed-buffer-like? item)
       (satisfies? jna/PToPtr item)))


(defn buffer-as-typed-pointer
  "Return a typed pointer that shares the backing store with the original direct
  pointer."
  [item]
  (let [^Buffer nio-buffer (dtype-proto/->buffer-backing-store item)]
    (when (.isDirect nio-buffer)
      (let [item-dtype (dtype-base/get-datatype item)
            ptr-value (Native/getDirectBufferPointer nio-buffer)
            ptr-addr (+ (Pointer/nativeValue ptr-value)
                        (* (.position nio-buffer)
                           (dtype-base/datatype->byte-size item-dtype)))
            ptr-value (Pointer. ptr-addr)]
        (->TypedPointer ptr-value (* (dtype-base/ecount nio-buffer)
                                     (dtype-base/datatype->byte-size item-dtype))
                        item-dtype)))))


(defn as-typed-pointer
  "Get something substitutable as a typed-pointer.  Implement all the protocols
  necessary to be tech.datatype.java-unsigned/typed-buffer *and* PToPtr and you can be
  considered a typed-pointer, *or* if you implemented unsigned/typed-buffer? and your
  backing store is a direct nio buffer (.isDirect returns true)"
  [item]
  (when (typed-pointer? item) item))


(defn ->typed-pointer
  "Creates a typed-pointer object.
  Implement PToPtr, mp/PElementCount, and dtype-base/get-datatype
and we convert your thing to a typed pointer."
  [item]
  ;;Implement 3 protocols and we provide the conversion.
  (let [ptr-data (jna/->ptr-backing-store item)
        ptr-dtype (dtype-base/get-datatype item)
        num-bytes (* (dtype-base/ecount item)
                     (dtype-base/datatype->byte-size ptr-dtype))]
    (->TypedPointer ptr-data num-bytes ptr-dtype)))


(defn typed-pointer->ptr
  ^Pointer [typed-pointer]
  (jna/->ptr-backing-store typed-pointer))



(defn unsafe-address->typed-pointer
  [^long address ^long byte-len datatype]
  (->TypedPointer (Pointer. address) byte-len datatype))


(defmacro typed-data-setter
  [datatype set-fn ptr item-seq]
  `(let [byte-size# (dtype-base/datatype->byte-size ~datatype)]
     (->> ~item-seq
          (map-indexed (fn [idx# val#]
                         (. ~ptr ~set-fn (* (long idx#) byte-size#)
                            (primitive/datatype->unchecked-cast-fn :ignored
                                                                   ~datatype val#))))
          dorun)))


(defn unsafe-free-ptr
  [^Pointer ptr]
  (Native/free (Pointer/nativeValue ptr)))


(defn make-typed-pointer
  "Make a typed pointer.  Aside from the usual option :unchecked?, there is a new option
  :untracked? which means to explicitly avoid using the resource or gc tracking system
  to track this pointer."
  [datatype elem-count-or-seq & [options]]
  (let [typed-buf (when-not (number? elem-count-or-seq)
                    (if (typed-buf/typed-buffer-like? elem-count-or-seq)
                      elem-count-or-seq
                      (typed-buf/make-typed-buffer datatype elem-count-or-seq options)))
        n-elems (if (number? elem-count-or-seq)
                  (long elem-count-or-seq)
                  (dtype-base/ecount typed-buf))
        byte-len (* n-elems (dtype-base/datatype->byte-size datatype))
        data (Native/malloc byte-len)
        retval (unsafe-address->typed-pointer data byte-len datatype)]
    ;;This will be freed if either the resource context is released *or* the return
    ;;value goes out of scope.  In For some use-cases, the returned item should be
    ;;untracked and the callers will assume resposibility for freeing the data.
    (when-not (:untracked? options)
      (resource/track retval #(Native/free data) [:stack :gc]))
    (when typed-buf
      (dtype-base/copy! typed-buf 0 retval 0 n-elems {:unchecked? true}))
    retval))


(defmethod dtype-proto/make-container :native-buffer
  [container-type datatype elem-count options]
  (make-typed-pointer datatype elem-count options))
