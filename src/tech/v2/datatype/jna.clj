(ns tech.v2.datatype.jna
  (:require [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.nio-buffer]
            [tech.v2.datatype.typed-buffer :as typed-buf]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.jna :as jna]
            [tech.resource :as resource])
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


(defn typed-pointer->ptr
  ^Pointer [typed-pointer]
  (jna/->ptr-backing-store typed-pointer))


(defn pointer->nio-buffer
  [^Pointer ptr datatype byte-len]
  (let [buffer (.getByteBuffer ptr 0 byte-len)
        gc-map (:ptr ptr)]
    (case datatype
      :int8 buffer
      :int16 (.asShortBuffer buffer)
      :int32 (.asIntBuffer buffer)
      :int64 (.asLongBuffer buffer)
      :float32 (.asFloatBuffer buffer)
      :float64 (.asDoubleBuffer buffer))))


(defn unsafe-ptr->typed-pointer
  "This is fairly unsafe.  The byte-len and datatype must match
  the actual pointer data.  But the GC can link the data to the
  resultant buffer."
  [^Pointer data ^long byte-len datatype]
  (let [nio-buf (pointer->nio-buffer data
                                     (casting/datatype->host-datatype datatype)
                                     byte-len)]
    (if (= datatype (casting/datatype->host-datatype datatype))
      nio-buf
      (typed-buf/->TypedBuffer datatype nio-buf))))


(defn unsafe-address->typed-pointer
  "This is really unsafe.  The GC won't be able to link Whatever produced the address
  to the produced pointer."
  [^long address ^long byte-len datatype]
  (unsafe-ptr->typed-pointer (make-jna-pointer address) byte-len datatype))


(defn unsafe-free-ptr
  [^Pointer ptr]
  (Native/free (Pointer/nativeValue ptr)))


(defn make-typed-pointer
  "Make a typed pointer.  Aside from the usual option :unchecked?, there is a new option
  :untracked? which means to explicitly avoid using the gc tracking system
  to track this pointer."
  [datatype elem-count-or-seq & [options]]
  (let [n-elems (if (number? elem-count-or-seq)
                  (long elem-count-or-seq)
                  (dtype-base/ecount elem-count-or-seq))
        byte-len (* n-elems (dtype-base/datatype->byte-size datatype))
        data (Native/malloc byte-len)
        retval (unsafe-address->typed-pointer data byte-len datatype)]
    (if-not (number? elem-count-or-seq)
      (dtype-proto/copy-raw->item! elem-count-or-seq retval 0 options)
      (when-not (:skip-init? options)
        (dtype-proto/set-constant! retval 0 0 n-elems)))
    ;;This will be freed if either the resource context is released *or* the return
    ;;value goes out of scope.  In For some use-cases, the returned item should be
    ;;untracked and the callers will assume responsibility for freeing the data.
    (when-not (:untracked? options)
      (resource/track retval #(Native/free data) [:gc]))

    retval))


(defmethod dtype-proto/make-container :native-buffer
  [container-type datatype elem-count options]
  (make-typed-pointer datatype elem-count options))
