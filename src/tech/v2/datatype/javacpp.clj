(ns tech.v2.datatype.javacpp
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.jna :as dtype-jna]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typed-buffer :as typed-buffer]
            [tech.jna :as jna]
            [tech.resource :as resource]
            [tech.resource.stack :as stack])
  (:import [org.bytedeco.javacpp
            BytePointer IntPointer LongPointer DoublePointer
            Pointer PointerPointer FloatPointer ShortPointer
            Pointer$DeallocatorReference Loader]
           [java.lang.reflect Field]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defprotocol PToPtr
  "Anything convertible to a pointer that shares the backing store.  Datatypes do not
  have to match."
  (convertible-to-javacpp-ptr? [item])
  (->javacpp-ptr [item]))


(defn as-javacpp-ptr
  [item]
  (when (convertible-to-javacpp-ptr? item)
    (->javacpp-ptr [item])))

;;Necessary for testing
(comment
  (import org.bytedeco.javacpp.opencv_core)
  )


;;!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
;; disable the javacpp auto-gc system.  This causes spurious OOM errors
;; and runs the GC endlessly at times when the amount of C++ memory allocated
;; is large compared to the maximum java heap size.
(System/setProperty "org.bytedeco.javacpp.nopointergc" "true")


(extend-protocol dtype-proto/PDatatype
  BytePointer
  (get-datatype [ptr] :int8)
  ShortPointer
  (get-datatype [ptr] :int16)
  IntPointer
  (get-datatype [ptr] :int32)
  LongPointer
  (get-datatype [ptr] :int64)
  FloatPointer
  (get-datatype [ptr] :float32)
  DoublePointer
  (get-datatype [ptr] :float64))


(defn make-empty-pointer-of-type
  ^Pointer [datatype]
  (case (casting/host-flatten datatype)
    :int8 (BytePointer.)
    :int16 (ShortPointer.)
    :int32 (IntPointer.)
    :int64 (LongPointer.)
    :float32 (FloatPointer.)
    :float64 (DoublePointer.)))


(defn- get-private-field [^Class cls field-name]
  (let [^Field field (first (filter
                             (fn [^Field x] (.. x getName (equals field-name)))
                             (.getDeclaredFields cls)))]
    (.setAccessible field true)
    field))

(defonce address-field (get-private-field Pointer "address"))
(defonce limit-field (get-private-field Pointer "limit"))
(defonce capacity-field (get-private-field Pointer "capacity"))
(defonce position-field (get-private-field Pointer "position"))
(defonce deallocator-field (get-private-field Pointer "deallocator"))


(defn offset-pointer
  "Create a 'fake' temporary pointer to use in api calls.  Note this function is
threadsafe while (.position ptr offset) is not."
  ^Pointer [^Pointer ptr ^long offset]
  (let [addr (.address ptr)
        pos (.position ptr)
        retval (make-empty-pointer-of-type (dtype/get-datatype ptr))]
    ;;Do not ever set position - this will fail in most api calls as the javacpp
    ;;code for dealing with position is incorrect.
    (.set ^Field address-field retval (+ addr
                                         (* (+ pos offset)
                                            (dtype-base/datatype->byte-size
                                             (dtype-base/get-datatype ptr)))))
    retval))


(defn duplicate-pointer
  ^Pointer [^Pointer ptr]
  (let [addr (.address ptr)
        pos (.position ptr)
        limit (.limit ptr)
        capacity (.capacity ptr)
        retval (make-empty-pointer-of-type (dtype/get-datatype ptr))]
    (.set ^Field address-field retval addr)
    (.set ^Field position-field retval pos)
    (.set ^Field limit-field retval limit)
    (.set ^Field capacity-field retval capacity)
    retval))


(defn set-pointer-limit-and-capacity
  ^Pointer [^Pointer ptr ^long elem-count]
  (.set ^Field limit-field ptr elem-count)
  (.set ^Field capacity-field ptr elem-count)
  ptr)


(defn release-pointer
  [^Pointer ptr]
  (.close ptr)
  (.deallocate ptr false)
  (.set ^Field deallocator-field ptr nil))


(defn ptr->typed-buffer
  [item]
  (let [item-dtype (dtype/get-datatype item)]
    (dtype-jna/unsafe-address->typed-pointer
     (com.sun.jna.Pointer/nativeValue (jna/->ptr-backing-store item))
     (* (dtype/ecount item) (casting/numeric-byte-width (dtype/get-datatype item)))
     (dtype/get-datatype item))))


(extend-type Pointer
  stack/PResource
  (release-resource [ptr] (release-pointer ptr))
  dtype-proto/PCountable
  (ecount [ptr] (.capacity ptr))
  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (dtype-proto/copy-raw->item! (ptr->typed-buffer raw-data)
                                 ary-target
                                 target-offset options))

  dtype-proto/PPrototype
  (from-prototype [ptr datatype shape]
    (-> (dtype-proto/make-container :native-buffer datatype (apply * shape) {})
        (->javacpp-ptr)))


  PToPtr
  (convertible-to-javacpp-ptr? [item] true)
  (->javacpp-ptr [item] item)

  jna/PToPtr
  (is-jna-ptr-convertible? [item] true)
  (->ptr-backing-store [item]
    ;;Anything convertible to a pointer is convertible to a jna ptr too.
    (let [^Pointer item-ptr (->javacpp-ptr item)
          retval
          (dtype-jna/make-jna-pointer (.address item-ptr))
          src-map {:ptr-data item-ptr}]
      (resource/track retval #(get src-map :ptr-data) [:gc])))


  dtype-proto/PToNioBuffer
  (convertible-to-nio-buffer? [src] true)
  (->buffer-backing-store [src]
    (dtype-proto/->buffer-backing-store
     (ptr->typed-buffer src)))


  dtype-proto/PBuffer
  (sub-buffer [src offset length]
    (dtype-proto/sub-buffer (ptr->typed-buffer src)
                            offset length))


  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (dtype-proto/->reader (ptr->typed-buffer item) options))


  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item options]
    (dtype-proto/->writer (ptr->typed-buffer item) options))


  dtype-proto/PToArray
  (->sub-array [src] nil)
  (->array-copy [src]
    (dtype-proto/->array-copy (ptr->typed-buffer src))))


(defn make-pointer-of-type
  [datatype ecount-or-seq]
  (-> (dtype-proto/make-container :native-buffer datatype ecount-or-seq {})
      (->javacpp-ptr)))


(defn make-typed-pointer
  "This module no longer has a typed pointer, function provided to ease portability
to jna system."
  [datatype elem-seq-or-count & [options]]
  (dtype-jna/make-typed-pointer datatype elem-seq-or-count options))


(defn as-jpp-pointer
  "Create a jcpp pointer that shares the backing store with the thing.
Thing must implement tech.jna/PToPtr,
tech.datatype.base/PDatatype, and clojure.core.matrix.protocols/PElementCount."
  [item]
  (let [jna-ptr (jna/as-ptr item)
        elem-count (dtype/ecount item)
        datatype (dtype/get-datatype item)]
    (when (and jna-ptr
               (casting/host-numeric-types datatype)
               (not= 0 elem-count))
      (-> (make-empty-pointer-of-type (casting/host-flatten datatype))
          (offset-pointer (/ (com.sun.jna.Pointer/nativeValue jna-ptr)
                             (casting/numeric-byte-width datatype)))
          (set-pointer-limit-and-capacity elem-count)))))


(extend-type Object
  PToPtr
  (convertible-to-javacpp-ptr? [item]
    (let [jna-ptr (jna/as-ptr item)
        elem-count (dtype/ecount item)
        datatype (dtype/get-datatype item)]
    (and jna-ptr
         (casting/numeric-type? datatype)
         (not= 0 elem-count))))
  (->javacpp-ptr [item]
    (as-jpp-pointer item)))
