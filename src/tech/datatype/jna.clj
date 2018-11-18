(ns tech.datatype.jna
  (:require [tech.datatype.base :as dtype-base]
            [tech.datatype.java-primitive :as primitive]
            [tech.datatype.java-unsigned :as unsigned]
            [tech.datatype :as dtype]
            [tech.jna :as jna]
            [tech.resource :as resource]
            [tech.gc-resource :as gc-resource]
            [clojure.core.matrix.protocols :as mp])
  (:import [com.sun.jna Pointer Native Function NativeLibrary]
           [com.sun.jna.ptr PointerByReference]
           [java.nio ByteBuffer Buffer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn- integer-datatype?
  [datatype]
  (boolean (#{:int8 :uint8 :int16 :uint16 :int32 :uint32 :int64 :uint64} datatype)))


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

  dtype-base/PDatatype
  (get-datatype [item] datatype)

  dtype-base/PAccess
  (set-value! [item offset value]
    (dtype-base/set-value! (unsigned/->typed-buffer item)
                           offset value))
  (set-constant! [item offset value elem-count]
    (when (< (long offset) 0 )
      (throw (ex-info "Offset out of range!"
                      {:offset offset})))
    (let [lval (long value)]
      (when-not (<= (long elem-count)
                    (- (dtype-base/ecount item)
                       (long offset)))
        (throw (ex-info "Element count out of range"
                        {:offset offset
                         :item-ecount (dtype-base/ecount item)
                         :elem-count elem-count})))
      (dtype-base/set-constant! (unsigned/->typed-buffer item) offset
                                value elem-count)))

  (get-value [item offset]
    (dtype-base/get-value (unsigned/->typed-buffer item) offset))

  mp/PElementCount
  (element-count [_] (quot byte-len
                           (dtype-base/datatype->byte-size datatype)))
  dtype-base/PContainerType
  (container-type [item] :jna-buffer)

  dtype-base/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (dtype-base/copy-raw->item! (unsigned/->typed-buffer raw-data) ary-target
                                target-offset options))

  dtype-base/PPrototype
  (from-prototype [item datatype shape]
    (make-typed-pointer datatype (dtype-base/shape->ecount shape)))

  primitive/PToBuffer
  (->buffer-backing-store [item]
    (let [jvm-type (unsigned/datatype->jvm-datatype datatype)
          buffer (.getByteBuffer ptr 0 byte-len)]
      (case jvm-type
        :int8 buffer
        :int16 (.asShortBuffer buffer)
        :int32 (.asIntBuffer buffer)
        :int64 (.asLongBuffer buffer)
        :float32 (.asFloatBuffer buffer)
        :float64 (.asDoubleBuffer buffer))))

  primitive/POffsetable
  (offset-item [item offset]
    (let [offset (long offset)
          byte-offset (* offset
                         (dtype-base/datatype->byte-size
                          (dtype-base/get-datatype item)))]
      (when-not (< (dtype-base/ecount item)
                   offset)
        (throw (ex-info "Offset out of range:"
                        {:offset offset
                         :ecount (dtype-base/ecount item)})))
      (->TypedPointer (offset-pointer ptr byte-offset)
                      (- byte-len byte-offset)
                      datatype)))

  primitive/PToArray
  (->array [item] nil)
  (->array-copy [item] (primitive/->array-copy
                        (unsigned/->typed-buffer item))))


(defn typed-pointer?
  "True if you are can be substituted for typed pointers directly."
  [item]
  (and (unsigned/typed-buffer? item)
       (satisfies? jna/PToPtr item)))


(defn buffer-as-typed-pointer
  "Return a typed pointer that shares the backing store with the original direct
  pointer."
  [item]
  (let [^Buffer nio-buffer (primitive/->buffer-backing-store item)]
    (when (.isDirect nio-buffer)
      (let [item-dtype (dtype/get-datatype item)
            ptr-value (Native/getDirectBufferPointer nio-buffer)
            ptr-addr (+ (Pointer/nativeValue ptr-value)
                        (* (.position nio-buffer)
                           (dtype-base/datatype->byte-size item-dtype)))
            ptr-value (Pointer. ptr-addr)]
        (->TypedPointer ptr-value (* (dtype/ecount nio-buffer)
                                     (dtype/datatype->byte-size item-dtype))
                        item-dtype)))))


(defn as-typed-pointer
  "Get something substitutable as a typed-pointer.  Implement all the protocols
  necessary to be tech.datatype.java-unsigned/typed-buffer *and* PToPtr and you can be
  considered a typed-pointer, *or* if you implemented unsigned/typed-buffer? and your
  backing store is a direct nio buffer (.isDirect returns true)"
  [item]
  (cond
    (typed-pointer? item)
    item
    (unsigned/typed-buffer? item)
    (buffer-as-typed-pointer item)))


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


(dtype-base/add-container-conversion-fn
 :jna-buffer :typed-buffer
 (fn [dest-type jna-buf]
   [(unsigned/->typed-buffer jna-buf)
    0]))


(defn unsafe-address->typed-pointer
  [^long address ^long byte-len datatype]
  (->TypedPointer (Pointer. address) byte-len datatype))


(defmacro typed-data-setter
  [datatype set-fn ptr item-seq]
  `(let [byte-size# (dtype-base/datatype->byte-size ~datatype)]
     (->> ~item-seq
          (map-indexed (fn [idx# val#]
                         (. ~ptr ~set-fn (* (long idx#) byte-size#)
                            (primitive/datatype->unchecked-cast-fn :ignored ~datatype val#))))
          dorun)))


(defn make-typed-pointer
  [datatype elem-count-or-seq & [options]]
  (let [n-elems (long (if (number? elem-count-or-seq)
                        elem-count-or-seq
                        (count elem-count-or-seq)))
        elem-count-or-seq (unsigned/unsigned-safe-elem-count-or-seq
                           datatype elem-count-or-seq options)
        byte-len (* n-elems (dtype-base/datatype->byte-size datatype))
        data (Native/malloc byte-len)
        retval (unsafe-address->typed-pointer data byte-len datatype)]
    ;;This will be freed if either the resource context is released *or*
    ;;the return value goes out of scope.
    (gc-resource/track retval #(Native/free data))
    (when-not (number? elem-count-or-seq)
      (let [jvm-datatype (unsigned/datatype->jvm-datatype datatype)
            data-ary (dtype/make-array-of-type jvm-datatype elem-count-or-seq {:unchecked? true})]
        (dtype/copy! data-ary 0 retval 0 n-elems {:unchecked? true})))
    retval))


(defmacro ^:private array->buffer-copy
  [datatype]
  (let [jvm-dtype (unsigned/datatype->jvm-datatype datatype)]
    `(fn [src# src-offset# dst# dst-offset# n-elems# options#]
       (let [src# (primitive/datatype->array-cast-fn ~jvm-dtype src#)
             dst-ptr# (typed-pointer->ptr dst#)]
         (.write dst-ptr# (int dst-offset#) src# (int src-offset#) (int n-elems#))))))


(defmacro ^:private buffer->array-copy
  [datatype]
  (let [jvm-dtype (unsigned/datatype->jvm-datatype datatype)]
    `(fn [src# src-offset# dst# dst-offset# n-elems# options#]
       (let [src# (typed-pointer->ptr src#)
             dst# (primitive/datatype->array-cast-fn ~jvm-dtype dst#)]
         (.read src# (int src-offset#) dst# (int dst-offset#) (int n-elems#))))))


(defn- buffer->buffer-copy
  [src src-offset dst dst-offset n-elems options]
  (let [src-dtype (dtype-base/get-datatype src)
        dst-dtype (dtype-base/get-datatype dst)]
    (if (or (= src-dtype dst-dtype)
            (and (:unchecked? options)
                 (unsigned/direct-conversion? src-dtype dst-dtype)))
      ;;Erase the unsigned datatype by going directly to the backing store
      ;;This will hit the optimized memcpy pathway
      (dtype-base/copy! (primitive/->buffer-backing-store src) src-offset
                        (primitive/->buffer-backing-store dst) dst-offset
                        n-elems options)
      (dtype-base/copy! (unsigned/->typed-buffer src) src-offset
                        (unsigned/->typed-buffer dst) dst-offset
                        n-elems options))))


(defn- attempt-as-typed-pointer-copy
  [src src-offset dst dst-offset n-elems options]
  (let [src-ptr (as-typed-pointer src)
        dst-ptr (as-typed-pointer dst)]
    (println src-ptr dst-ptr src dst)
    (if (and src-ptr dst-ptr)
      (do
        (buffer->buffer-copy src-ptr src-offset dst-ptr dst-offset n-elems options))
      (do
        (dtype-base/copy! (unsigned/->typed-buffer src) src-offset
                          (unsigned/->typed-buffer dst) dst-offset
                          n-elems options)))))


;;Override the typed-buffer copy mechanism to check if the thing can be converted
;;to a typed pointer which has faster copy mechanisms
(def possible-fast-path
  (->>
   (for [src-dtype unsigned/datatypes
         dst-dtype unsigned/datatypes]
     (do
       (dtype-base/add-copy-operation :typed-buffer :jna-buffer src-dtype dst-dtype true
                                      attempt-as-typed-pointer-copy)
       (dtype-base/add-copy-operation :jna-buffer :typed-buffer src-dtype dst-dtype true
                                      attempt-as-typed-pointer-copy)
       (dtype-base/add-copy-operation :nio-buffer :jna-buffer src-dtype dst-dtype true
                                      attempt-as-typed-pointer-copy)
       (dtype-base/add-copy-operation :jna-buffer :nio-buffer src-dtype dst-dtype true
                                      attempt-as-typed-pointer-copy)
       [src-dtype dst-dtype]))
   vec))


(defmacro array-copy-fns
  []
  (->> (for [dtype unsigned/datatypes]
         (let [jvm-dtype (unsigned/datatype->jvm-datatype dtype)]
           (if (= dtype jvm-dtype)
             `(let [to-copy-fn# (array->buffer-copy ~dtype)
                    from-copy-fn# (buffer->array-copy ~dtype)]
                (dtype-base/add-copy-operation :java-array :jna-buffer ~dtype ~dtype true to-copy-fn#)
                (dtype-base/add-copy-operation :java-array :jna-buffer ~dtype ~dtype false to-copy-fn#)
                (dtype-base/add-copy-operation :jna-buffer :java-array ~dtype ~dtype true from-copy-fn#)
                (dtype-base/add-copy-operation :jna-buffer :java-array ~dtype ~dtype false from-copy-fn#)
                ~dtype)
             `(let [to-copy-fn# (array->buffer-copy ~dtype)
                    from-copy-fn# (buffer->array-copy ~dtype)]
                (dtype-base/add-copy-operation :java-array :jna-buffer ~jvm-dtype ~dtype true to-copy-fn#)
                (dtype-base/add-copy-operation :jna-buffer :java-array ~dtype ~jvm-dtype true from-copy-fn#)
                ~dtype))))
       vec))

(def ^:private primitive-array-copy (array-copy-fns))


(def ^:private buffer-copy
  (->> (for [[src-dtype dst-dtype] unsigned/all-possible-datatype-pairs
             unchecked? [true false]]
         (do
           (dtype-base/add-copy-operation :jna-buffer :jna-buffer src-dtype dst-dtype unchecked?
                                          buffer->buffer-copy)
           [src-dtype dst-dtype unchecked?]))
       vec))
