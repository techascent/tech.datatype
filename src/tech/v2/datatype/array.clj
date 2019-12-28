(ns tech.v2.datatype.array
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.base :as base]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.typed-buffer :as typed-buffer]
            [tech.jna :as jna])
  (:import [java.nio Buffer ByteBuffer ShortBuffer
            IntBuffer LongBuffer FloatBuffer DoubleBuffer]
           [java.lang.reflect Constructor]
           [tech.v2.datatype ObjectReader ObjectWriter]
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


(declare make-array-of-type)


(defmacro datatype->buffer-creation
  [datatype src-ary]
  (case datatype
    :int8 `(ByteBuffer/wrap ^bytes ~src-ary)
    :int16 `(ShortBuffer/wrap ^shorts ~src-ary)
    :int32 `(IntBuffer/wrap ^ints ~src-ary)
    :int64 `(LongBuffer/wrap ^longs ~src-ary)
    :float32 `(FloatBuffer/wrap ^floats ~src-ary)
    :float64 `(DoubleBuffer/wrap ^doubles ~src-ary)))


(defmacro implement-numeric-array-type
  [ary-cls datatype]
  `(clojure.core/extend
       ~ary-cls
     dtype-proto/PDatatype
     {:get-datatype (fn [arg#] ~datatype)}
     dtype-proto/PToBackingStore
     {:->backing-store-seq (fn [arg#] [(dtype-proto/->sub-array arg#)])}
     dtype-proto/PCopyRawData
     {:copy-raw->item! (fn [raw-data# ary-target# target-offset# options#]
                         (base/raw-dtype-copy! raw-data# ary-target#
                                               target-offset# options#))}
     dtype-proto/PPrototype
     {:from-prototype (fn [src-ary# datatype# shape#]
                        (make-array-of-type datatype# (base/shape->ecount shape#)))}

     dtype-proto/PToNioBuffer
     {:convertible-to-nio-buffer? (fn [item#] true)
      :->buffer-backing-store (fn [item#] (datatype->buffer-creation ~datatype item#))}

     dtype-proto/PToArray
     {:->sub-array (fn [item#]
                     {:java-array item#
                      :offset 0
                      :length (count item#)})
      :->array-copy (fn [item#]
                      (base/copy! item# (make-array-of-type ~datatype
                                                            (count item#))))}
     dtype-proto/PBuffer
     {:sub-buffer (fn [buffer# offset# length#]
                    (dtype-proto/sub-buffer (dtype-proto/->buffer-backing-store buffer#)
                                            offset# length#))}
     dtype-proto/PToList
     {:convertible-to-fastutil-list? (fn [item#] true)
      :->list-backing-store (fn [item#]
                              (typecast/wrap-array-fastpath ~datatype item#))}))


(implement-numeric-array-type (Class/forName "[B") :int8)
(implement-numeric-array-type (Class/forName "[S") :int16)
(implement-numeric-array-type (Class/forName "[I") :int32)
(implement-numeric-array-type (Class/forName "[J") :int64)
(implement-numeric-array-type (Class/forName "[F") :float32)
(implement-numeric-array-type (Class/forName "[D") :float64)


(extend-type (Class/forName "[Z")
  dtype-proto/PDatatype
  (get-datatype [_] :boolean)

  dtype-proto/PToBackingStore
  (->backing-store-seq [arg] [(dtype-proto/->sub-array arg)])

  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target offset options]
    (base/raw-dtype-copy! raw-data ary-target offset options))

  dtype-proto/PPrototype
  (from-prototype [src-ary datatype shape]
    (make-array-of-type datatype (base/shape->ecount shape)))

  dtype-proto/PBuffer
  (sub-buffer [buffer offset length]
    (dtype-proto/sub-buffer (dtype-proto/->list-backing-store buffer) offset length))

  dtype-proto/PToArray
  (->sub-array [item]
    {:java-array item
     :offset 0
     :length (alength ^booleans item)})
  (->array-copy [src-ary]
    (base/copy! src-ary (make-array-of-type
                         :boolean
                         (alength (typecast/as-boolean-array src-ary)))))

  dtype-proto/PToList
  (convertible-to-fastutil-list? [item] true)
  (->list-backing-store [item]
    (typecast/wrap-array-fastpath :boolean item)))


(extend-type (Class/forName "[C")
  dtype-proto/PDatatype
  (get-datatype [_] :int32)

  dtype-proto/PToBackingStore
  (->backing-store-seq [arg] [(dtype-proto/->sub-array arg)])

  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target offset options]
    (base/raw-dtype-copy! raw-data ary-target offset options))

  dtype-proto/PPrototype
  (from-prototype [src-ary datatype shape]
    (make-array-of-type datatype (base/shape->ecount shape)))

  dtype-proto/PBuffer
  (sub-buffer [buffer offset length]
    (dtype-proto/sub-buffer (dtype-proto/->list-backing-store buffer) offset length))

  dtype-proto/PToArray
  (->sub-array [item]
    {:java-array item
     :offset 0
     :length (alength ^booleans item)})
  (->array-copy [src-ary]
    (base/copy! src-ary (make-array-of-type
                         :char
                         (alength (typecast/as-char-array src-ary)))))

  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (let [^chars char-data item
          n-items (alength char-data)]
      (-> (reify tech.v2.datatype.IntReader
            (lsize [item] n-items)
            (read [item idx]
              (-> (aget char-data idx)
                  int)))
          (dtype-proto/->reader options))))


  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item options]
    (let [^chars char-data item
          n-items (alength char-data)]
      (-> (reify tech.v2.datatype.IntWriter
            (lsize [item] n-items)
            (write [item idx value]
              (aset char-data idx (char value))))
          (dtype-proto/->writer options))))


  dtype-proto/PToList
  (convertible-to-fastutil-list? [item] true)
  (->list-backing-store [item]
    (typecast/wrap-array-fastpath :char item)))


(defonce ^:dynamic *array-constructors* (atom {}))


(defn add-array-constructor!
  [item-dtype cons-fn]
  (swap! *array-constructors* assoc item-dtype cons-fn)
  (keys @*array-constructors*))


(defn add-numeric-array-constructor
  [item-dtype ary-cons-fn]
  (add-array-constructor!
   item-dtype
   (fn [elem-count-or-seq options]
     (cond
       (number? elem-count-or-seq)
       (ary-cons-fn elem-count-or-seq)
       (and (dtype-proto/convertible-to-reader? elem-count-or-seq))
       (if (= item-dtype (base/get-datatype elem-count-or-seq))
         (dtype-proto/->array-copy elem-count-or-seq)
         (let [n-elems (base/ecount elem-count-or-seq)]
           (base/copy! elem-count-or-seq 0
                       (ary-cons-fn n-elems) 0
                       n-elems options)))
       :else
       (let [elem-count-or-seq (if (or (number? elem-count-or-seq)
                                       (:unchecked? options))
                                 elem-count-or-seq
                                 (map #(casting/cast % item-dtype) elem-count-or-seq))]
         (ary-cons-fn elem-count-or-seq))))))


(add-numeric-array-constructor :int8 byte-array)
(add-numeric-array-constructor :int16 short-array)
(add-numeric-array-constructor :int32 int-array)
(add-numeric-array-constructor :int64 long-array)
(add-numeric-array-constructor :float32 float-array)
(add-numeric-array-constructor :float64 double-array)
(add-numeric-array-constructor :boolean boolean-array)
(add-numeric-array-constructor :char char-array)


(defn make-object-array-of-type
  [obj-type elem-count-or-seq options]
  (let [constructor (when (:constructor options)
                      (:constructor options))
        elem-count-or-seq (if (number? elem-count-or-seq)
                            elem-count-or-seq
                            (if constructor
                              (map constructor elem-count-or-seq)
                              elem-count-or-seq))]
    (if (number? elem-count-or-seq)
      (if constructor
        (into-array obj-type (repeatedly (long elem-count-or-seq)
                                         constructor))
        (make-array obj-type (long elem-count-or-seq)))
      (into-array obj-type elem-count-or-seq))))


(defn make-array-of-type
  ([datatype elem-count-or-seq options]
   (if (instance? Class datatype)
     (make-object-array-of-type datatype elem-count-or-seq options)
     (if-let [cons-fn (get @*array-constructors* datatype)]
       (cons-fn elem-count-or-seq options)
       (make-object-array-of-type Object elem-count-or-seq options))))
  ([datatype elem-count-or-seq]
   (make-array-of-type datatype elem-count-or-seq {})))


(defmethod dtype-proto/make-container :java-array
  [_ datatype elem-count-or-seq options]
  (make-array-of-type datatype elem-count-or-seq options))


(defonce ^:dynamic *object-array-datatype-override* (atom nil))


(defn add-object-array-datatype-override!
  [cls-dtype override-val]
  (swap! *object-array-datatype-override* assoc cls-dtype override-val)
  (keys @*object-array-datatype-override*))


(defn extend-object-array-type
  [obj-ary-cls]
  (when-not (.isArray ^Class obj-ary-cls)
    (throw (ex-info "Obj class is not an array class" {})))

  (clojure.core/extend
      obj-ary-cls
    dtype-proto/PDatatype
    {:get-datatype (fn [item]
                     (let [ary-data-cls (.getComponentType ^Class (type item))]
                       (get @*object-array-datatype-override* ary-data-cls
                            ary-data-cls)))}

    dtype-proto/PCountable
    {:ecount (fn [item] (alength ^"[Ljava.lang.Object;" item))}

    dtype-proto/PToBackingStore
    {:->backing-store-seq (fn [item] [(dtype-proto/->sub-array item)])}

    dtype-proto/PBuffer
    {:sub-buffer
     (fn [buffer offset length]
       (-> (dtype-proto/sub-buffer (dtype-proto/->list-backing-store buffer)
                                   offset length)
           (typed-buffer/set-datatype (dtype-proto/get-datatype buffer))))}

    dtype-proto/PToList
    {:convertible-to-fastutil-list? (constantly true)
     :->list-backing-store (fn [item#]
                             (ObjectArrayList/wrap item# (count item#)))}


    dtype-proto/PCopyRawData
    {:copy-raw->item! (fn [raw-data ary-target offset options]
                        (base/raw-dtype-copy! raw-data ary-target offset options))}


    dtype-proto/PPrototype
    {:from-prototype (fn [_ datatype shape]
                       (make-array-of-type datatype (base/shape->ecount shape)))}


    dtype-proto/PToReader
    {:convertible-to-reader? (constantly true)
     :->reader
     (fn [item options]
       (let [datatype (or (:datatype options)
                          (dtype-proto/get-datatype item))
             unchecked? (:unchecked? options)
             ^"[Ljava.lang.Object;" item item]
         (reify
           ObjectReader
           (getDatatype [reader] datatype)
           (lsize [reader] (alength item))
           (read [reader idx]
             (aget item idx))
           dtype-proto/PToList
           (convertible-to-fastutil-list? [reader] true)
           (->list-backing-store [reader] (dtype-proto/as-list item))
           dtype-proto/PToArray
           (->sub-array [reader] (dtype-proto/->sub-array item))
           (->array-copy [reader] (dtype-proto/->array-copy item))

           dtype-proto/PBuffer
           (sub-buffer [reader offset length]
             (-> (dtype-proto/sub-buffer item offset length)
                 (dtype-proto/->reader {:datatype datatype
                                        :unchecked? unchecked?})))
           dtype-proto/PSetConstant
           (set-constant! [reader offset value elem-count]
             (dtype-proto/set-constant! item offset value elem-count)))))}

    dtype-proto/PToWriter
    {:convertible-to-writer? (constantly true)
     :->writer
     (fn [item options]
       (let [datatype (or (:datatype options)
                          (dtype-proto/get-datatype item))
             unchecked? (:unchecked? options)
             ^"[Ljava.lang.Object;" item item]
         (reify
           ObjectWriter
           (getDatatype [writer] datatype)
           (lsize [writer] (alength item))
           (write [writer idx value]
             (aset item idx value))
           dtype-proto/PToList
           (convertible-to-fastutil-list? [writer] true)
           (->list-backing-store [writer] (dtype-proto/as-list item))
           dtype-proto/PToArray
           (->sub-array [writer] (dtype-proto/->sub-array item))
           (->array-copy [writer] (dtype-proto/->array-copy item))
           dtype-proto/PBuffer
           (sub-buffer [writer offset length]
             (-> (dtype-proto/sub-buffer item offset length)
                 (dtype-proto/->writer {:datatype datatype
                                        :unchecked? unchecked?})))
           dtype-proto/PSetConstant
           (set-constant! [writer offset value elem-count]
             (dtype-proto/set-constant! item offset value elem-count)))))}

    dtype-proto/PToArray
    {:->sub-array (fn [item] {:java-array item
                              :offset 0
                              :length (count item)})
     :->array-copy (fn [src-ary]
                     (base/copy! src-ary
                                 (make-array-of-type
                                  (base/get-datatype src-ary)
                                  (alength (typecast/as-object-array src-ary)))))}))
