(ns tech.datatype.array
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.base :as base]
            [clojure.core.matrix.protocols :as mp]
            [tech.datatype.casting :as casting]
            [tech.datatype.typecast :refer :all :as typecast]
            [tech.datatype.typed-buffer :as typed-buffer]
            [tech.jna :as jna])
  (:import [java.nio Buffer ByteBuffer ShortBuffer
            IntBuffer LongBuffer FloatBuffer DoubleBuffer]
           [java.lang.reflect Constructor]
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
                        (when-not (= 1 (count shape#))
                          (throw (ex-info "arrays are 1 dimensional" {})))
                        (make-array-of-type datatype# (base/shape->ecount shape#)))}

     dtype-proto/PToNioBuffer
     {:convertible-to-nio-buffer? (fn [item#] true)
      :->buffer-backing-store (fn [item#] (datatype->buffer-creation ~datatype item#))}

     dtype-proto/PToArray
     {:->sub-array (fn [item#]
                     {:array-data item#
                      :offset 0
                      :length (base/ecount item#)})
      :->array-copy (fn [item#]
                      (base/copy! item# (make-array-of-type ~datatype
                                                            (base/ecount item#))))}
     dtype-proto/PBuffer
     {:sub-buffer (fn [buffer# offset# length#]
                    (dtype-proto/sub-buffer (dtype-proto/->buffer-backing-store buffer#)
                                            offset# length#))}
     dtype-proto/PToList
     {:convertible-to-fastutil-list? (fn [item#] true)
      :->list-backing-store (fn [item#]
                              (typecast/wrap-array-with-list item#))}
     dtype-proto/PToReader
     {:->reader-of-type (fn [item# datatype# unchecked?#]
                          (dtype-proto/->reader-of-type
                           (dtype-proto/->buffer-backing-store item#)
                           datatype# unchecked?#))}


     dtype-proto/PToWriter
     {:->writer-of-type (fn [item# datatype# unchecked?#]
                          (dtype-proto/->writer-of-type
                           (dtype-proto/->buffer-backing-store item#)
                           datatype#
                           unchecked?#))}

     dtype-proto/PToIterable
     {:->iterable-of-type (fn [item# datatype# unchecked?#]
                            (dtype-proto/->reader-of-type
                             item# datatype# unchecked?#))}))


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
    {:array-data item
     :offset 0
     :length (alength ^booleans item)})
  (->array-copy [src-ary]
    (base/copy! src-ary (make-array-of-type
                         :boolean
                         (alength (as-boolean-array src-ary)))))

  dtype-proto/PToList
  (convertible-to-fastutil-list? [item] true)
  (->list-backing-store [item]
    (typecast/wrap-array-with-list item))


  dtype-proto/PToWriter
  (->writer-of-type [item datatype unchecked?]
    (dtype-proto/->writer-of-type (dtype-proto/->list-backing-store item)
                                  datatype unchecked?))


  dtype-proto/PToReader
  (->reader-of-type [item datatype unchecked?]
    (dtype-proto/->reader-of-type (dtype-proto/->list-backing-store item)
                                  datatype unchecked?))

  dtype-proto/PToIterable
  (->iterable-of-type [item datatype unchecked?]
    (dtype-proto/->reader-of-type item datatype unchecked?)))


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
       (and (satisfies? dtype-proto/PToReader elem-count-or-seq)
            (satisfies? dtype-proto/PBuffer elem-count-or-seq))
       (if (and (satisfies? dtype-proto/PToArray elem-count-or-seq)
                (= item-dtype (base/get-datatype elem-count-or-seq)))
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


(defn make-object-array-of-type
  [obj-type elem-count-or-seq options]
  (let [elem-count-or-seq (if (or (number? elem-count-or-seq)
                                     (:unchecked? options))
                               elem-count-or-seq
                               (map (partial jna/ensure-type obj-type)
                                    elem-count-or-seq))]
    (if (number? elem-count-or-seq)
      (let [constructor (if (:construct? options)
                          (.getConstructor ^Class obj-type (make-array Class 0))
                          nil)]
        (if constructor
          (into-array obj-type (repeatedly (long elem-count-or-seq)
                                           #(.newInstance
                                             ^Constructor constructor
                                             (make-array Object 0))))
          (make-array obj-type (long elem-count-or-seq))))
      (into-array obj-type elem-count-or-seq))))


(defn make-array-of-type
  ([datatype elem-count-or-seq options]
   (if (instance? Class datatype)
     (make-object-array-of-type datatype elem-count-or-seq options)
     (if-let [cons-fn (get @*array-constructors* datatype)]
       (cons-fn elem-count-or-seq options)
       (throw (ex-info (format "Failed to find constructor for datatype %s" datatype)
                       {:datatype datatype})))))
  ([datatype elem-count-or-seq]
   (make-array-of-type datatype elem-count-or-seq {})))


(defmethod dtype-proto/make-container :java-array
  [container-type datatype elem-count-or-seq options]
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

    dtype-proto/PToBackingStore
    {:->backing-store-seq (fn [item] [(dtype-proto/->sub-array item)])}

    dtype-proto/PBuffer
    {:sub-buffer
     (fn [buffer offset length]
       (-> (dtype-proto/sub-buffer (dtype-proto/->list-backing-store buffer)
                                   offset length)
           (typed-buffer/set-datatype (dtype-proto/get-datatype buffer))))}

    dtype-proto/PToList
    {:convertible-to-fastutil-list? (fn [item#] true)
     :->list-backing-store (fn [item#]
                             (ObjectArrayList/wrap item# (base/ecount item#)))}


    dtype-proto/PCopyRawData
    {:copy-raw->item! (fn [raw-data ary-target offset options]
                        (base/raw-dtype-copy! raw-data 0 ary-target offset
                                              (base/ecount raw-data) options))}


    dtype-proto/PPrototype
    {:from-prototype (fn [src-ary datatype shape]
                       (make-array-of-type datatype (base/shape->ecount shape)))}

    dtype-proto/PToArray
    {:->sub-array (fn [item] {:array-data item
                              :offset 0
                              :length (base/ecount item)})
     :->array-copy (fn [src-ary]
                     (base/copy! src-ary
                                 (make-array-of-type
                                  (base/get-datatype src-ary)
                                  (alength (as-object-array src-ary)))))}
    dtype-proto/PToWriter
    {:->writer-of-type
     (fn [item# datatype# unchecked?#]
       (dtype-proto/->writer-of-type (dtype-proto/->list-backing-store item#)
                                     datatype# unchecked?#))}


    dtype-proto/PToReader
    {:->reader-of-type
     (fn [item# datatype# unchecked?#]
       (dtype-proto/->reader-of-type (dtype-proto/->list-backing-store item#)
                                     datatype# unchecked?#))}

    dtype-proto/PToIterable
    {:->iterable-of-type (fn [item# datatype# unchecked?#]
                           (dtype-proto/->reader-of-type
                            item# datatype# unchecked?#))}))


(extend-object-array-type (Class/forName "[Ljava.lang.Object;"))
(extend-object-array-type (Class/forName "[Ljava.lang.String;"))
(add-object-array-datatype-override! String :string)
(add-numeric-array-constructor :string #(make-object-array-of-type String % {:construct? true}))
