(ns tech.datatype.marshal
  "Namespace to contain the madness that happens when you want to marshal
  an (nio buffer or array) or one type to a (nio buffer or array) or another type."
  (:require [clojure.core.matrix.macros :refer [c-for]]
            [tech.datatype.base :as base]
            [clojure.set :as cset])
  (:import [java.nio ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer Buffer]
           [tech.datatype DoubleArrayView FloatArrayView
            LongArrayView IntArrayView ShortArrayView ByteArrayView
            ArrayView ArrayViewBase]))

;;Some utility items to make the macros easier.
(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defprotocol PContainerType
  (container-type [item]))

(extend-type ArrayViewBase
  PContainerType
  (container-type [item] :array-view))

(extend-type Buffer
  PContainerType
  (container-type [item] :nio-buffer))

(extend-type (Class/forName "[B")
  PContainerType
  (container-type [item] :java-array))

(extend-type (Class/forName "[S")
  PContainerType
  (container-type [item] :java-array))

(extend-type (Class/forName "[I")
  PContainerType
  (container-type [item] :java-array))

(extend-type (Class/forName "[J")
  PContainerType
  (container-type [item] :java-array))

(extend-type (Class/forName "[F")
  PContainerType
  (container-type [item] :java-array))

(extend-type (Class/forName "[D")
  PContainerType
  (container-type [item] :java-array))


;;Conversion is src-container< type>, offset -> dst-container<type>, offset
;;Conversion map is a double-lookup of src-type to a map of dst-type to a function
;;that converts src type to dst type.
(def ^:dynamic conversion-table (atom {:array-view
                                       {:java-array
                                        (fn [src-item src-offset]
                                          (let [src-dtype (base/get-datatype src-item)
                                                src-offset (long src-offset)
                                                src-item ^ArrayViewBase src-item
                                                view-offset (.offset src-item)
                                                dst-item (condp = src-dtype
                                                           :byte (.data ^ByteArrayView src-item)
                                                           :short (.data ^ShortArrayView src-item)
                                                           :int (.data ^IntArrayView src-item)
                                                           :long (.data ^LongArrayView src-item)
                                                           :float (.data ^FloatArrayView src-item)
                                                           :double (.data ^DoubleArrayView src-item))]
                                            [dst-item (+ src-offset view-offset)]))}}))


(defn identity-conversion
  [src-item src-offset]
  [src-item src-offset])


(defn add-conversion-fn
  [src-container-type dst-container-type convert-fn]
  (swap! conversion-table
         (fn [convert-map]
           (assoc-in convert-map [:src-container-type :dst-container-type] convert-fn))))


(defn as-byte-buffer
  ^ByteBuffer [obj] obj)

(defn as-short-buffer
  ^ShortBuffer [obj] obj)

(defn as-int-buffer
  ^IntBuffer [obj] obj)

(defn as-long-buffer
  ^LongBuffer [obj] obj)

(defn as-float-buffer
  ^FloatBuffer [obj] obj)

(defn as-double-buffer
  ^DoubleBuffer [obj] obj)

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

(defn as-byte-array-view
  ^ByteArrayView [obj] obj)

(defn as-short-array-view
  ^ShortArrayView [obj] obj)

(defn as-int-array-view
  ^IntArrayView [obj] obj)

(defn as-long-array-view
  ^LongArrayView [obj] obj)

(defn as-float-array-view
  ^FloatArrayView [obj] obj)

(defn as-double-array-view
  ^DoubleArrayView [obj] obj)


(defmacro datatype->array-cast-fn
  [dtype buf]
  (condp = dtype
    :byte `(as-byte-array ~buf)
    :short `(as-short-array ~buf)
    :int `(as-int-array ~buf)
    :long `(as-long-array ~buf)
    :float `(as-float-array ~buf)
    :double `(as-double-array ~buf)))


(defmacro datatype->view-cast-fn
  [dtype buf]
  (condp = dtype
    :byte `(as-byte-array-view ~buf)
    :short `(as-short-array-view ~buf)
    :int `(as-int-array-view ~buf)
    :long `(as-long-array-view ~buf)
    :float `(as-float-array-view ~buf)
    :double `(as-double-array-view ~buf)))


(defmacro datatype->buffer-cast-fn
  [dtype buf]
  (condp = dtype
    :byte `(as-byte-buffer ~buf)
    :short `(as-short-buffer ~buf)
    :int `(as-int-buffer ~buf)
    :long `(as-long-buffer ~buf)
    :float `(as-float-buffer ~buf)
    :double `(as-double-buffer ~buf)))


(defmacro datatype->cast-fn
  [dtype val]
  (condp = dtype
    :byte `(byte ~val)
    :short `(short ~val)
    :int `(int ~val)
    :long `(long ~val)
    :float `(float ~val)
    :double `(double ~val)))

(defmacro create-buffer->array-fn
  "Create a function that assumes the types do not match
and thus needs to cast."
  [buf-cast-fn ary-cast-fn dest-cast-fn]
  `(fn [src# src-offset# dest# dest-offset# n-elems#]
     (let [src# (~buf-cast-fn src#)
           src-offset# (long src-offset#)
           dest# (~ary-cast-fn dest#)
           dest-offset# (long dest-offset#)
           n-elems# (long n-elems#)]
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (aset dest# (+ dest-offset# idx#)
                    (~dest-cast-fn (.get src# (+ src-offset# idx#))))))))


(defmacro create-array->buffer-fn
  [ary-cast-fn buf-cast-fn dest-cast-fn]
  `(fn [src# src-offset# dest# dest-offset# n-elems#]
     (let [src# (~ary-cast-fn src#)
           src-offset# (long src-offset#)
           dest# (~buf-cast-fn dest#)
           dest-offset# (long dest-offset#)
           n-elems# (long n-elems#)]
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (.put dest# (+ dest-offset# idx#)
                    (~dest-cast-fn (aget src# (+ src-offset# idx#))))))))


(defmacro create-array->array-fn
  [src-type-fn dest-type-fn dest-cast-fn]
  `(fn [src# src-offset# dest# dest-offset# n-elems#]
     (let [src# (~src-type-fn src#)
           src-offset# (long src-offset#)
           dest# (~dest-type-fn dest#)
           dest-offset# (long dest-offset#)
           n-elems# (long n-elems#)]
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (aset dest# (+ dest-offset# idx#)
                    (~dest-cast-fn (aget src# (+ src-offset# idx#))))))))



(defmacro create-buffer->buffer-fn
  [src-type-fn dest-type-fn dest-cast-fn]
  `(fn [src# src-offset# dest# dest-offset# n-elems#]
     (let [src# (~src-type-fn src#)
           src-offset# (long src-offset#)
           dest# (~dest-type-fn dest#)
           dest-offset# (long dest-offset#)
           n-elems# (long n-elems#)]
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (.put dest# (+ dest-offset# idx#)
                    (~dest-cast-fn (.get src# (+ src-offset# idx#))))))))


;;Copy is src-container<type>, offset, dst-container<type>, offset, num-elems -> nil
(def ^:dynamic copy-table (atom {}))

(def datatype-pairs
  (->> (for [src-dtype base/datatypes
             dst-dtype base/datatypes]
         [src-dtype dst-dtype])
       vec))

(defn add-copy-operation
  "Add a new copy operation; the operation map must contain all n^2 datatype copy ops."
  [src-container-type dst-container-type copy-operation-map]
  (when-not (= (set (keys copy-operation-map))
               (set datatype-pairs))
    (throw (ex-info "Not all datatype combinations are present in the copy operation map"
                    {:missing (cset/difference (set datatype-pairs)
                                               (set (keys copy-operation-map)))})))
  (swap! copy-table assoc [src-container-type dst-container-type] copy-operation-map))


(defmacro build-core-copy-operations
  []
  {[:java-array :java-array]
   (->> datatype-pairs
        (map (fn [[src-dtype dst-dtype]]
               [[src-dtype dst-dtype]
                `(fn [src# src-offset# dst# dst-offset# n-elems#]
                   (let [src# (datatype->array-cast-fn ~src-dtype src#)
                         dst# (datatype->array-cast-fn ~dst-dtype dst#)
                         src-offset# (long src-offset#)
                         dst-offset# (long dst-offset#)
                         n-elems# (long n-elems#)]
                     (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
                            (aset dst# (+ idx# dst-offset#)
                                  (datatype->cast-fn
                                   ~dst-dtype
                                   (aget src# (+ idx# src-offset#)))))))]))
        (into {}))
   [:java-array :nio-buffer]
   (->> datatype-pairs
        (map (fn [[src-dtype dst-dtype]]
               [[src-dtype dst-dtype]
                `(fn [src# src-offset# dst# dst-offset# n-elems#]
                   (let [src# (datatype->array-cast-fn ~src-dtype src#)
                         dst# (datatype->buffer-cast-fn ~dst-dtype dst#)
                         src-offset# (long src-offset#)
                         dst-offset# (long dst-offset#)
                         n-elems# (long n-elems#)]
                     (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
                            (.put dst# (+ idx# dst-offset#)
                                  (datatype->cast-fn
                                   ~dst-dtype
                                   (aget src# (+ idx# src-offset#)))))))]))
        (into {}))
   [:nio-buffer :java-array]
   (->> datatype-pairs
        (map (fn [[src-dtype dst-dtype]]
               [[src-dtype dst-dtype]
                `(fn [src# src-offset# dst# dst-offset# n-elems#]
                   (let [src# (datatype->buffer-cast-fn ~src-dtype src#)
                         dst# (datatype->array-cast-fn ~dst-dtype dst#)
                         src-offset# (long src-offset#)
                         dst-offset# (long dst-offset#)
                         n-elems# (long n-elems#)]
                     (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
                            (aset dst# (+ idx# dst-offset#)
                                  (datatype->cast-fn
                                   ~dst-dtype
                                   (.get src# (+ idx# src-offset#)))))))]))
        (into {}))
   [:nio-buffer :nio-buffer]
   (->> datatype-pairs
        (map (fn [[src-dtype dst-dtype]]
               [[src-dtype dst-dtype]
                `(fn [src# src-offset# dst# dst-offset# n-elems#]
                   (let [src# (datatype->buffer-cast-fn ~src-dtype src#)
                         dst# (datatype->buffer-cast-fn ~dst-dtype dst#)
                         src-offset# (long src-offset#)
                         dst-offset# (long dst-offset#)
                         n-elems# (long n-elems#)]
                     (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
                            (.put dst# (+ idx# dst-offset#)
                                  (datatype->cast-fn
                                   ~dst-dtype
                                   (.get src# (+ idx# src-offset#)))))))]))
        (into {}))})


(def copy-ops (build-core-copy-operations))


(doseq [[container-pair copy-map] copy-ops]
  (add-copy-operation (first container-pair)
                      (second container-pair)
                      copy-map))


(defn copy!
  [src src-offset dst dst-offset n-elems]
  (let [src-container (container-type src)
        dst-container (container-type dst)
        copy-fn-and-conversions
        (if-let [table-copy-map (get @copy-table [src-container dst-container])]
          [table-copy-map nil nil]
          (let [src-conversions (get @conversion-table src-container)
                dst-conversions (get @conversion-table dst-container)]
            (->> (for [src-conversion (concat [[src-container nil]]
                                              (seq src-conversions))
                       dst-conversion (concat [[dst-container nil]]
                                              (seq dst-conversions))]
                   ;;When the copy table has an entry for the converted types
                   ;;Then use the copy entry along with the conversion
                   (let [[src-conv-cont src-conv] src-conversion
                         [dst-conv-cont dst-conv] dst-conversion]
                     (when-let [table-copy-map (get @copy-table [src-conv-cont dst-conv-cont])]
                       [table-copy-map src-conv dst-conv])))
                 (remove nil?)
                 first)))]

    (if-not copy-fn-and-conversions
      ;;Use slow path if we don't have a good marshalling pathway
      (base/generic-copy! src src-offset dst dst-offset n-elems)
      ;;Else do a constant time conversion and do a fast path copy.
      (let [[table-copy-map src-conv dst-conv] copy-fn-and-conversions
            [src src-offset] (if src-conv
                               (src-conv src src-offset)
                               [src src-offset])
            [dst dst-offset] (if dst-conv
                               (dst-conv dst dst-offset)
                               [dst dst-offset])
            table-fn (get table-copy-map [(base/get-datatype src)
                                          (base/get-datatype dst)])]
        (table-fn src src-offset dst dst-offset n-elems)))))


(extend-type Object
  base/PCopyQuery
  (get-copy-fn [dest dest-offset] #(copy! %1 %2 dest dest-offset %3)))
