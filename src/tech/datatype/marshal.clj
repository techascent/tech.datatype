(ns tech.datatype.marshal
  "Namespace to contain the madness that happens when you want to marshal
  an (nio buffer or array) or one type to a (nio buffer or array) or another type."
  (:require [clojure.core.matrix.macros :refer [c-for]]
            [tech.datatype.base :as base]
            [clojure.set :as cset])
  (:import [java.nio ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer Buffer]))

;;Some utility items to make the macros easier.
(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defprotocol PContainerType
  (container-type [item]))


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

(extend-type Object
  PContainerType
  (container-type [item] :object))



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

(defmacro datatype->array-cast-fn
  [dtype buf]
  (condp = dtype
    :int8 `(as-byte-array ~buf)
    :int16 `(as-short-array ~buf)
    :int32 `(as-int-array ~buf)
    :int64 `(as-long-array ~buf)
    :float32 `(as-float-array ~buf)
    :float64 `(as-double-array ~buf)))



(defmacro datatype->buffer-cast-fn
  [dtype buf]
  (condp = dtype
    :int8 `(as-byte-buffer ~buf)
    :int16 `(as-short-buffer ~buf)
    :int32 `(as-int-buffer ~buf)
    :int64 `(as-long-buffer ~buf)
    :float32 `(as-float-buffer ~buf)
    :float64 `(as-double-buffer ~buf)))


(defmacro datatype->unchecked-cast-fn
  [dtype val]
  (condp = dtype
    :int8 `(unchecked-byte ~val)
    :int16 `(unchecked-short ~val)
    :int32 `(unchecked-int ~val)
    :int64 `(unchecked-long ~val)
    :float32 `(unchecked-float ~val)
    :float64 `(unchecked-double ~val)))


(defmacro datatype->cast-fn
  [dtype val]
  (condp = dtype
    :int8 `(byte ~val)
    :int16 `(short ~val)
    :int32 `(int ~val)
    :int64 `(long ~val)
    :float32 `(float ~val)
    :float64 `(double ~val)))


(def datatype-pairs
  (->> (for [src-dtype base/datatypes
             dst-dtype base/datatypes]
         [src-dtype dst-dtype])
       vec))


(defmacro array-array-copy
  [src-dtype dst-dtype src src-offset dst dst-offset n-elems options]
  `(let [src# (datatype->array-cast-fn ~src-dtype ~src)
         dst# (datatype->array-cast-fn ~dst-dtype ~dst)
         src-offset# (long ~src-offset)
         dst-offset# (long ~dst-offset)
         n-elems# (long ~n-elems)
         unchecked?# (get ~options :unchecked?)]
     (if unchecked?#
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (aset dst# (+ idx# dst-offset#)
                    (datatype->unchecked-cast-fn
                     ~dst-dtype
                     (aget src# (+ idx# src-offset#)))))
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (aset dst# (+ idx# dst-offset#)
                    (datatype->cast-fn
                     ~dst-dtype
                     (aget src# (+ idx# src-offset#))))))))


(defmacro array-buffer-copy
  [src-dtype dst-dtype src src-offset dst dst-offset n-elems options]
  `(let [src# (datatype->array-cast-fn ~src-dtype ~src)
         dst# (datatype->buffer-cast-fn ~dst-dtype ~dst)
         src-offset# (long ~src-offset)
         dst-offset# (long ~dst-offset)
         n-elems# (long ~n-elems)
         unchecked?# (get ~options :unchecked?)]
     (if unchecked?#
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (.put dst# (+ idx# dst-offset#)
                    (datatype->unchecked-cast-fn
                     ~dst-dtype
                     (aget src# (+ idx# src-offset#)))))
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (.put dst# (+ idx# dst-offset#)
                    (datatype->cast-fn
                     ~dst-dtype
                     (aget src# (+ idx# src-offset#))))))))


(defmacro buffer-array-copy
  [src-dtype dst-dtype src src-offset dst dst-offset n-elems options]
  `(let [src# (datatype->buffer-cast-fn ~src-dtype ~src)
         dst# (datatype->array-cast-fn ~dst-dtype ~dst)
         src-offset# (long ~src-offset)
         dst-offset# (long ~dst-offset)
         n-elems# (long ~n-elems)
         unchecked?# (get ~options :unchecked?)]
     (if unchecked?#
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (aset dst# (+ idx# dst-offset#)
                    (datatype->unchecked-cast-fn
                     ~dst-dtype
                     (.get src# (+ idx# src-offset#)))))
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (aset dst# (+ idx# dst-offset#)
                    (datatype->cast-fn
                     ~dst-dtype
                     (.get src# (+ idx# src-offset#))))))))


(defmacro buffer-buffer-copy
  [src-dtype dst-dtype src src-offset dst dst-offset n-elems options]
  `(let [src# (datatype->buffer-cast-fn ~src-dtype ~src)
         dst# (datatype->buffer-cast-fn ~dst-dtype ~dst)
         src-offset# (long ~src-offset)
         dst-offset# (long ~dst-offset)
         n-elems# (long ~n-elems)
         unchecked?# (get ~options :unchecked?)]
     (if unchecked?#
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (.put dst# (+ idx# dst-offset#)
                    (datatype->unchecked-cast-fn
                     ~dst-dtype
                     (.get src# (+ idx# src-offset#)))))
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (.put dst# (+ idx# dst-offset#)
                    (datatype->cast-fn
                     ~dst-dtype
                     (.get src# (+ idx# src-offset#))))))))


(defmacro generate-core-copy-dtype-dst-dispatch
  [copy-macro src-datatype src src-offset dst dst-offset n-elems options]
  `(case (base/get-datatype ~dst)
     ~@(->> (for [dst-datatype base/datatypes]
              `(~dst-datatype (~copy-macro ~src-datatype ~dst-datatype
                               ~src ~src-offset ~dst ~dst-offset ~n-elems ~options)))
            (apply concat))))


(defmacro generate-core-copy-dtype-dispatch
  [copy-macro src src-offset dst dst-offset n-elems options]
  `(case (base/get-datatype ~src)
     ~@(->> (for [src-datatype base/datatypes]
              `(~src-datatype (generate-core-copy-dtype-dst-dispatch
                               ~copy-macro ~src-datatype
                               ~src ~src-offset ~dst ~dst-offset
                               ~n-elems ~options)))
            (apply concat))))


(defmacro generate-core-copy-op
  [src src-offset dst dst-offset n-elems options]
  `(case (container-type ~src)
     :java-array
     (case (container-type ~dst)
       :java-array (generate-core-copy-dtype-dispatch array-array-copy
                                                      ~src ~src-offset
                                                      ~dst ~dst-offset ~n-elems
                                                      ~options)
       :nio-buffer (generate-core-copy-dtype-dispatch array-buffer-copy
                                                      ~src ~src-offset
                                                      ~dst ~dst-offset ~n-elems
                                                      ~options))
     :nio-buffer
     (case (container-type ~dst)
       :java-array (generate-core-copy-dtype-dispatch buffer-array-copy
                                                      ~src ~src-offset
                                                      ~dst ~dst-offset ~n-elems
                                                      ~options)
       :nio-buffer (generate-core-copy-dtype-dispatch buffer-buffer-copy
                                                      ~src ~src-offset
                                                      ~dst ~dst-offset ~n-elems
                                                      ~options))))


(defn core-copy-operation
  [src src-offset dst dst-offset n-elems options]
  (generate-core-copy-op src src-offset dst dst-offset n-elems options))


;;Container Conversion Table
;;{src-container-type {dst-container-type (fn [src dst-container-type] [dst dst-offset])
;;}}
;;Conversion map is a double-lookup of src-type to a map of dst-type to a function
;;that converts src type to dst type.
(defonce ^:dynamic *container-conversion-table*
  (atom {}))


(defn add-container-conversion-fn
  [src-container-type dst-container-type convert-fn]
  (swap! *container-conversion-table*
         (fn [convert-map]
           (assoc-in convert-map [src-container-type dst-container-type] convert-fn))))


;;Copy is src-container<type>, offset, dst-container<type>, offset, num-elems -> nil
(defonce ^:dynamic *copy-table* (atom {}))


(defn add-copy-operation
  "Add a new copy operation; the operation map must contain all n^2 datatype copy ops."
  [src-container-type dst-container-type src-dtype dst-dtype copy-fn]
  (swap! *copy-table* assoc [src-container-type dst-container-type src-dtype dst-dtype]
         copy-fn))


;;Setup base datatypes and copy operations

(->> (for [src-container [:nio-buffer :java-array]
           dst-container [:nio-buffer :java-array]
           src-dtype base/datatypes
           dst-dtype base/datatypes]
       (add-copy-operation src-container dst-container src-dtype dst-dtype
                           core-copy-operation))
     dorun)


(defn- find-copy-fn
  [src-container dst-container src-dtype dst-dtype]
  (let [cache-fn-key [src-container
                      dst-container
                      src-dtype
                      dst-dtype]]
    (if-let [cache-fn (get @*copy-table* cache-fn-key)]
      cache-fn
      (let [copy-table @*copy-table*
            conversion-table @*conversion-table*
            cache-copy-fn (fn [copy-fn]
                            (swap! *copy-table*
                                   assoc cache-fn-key copy-fn)
                            copy-fn)
            src-conversions (get conversion-table src-container)
            dst-conversions (get conversion-table dst-container)
            ;;This should be a combination of dijkstras shortest path
            ;;to something in the table copy map for both src and dst.
            ;;It is not at this time.
            table-data
            (->> (for [src-conversion (concat [[src-container nil]]
                                              (seq src-conversions))
                       dst-conversion (concat [[dst-container nil]]
                                              (seq dst-conversions))]
                   ;;When the copy table has an entry for the converted types
                   ;;Then use the copy entry along with the conversion
                   (let [[src-conv-cont src-conv] src-conversion
                         [dst-conv-cont dst-conv] dst-conversion]
                     (when-let [table-copy-map (get copy-table
                                                    [src-conv-cont dst-conv-cont])]
                       [(get table-copy-map [src-dtype dst-dtype])
                        (partial src-conv src-conv-cont)
                        (partial dst-conv dst-conv-cont)])))
                 (remove nil?)
                 first)]
        (if table-data
          (let [[table-copy-fn src-conv dst-conv] table-data]
            (cache-copy-fn
             (fn [src src-offset dst dst-offset n-elems options]
               (let [[src src-conv-offset] (if src-conv
                                             (src-conv src)
                                             [src 0])
                     [dst dst-conv-offset] (if dst-conv
                                             (dst-conv dst)
                                             [dst 0])]
                 (table-copy-fn src (+ (long src-offset) (long src-conv-offset))
                                dst (+ (long dst-offset) (long dst-conv-offset))
                                n-elems)))))
          base/generic-copy!)))))


(defn copy!
  [src src-offset dst dst-offset n-elems options]
  (let [copy-fn (find-copy-fn (container-type src) (container-type dst)
                              (base/get-datatype src) (base/get-datatype dst))]
    (copy-fn src src-offset dst dst-offset n-elems options)
    dst))


(extend-type Object
  base/PCopyQuery
  (get-copy-fn [dest dest-offset] #(copy! %1 %2 dest dest-offset %3 %4)))
