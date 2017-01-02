(ns think.datatype.marshal
  "Namespace to contain the madness that happens when you want to marshal
  an (nio buffer or array) or one type to a (nio buffer or array) or another type."
  (:require [clojure.core.matrix.macros :refer [c-for]])
  (:import [java.nio ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer Buffer]))

;;Some utility items to make the macros easier.
(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defprotocol PCopyToArray
  (copy-to-byte-array [src src-offset dest dest-offset n-elems])
  (copy-to-short-array [src src-offset dest dest-offset n-elems])
  (copy-to-int-array [src src-offset dest dest-offset n-elems])
  (copy-to-long-array [src src-offset dest dest-offset n-elems])
  (copy-to-float-array [src src-offset dest dest-offset n-elems])
  (copy-to-double-array [src src-offset dest dest-offset n-elems]))


(defprotocol PCopyToBuffer
  (copy-to-byte-buffer [src src-offset dest dest-offset n-elems])
  (copy-to-short-buffer [src src-offset dest dest-offset n-elems])
  (copy-to-int-buffer [src src-offset dest dest-offset n-elems])
  (copy-to-long-buffer [src src-offset dest dest-offset n-elems])
  (copy-to-float-buffer [src src-offset dest dest-offset n-elems])
  (copy-to-double-buffer [src src-offset dest dest-offset n-elems]))


(defprotocol PTypeToCopyToFn
  (get-copy-to-fn [dest offset]))


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


(defmacro buffer-type-iterator
  [inner-macro & args]
  `[(~inner-macro ByteBuffer as-byte-buffer 'copy-to-byte-buffer byte ~@args)
    (~inner-macro ShortBuffer as-short-buffer 'copy-to-short-buffer short ~@args)
    (~inner-macro IntBuffer as-int-buffer 'copy-to-int-buffer int ~@args)
    (~inner-macro LongBuffer as-long-buffer 'copy-to-long-buffer long ~@args)
    (~inner-macro FloatBuffer as-float-buffer 'copy-to-float-buffer float ~@args)
    (~inner-macro DoubleBuffer as-double-buffer 'copy-to-double-buffer double ~@args)])


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


(defmacro array-type-iterator
  [inner-macro & args]
  `[(~inner-macro (Class/forName "[B") as-byte-array 'copy-to-byte-array byte ~@args)
    (~inner-macro (Class/forName "[S") as-short-array 'copy-to-short-array short ~@args)
    (~inner-macro (Class/forName "[I") as-int-array 'copy-to-int-array int ~@args)
    (~inner-macro (Class/forName "[J") as-long-array 'copy-to-long-array long ~@args)
    (~inner-macro (Class/forName "[F") as-float-array 'copy-to-float-array float ~@args)
    (~inner-macro (Class/forName "[D") as-double-array 'copy-to-double-array double ~@args)])


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


(defmacro array->array-copy-iter
  [dest-type dest-type-fn copy-to-dest-fn dest-cast-fn src-cast-fn]
  `[(keyword (name ~copy-to-dest-fn)) (create-array->array-fn ~src-cast-fn ~dest-type-fn ~dest-cast-fn)])


(defmacro array->buffer-copy-iter
  [dest-type dest-type-fn copy-to-dest-fn dest-cast-fn src-cast-fn]
  `[(keyword (name ~copy-to-dest-fn)) (create-array->buffer-fn ~src-cast-fn ~dest-type-fn ~dest-cast-fn)])


(defmacro array-marshal-impl
  [ary-type cast-type-fn copy-to-fn cast-fn]
  `(extend ~ary-type
     PTypeToCopyToFn
     {:get-copy-to-fn (fn [dest# offset#] #(~(eval copy-to-fn) %1 %2 dest# offset# %3))}
     PCopyToArray
     (->> (array-type-iterator array->array-copy-iter ~cast-type-fn)
          (into {}))
     PCopyToBuffer
     (->> (buffer-type-iterator array->buffer-copy-iter ~cast-type-fn)
          (into {}))))


(defmacro buffer->array-copy-iter
  [dest-type dest-type-fn copy-to-dest-fn dest-cast-fn src-cast-fn]
  `[(keyword (name ~copy-to-dest-fn)) (create-buffer->array-fn ~src-cast-fn ~dest-type-fn ~dest-cast-fn)])


(defmacro buffer->buffer-copy-iter
  [dest-type dest-type-fn copy-to-dest-fn dest-cast-fn src-cast-fn]
  `[(keyword (name ~copy-to-dest-fn)) (create-buffer->buffer-fn ~src-cast-fn ~dest-type-fn ~dest-cast-fn)])


(defmacro buffer-marshal-impl
  [buf-type cast-type-fn copy-to-fn cast-fn]
  `(extend ~buf-type
     PTypeToCopyToFn
     {:get-copy-to-fn (fn [dest# offset#] #(~(eval copy-to-fn) %1 %2 dest# offset# %3))}
     PCopyToArray
     (->> (array-type-iterator buffer->array-copy-iter ~cast-type-fn)
          (into {}))
     PCopyToBuffer
     (->> (buffer-type-iterator buffer->buffer-copy-iter ~cast-type-fn)
          (into {}))))


(def array-bindings
  (array-type-iterator array-marshal-impl))


(def buffer-bindings
  (buffer-type-iterator buffer-marshal-impl))


(defn marshal-copy-to
  [src src-offset dest dest-offset n-elems]
  ((get-copy-to-fn dest dest-offset) src src-offset n-elems))
