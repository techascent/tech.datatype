(ns think.datatype.marshal
  "Namespace to contain the madness that happens when you want to marshal
  an (nio buffer or array) or one type to a (nio buffer or array) or another type."
  (:require [clojure.core.matrix.macros :refer [c-for]])
  (:import [java.nio ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer Buffer]
           [think.datatype DoubleArrayView FloatArrayView
            LongArrayView IntArrayView ShortArrayView ByteArrayView
            ArrayView]))

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


(defprotocol PIndexedCopyToArray
  (indexed-copy-to-byte-array [src src-offset src-indexes dest dest-offset dest-indexes])
  (indexed-copy-to-short-array [src src-offset src-indexes dest dest-offset dest-indexes])
  (indexed-copy-to-int-array [src src-offset src-indexes dest dest-offset dest-indexes])
  (indexed-copy-to-long-array [src src-offset src-indexes dest dest-offset dest-indexes])
  (indexed-copy-to-float-array [src src-offset src-indexes dest dest-offset dest-indexes])
  (indexed-copy-to-double-array [src src-offset src-indexes dest dest-offset dest-indexes]))


(defprotocol PCopyToBuffer
  (copy-to-byte-buffer [src src-offset dest dest-offset n-elems])
  (copy-to-short-buffer [src src-offset dest dest-offset n-elems])
  (copy-to-int-buffer [src src-offset dest dest-offset n-elems])
  (copy-to-long-buffer [src src-offset dest dest-offset n-elems])
  (copy-to-float-buffer [src src-offset dest dest-offset n-elems])
  (copy-to-double-buffer [src src-offset dest dest-offset n-elems]))


(defprotocol PIndexedCopyToBuffer
  "Indexed copy.  All indexes must be int arrays."
  (indexed-copy-to-byte-buffer [src src-offset src-indexes dest dest-offset dest-indexes])
  (indexed-copy-to-short-buffer [src src-offset src-indexes dest dest-offset dest-indexes])
  (indexed-copy-to-int-buffer [src src-offset src-indexes dest dest-offset dest-indexes])
  (indexed-copy-to-long-buffer [src src-offset src-indexes dest dest-offset dest-indexes])
  (indexed-copy-to-float-buffer [src src-offset src-indexes dest dest-offset dest-indexes])
  (indexed-copy-to-double-buffer [src src-offset src-indexes dest dest-offset dest-indexes]))


(defprotocol PTypeToCopyToFn
  "Given two arguments, [dest dest-offset] return a function taking signature
[src src-offset] that efficiently implements the copy operation."
  (get-copy-to-fn [dest dest-offset]))


(defprotocol PIndexedTypeToCopyToFn
    "Given two arguments, [dest dest-offset] return a function taking signature
[src src-offset ^ints src-indexes ^ints dest-indexes]
that efficiently implements the copy operation or indicates with a further protocol
which operation to use."
  (get-indexed-copy-to-fn [dest dest-offset]))


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


(defmacro indexed-buffer-type-iterator
  [inner-macro & args]
  `[(~inner-macro ByteBuffer as-byte-buffer 'indexed-copy-to-byte-buffer byte ~@args)
    (~inner-macro ShortBuffer as-short-buffer 'indexed-copy-to-short-buffer short ~@args)
    (~inner-macro IntBuffer as-int-buffer 'indexed-copy-to-int-buffer int ~@args)
    (~inner-macro LongBuffer as-long-buffer 'indexed-copy-to-long-buffer long ~@args)
    (~inner-macro FloatBuffer as-float-buffer 'indexed-copy-to-float-buffer float ~@args)
    (~inner-macro DoubleBuffer as-double-buffer 'indexed-copy-to-double-buffer double ~@args)])


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


(defmacro indexed-array-type-iterator
  [inner-macro & args]
  `[(~inner-macro (Class/forName "[B") as-byte-array 'indexed-copy-to-byte-array byte ~@args)
    (~inner-macro (Class/forName "[S") as-short-array 'indexed-copy-to-short-array short ~@args)
    (~inner-macro (Class/forName "[I") as-int-array 'indexed-copy-to-int-array int ~@args)
    (~inner-macro (Class/forName "[J") as-long-array 'indexed-copy-to-long-array long ~@args)
    (~inner-macro (Class/forName "[F") as-float-array 'indexed-copy-to-float-array float ~@args)
    (~inner-macro (Class/forName "[D") as-double-array 'indexed-copy-to-double-array double ~@args)])


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


(defmacro buffer->array-copy-iter
  [dest-type dest-type-fn copy-to-dest-fn dest-cast-fn src-cast-fn]
  `[(keyword (name ~copy-to-dest-fn)) (create-buffer->array-fn ~src-cast-fn ~dest-type-fn ~dest-cast-fn)])


(defmacro buffer->buffer-copy-iter
  [dest-type dest-type-fn copy-to-dest-fn dest-cast-fn src-cast-fn]
  `[(keyword (name ~copy-to-dest-fn)) (create-buffer->buffer-fn ~src-cast-fn ~dest-type-fn ~dest-cast-fn)])


(defmacro create-indexed-buffer->array-fn
  "Create a function that assumes the types do not match
and thus needs to cast."
  [buf-cast-fn ary-cast-fn dest-cast-fn]
  `(fn [src# src-offset# src-indexes# dest# dest-offset# dest-indexes#]
     (let [src# (~buf-cast-fn src#)
           src-indexes# (as-int-array src-indexes#)
           dest# (~ary-cast-fn dest#)
           dest-indexes# (as-int-array dest-indexes#)
           n-elems# (alength src-indexes#)
           src-offset# (long src-offset#)
           dest-offset# (long dest-offset#)]
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (aset dest# (+ dest-offset# (aget dest-indexes# idx#))
                    (~dest-cast-fn (.get src# (+ src-offset#
                                                 (aget src-indexes# idx#)))))))))


(defmacro create-indexed-array->buffer-fn
  [ary-cast-fn buf-cast-fn dest-cast-fn]
  `(fn [src# src-offset# src-indexes# dest# dest-offset# dest-indexes#]
     (let [src# (~ary-cast-fn src#)
           src-indexes# (as-int-array src-indexes#)
           dest# (~buf-cast-fn dest#)
           dest-indexes# (as-int-array dest-indexes#)
           n-elems# (alength src-indexes#)
           src-offset# (long src-offset#)
           dest-offset# (long dest-offset#)]
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (.put dest# (+ dest-offset# (aget dest-indexes# idx#))
                    (~dest-cast-fn (aget src# (+ src-offset#
                                                 (aget src-indexes# idx#)))))))))


(defmacro create-indexed-array->array-fn
  [src-type-fn dest-type-fn dest-cast-fn]
  `(fn [src# src-offset# src-indexes# dest# dest-offset# dest-indexes#]
     (let [src# (~src-type-fn src#)
           src-indexes# (as-int-array src-indexes#)
           dest# (~dest-type-fn dest#)
           dest-indexes# (as-int-array dest-indexes#)
           n-elems# (alength src-indexes#)
           src-offset# (long src-offset#)
           dest-offset# (long dest-offset#)]
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (aset dest# (+ dest-offset# (aget dest-indexes# idx#))
                    (~dest-cast-fn (aget src# (+ src-offset#
                                                 (aget src-indexes# idx#)))))))))


(defmacro create-indexed-buffer->buffer-fn
  [src-type-fn dest-type-fn dest-cast-fn]
  `(fn [src# src-offset# src-indexes# dest# dest-offset# dest-indexes#]
     (let [src# (~src-type-fn src#)
           src-indexes# (as-int-array src-indexes#)
           dest# (~dest-type-fn dest#)
           dest-indexes# (as-int-array dest-indexes#)
           n-elems# (alength src-indexes#)
           src-offset# (long src-offset#)
           dest-offset# (long dest-offset#)]
       (c-for [idx# 0 (< idx# n-elems#) (inc idx#)]
              (.put dest# (+ dest-offset# (aget dest-indexes# idx#))
                    (~dest-cast-fn (.get src# (+ src-offset#
                                                 (aget src-indexes# idx#)))))))))


(defmacro indexed-array->array-copy-iter
  [dest-type dest-type-fn copy-to-dest-fn dest-cast-fn src-cast-fn]
  `[(keyword (name ~copy-to-dest-fn)) (create-indexed-array->array-fn ~src-cast-fn
                                                                      ~dest-type-fn
                                                                      ~dest-cast-fn)])


(defmacro indexed-array->buffer-copy-iter
  [dest-type dest-type-fn copy-to-dest-fn dest-cast-fn src-cast-fn]
  `[(keyword (name ~copy-to-dest-fn)) (create-indexed-array->buffer-fn ~src-cast-fn
                                                                       ~dest-type-fn
                                                                       ~dest-cast-fn)])


(defmacro indexed-buffer->array-copy-iter
  [dest-type dest-type-fn copy-to-dest-fn dest-cast-fn src-cast-fn]
  `[(keyword (name ~copy-to-dest-fn)) (create-indexed-buffer->array-fn ~src-cast-fn
                                                                       ~dest-type-fn
                                                                       ~dest-cast-fn)])


(defmacro indexed-buffer->buffer-copy-iter
  [dest-type dest-type-fn copy-to-dest-fn dest-cast-fn src-cast-fn]
  `[(keyword (name ~copy-to-dest-fn)) (create-indexed-buffer->buffer-fn ~src-cast-fn
                                                                        ~dest-type-fn
                                                                        ~dest-cast-fn)])


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

(defmacro indexed-array-marshal-impl
  [ary-type cast-type-fn copy-to-fn cast-fn]
  `(extend ~ary-type
     PIndexedTypeToCopyToFn
     {:get-indexed-copy-to-fn (fn [dest# dest-offset#] #(~(eval copy-to-fn)
                                                         %1 %2 %3
                                                         dest# dest-offset# %4))}
     PIndexedCopyToArray
     (->> (indexed-array-type-iterator indexed-array->array-copy-iter ~cast-type-fn)
          (into {}))
     PIndexedCopyToBuffer
     (->> (indexed-buffer-type-iterator indexed-array->buffer-copy-iter ~cast-type-fn)
          (into {}))))


(defmacro indexed-buffer-marshal-impl
  [buf-type cast-type-fn copy-to-fn cast-fn]
  `(extend ~buf-type
     PIndexedTypeToCopyToFn
     {:get-indexed-copy-to-fn (fn [dest# dest-offset#]
                                #(~(eval copy-to-fn)
                                  %1 %2 %3
                                  dest# dest-offset# %4))}
     PIndexedCopyToArray
     (->> (indexed-array-type-iterator indexed-buffer->array-copy-iter ~cast-type-fn)
          (into {}))
     PIndexedCopyToBuffer
     (->> (indexed-buffer-type-iterator indexed-buffer->buffer-copy-iter ~cast-type-fn)
          (into {}))))


(def array-bindings
  (array-type-iterator array-marshal-impl))


(def array-indexed-bindings
  (indexed-array-type-iterator indexed-array-marshal-impl))


(def buffer-bindings
  (buffer-type-iterator buffer-marshal-impl))


(def buffer-indexed-bindings
  (indexed-buffer-type-iterator indexed-buffer-marshal-impl))


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


(defprotocol ArrayViewToArray
  (view->array [view])
  (view->array-offset [view offset]))


(defmacro array-view-iterator
  [inner-macro & args]
  `[(~inner-macro ByteArrayView as-byte-array-view 'copy-to-byte-array byte ~@args)
    (~inner-macro ShortArrayView as-short-array-view 'copy-to-short-array short ~@args)
    (~inner-macro IntArrayView as-int-array-view 'copy-to-int-array int ~@args)
    (~inner-macro LongArrayView as-long-array-view 'copy-to-long-array long ~@args)
    (~inner-macro FloatArrayView as-float-array-view 'copy-to-float-array float ~@args)
    (~inner-macro DoubleArrayView as-double-array-view 'copy-to-double-array double ~@args)])


(defmacro view->copy-impl
  [array-type cast-type-fn copy-to-fn cast-fn]
  `[(keyword (name ~copy-to-fn)) (fn [src# src-offset# dest# dest-offset# n-elems#]
                                   (~(eval copy-to-fn)
                                    (view->array src#)
                                    (view->array-offset src# src-offset#)
                                    dest# dest-offset# n-elems#))])


(defmacro indexed-array-view-iterator
  [inner-macro & args]
  `[(~inner-macro ByteArrayView as-byte-array-view 'indexed-copy-to-byte-array byte ~@args)
    (~inner-macro ShortArrayView as-short-array-view 'indexed-copy-to-short-array short ~@args)
    (~inner-macro IntArrayView as-int-array-view 'indexed-copy-to-int-array int ~@args)
    (~inner-macro LongArrayView as-long-array-view 'indexed-copy-to-long-array long ~@args)
    (~inner-macro FloatArrayView as-float-array-view 'indexed-copy-to-float-array float ~@args)
    (~inner-macro DoubleArrayView as-double-array-view 'indexed-copy-to-double-array double ~@args)])


(defn offset-indexes
  ^ints [^ints indexes, ^long offset]
  (if (= 0 offset)
    indexes
    (let [idx-len (alength indexes)
          retval (int-array idx-len)]
      (c-for [idx 0 (< idx idx-len) (inc idx)]
             (aset retval idx (int (+ (aget indexes idx) offset)))))))


(defmacro view->indexed-copy-impl
  [array-type cast-type-fn copy-to-fn cast-fn]
  `[(keyword (name ~copy-to-fn)) (fn [src# src-offset# src-indexes# dest# dest-offset# dest-indexes#]
                                   (~(eval copy-to-fn)
                                    (view->array src#)
                                    (view->array-offset src# src-offset#)
                                    src-indexes#
                                    dest#
                                    dest-offset#
                                    dest-indexes#))])


(defmacro array-view-marshal-impl
  [view-type cast-type-fn copy-to-fn cast-fn]
  `(extend ~view-type
     ArrayViewToArray
     {:view->array (fn [view#] (.data (~cast-type-fn view#)))
      :view->array-offset (fn [view# offset#] (.index (~cast-type-fn view#) (long offset#)))}
     PTypeToCopyToFn
     {:get-copy-to-fn (fn [dest# offset#]
                        #(~(eval copy-to-fn) %1 %2
                          (view->array dest#)
                          (view->array-offset dest# offset#) %3))}
     PCopyToArray
     (->> (array-type-iterator view->copy-impl)
          (into {}))
     PCopyToBuffer
     (->> (buffer-type-iterator view->copy-impl)
          (into {}))))


(defmacro indexed-array-view-marshal-impl
  [view-type cast-type-fn copy-to-fn cast-fn]
  `(extend ~view-type
     PIndexedTypeToCopyToFn
     {:get-indexed-copy-to-fn (fn [dest# dest-offset#]
                                (fn [src# src-offset# src-indexes# dest-indexes#]
                                  (~(eval copy-to-fn) src# src-offset# src-indexes#
                                   (view->array dest#)
                                   (view->array-offset dest# dest-offset#)
                                   dest-indexes#)))}
     PIndexedCopyToArray
     (->> (indexed-array-type-iterator view->indexed-copy-impl)
          (into {}))
     PIndexedCopyToBuffer
     (->> (indexed-buffer-type-iterator view->indexed-copy-impl)
          (into {}))))


(def array-view-bindings
  (array-view-iterator array-view-marshal-impl))


(def indexed-array-view-bindings
  (indexed-array-view-iterator indexed-array-view-marshal-impl))


(defn marshal-copy-to
  [src src-offset dest dest-offset n-elems]
  ((get-copy-to-fn dest dest-offset) src src-offset n-elems))
