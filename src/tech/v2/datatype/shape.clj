(ns tech.v2.datatype.shape
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typecast :as typecast])
  (:import [java.util List]
           [java.nio Buffer
            ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer]))


(set! *warn-on-reflection* true)


(defn scalar?
  [item]
  (or (number? item)
      (string? item)
      (and
       (not (when (instance? Class (type item))
              (.isArray ^Class (type item))))
       (not (dtype-proto/convertible-to-iterable? item)))))


(declare shape)


(extend-type Object
  dtype-proto/PCountable
  (ecount [item]
    (cond
      (scalar? item)
      0
      (and (instance? Class (type item))
           (.isArray ^Class (type item)))
      (apply * (dtype-proto/shape item))
      :else
      (count item)))
  dtype-proto/PShape
  (shape [item]
    (cond
      (scalar? item)
      nil
      (and (instance? Class (type item))
           (.isArray ^Class (type item)))
      (let [n-elems (count item)]
        (-> (concat [n-elems]
                    (when (> n-elems 0)
                      (let [first-elem (first item)]
                        (shape first-elem))))
            vec))
      :else
      [(dtype-proto/ecount item)])))


(extend-type List
  dtype-proto/PShape
  (shape [item]
    (if (seqable? (first item))
      (->> (concat [(.size item)]
                   (dtype-proto/shape (first item)))
           vec)
      [(.size item)])))


(extend-type Buffer
  dtype-proto/PCountable
  (ecount [item]
    (.remaining item)))



(def array-class-names
  {"[B" :int8
   "[S" :int16
   "[I" :int32
   "[J" :int64
   "[F" :float32
   "[D" :float64
   "[Z" :boolean})


(defmacro extend-ary-cls
  [ary-name ary-dtype]
  `(clojure.core/extend
       (Class/forName ~ary-name)
     dtype-proto/PDatatype
     {:get-datatype (constantly ~ary-dtype)}
     dtype-proto/PCountable
     {:ecount (fn [item#] (alength (typecast/datatype->array-cast-fn ~ary-dtype item#)))}))


(defmacro extend-all-array-classes
  []
  `(do
     ~@(->> array-class-names
            (map (fn [[ary-name ary-dtype]]
                   `(extend-ary-cls ~ary-name ~ary-dtype))))))


(extend-all-array-classes)


(defmacro extend-buffer
  [datatype]
  `(clojure.core/extend
       (resolve (typecast/datatype->buffer-type ~datatype))
     dtype-proto/PDatatype
     {:get-datatype (constantly ~datatype)}
     dtype-proto/PCountable
     {:ecount (fn [buffer#] (.remaining (typecast/datatype->buffer-cast-fn ~datatype buffer#)))}))


(def buffer-extend-map  (typecast/extend-host-numeric-datatypes extend-buffer))


(defn ecount
  "Type hinted element count"
  ^long [item]
  (if-not item
    0
    (long (dtype-proto/ecount item))))


(defn shape
  [item]
  (if-not item
    nil
    (dtype-proto/shape item)))
