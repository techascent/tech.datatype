(ns tech.v2.datatype.nio-access
  (:require [tech.v2.datatype.casting :as casting]
            [primitive-math :as pmath])
  (:import [java.nio ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer]
           [it.unimi.dsi.fastutil.bytes ByteList ByteArrayList]
           [it.unimi.dsi.fastutil.shorts ShortList ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntList IntArrayList]
           [it.unimi.dsi.fastutil.longs LongList LongArrayList]
           [it.unimi.dsi.fastutil.floats FloatList FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleList DoubleArrayList]
           [it.unimi.dsi.fastutil.booleans BooleanList BooleanArrayList]
           [it.unimi.dsi.fastutil.objects ObjectList ObjectArrayList]))


(defmacro buf-put
  [buffer idx buf-pos value]
  (if (= 0 buf-pos)
    `(.put ~buffer ~idx ~value)
    `(.put ~buffer (pmath/+ ~idx ~buf-pos) ~value)))


(defmacro buf-get
  [buffer idx buf-pos]
  (if (= 0 buf-pos)
    `(.get ~buffer ~idx ~buf-pos)
    `(.get ~buffer (pmath/+ ~idx ~buf-pos))))


(defmacro datatype->pos-fn
  [datatype buffer]
  (if (casting/numeric-type? datatype)
    `(.position ~buffer)
    `0))


(defmacro datatype->read-fn
  [datatype buffer idx pos]
  (case datatype
    :boolean `(.getBoolean ~buffer ~idx)
    :object `(.get ~buffer ~idx)
    `(buf-get ~buffer ~idx ~pos)))

(defmacro datatype->write-fn
  [datatype buffer idx pos value]
  (case datatype
    :boolean `(.set ~buffer ~idx ~value)
    :object `(.set ~buffer ~idx ~value)
    `(buf-put ~buffer ~idx ~pos ~value)))


(defmacro datatype->list-read-fn
  [datatype buffer idx]
  (case datatype
    :int8 `(.getByte ~buffer ~idx)
    :int16 `(.getShort ~buffer ~idx)
    :int32 `(.getInt ~buffer ~idx)
    :int64 `(.getLong ~buffer ~idx)
    :float32 `(.getFloat ~buffer ~idx)
    :float64 `(.getDouble ~buffer ~idx)
    :boolean `(.getBoolean ~buffer ~idx)
    :object `(.get ~buffer ~idx)))


(def ary-type-set (->> ["[B" "[S" "[I" "[J" "[F" "[D" "[C" "[Z"
                        "[Ljava.lang.Object;"]
                       (map #(Class/forName %))
                       set))


(defn array-type?
  [item-type]
  (or (ary-type-set item-type)
      (and (instance? Class item-type)
           (.isArray ^Class item-type))))



(def nio-type-set (set [ByteBuffer ShortBuffer IntBuffer LongBuffer
                        FloatBuffer DoubleBuffer]))


(defn nio-type?
  [cls-type]
  (boolean (nio-type-set cls-type)))


(def list-type-set (set [ByteList ShortList IntList LongList FloatList DoubleList BooleanList ObjectList
                         ByteArrayList ShortArrayList IntArrayList LongArrayList FloatArrayList DoubleArrayList
                         BooleanArrayList ObjectArrayList]))

(defn list-type?
  [cls-type]
  (boolean (list-type-set cls-type)))


(defmacro cls-type->read-fn
  [cls-type item-datatype item idx pos]
  (let [cls-type (if (symbol? cls-type)
                   (resolve cls-type)
                   cls-type)]
    (cond
      (nio-type? cls-type)
      `(buf-get ~item ~idx ~pos)
      (list-type? cls-type)
      `(datatype->list-read-fn ~item-datatype ~item ~idx)
      (array-type? cls-type)
      `(aget ~item (pmath/+ ~idx ~pos))
      :else
      (throw (ex-info (format "Failed to discern correct read function: %s" cls-type) {})))))


(defmacro cls-type->write-fn
  [cls-type item idx pos value]
  (let [cls-type (if (symbol? cls-type)
                   (resolve cls-type)
                   cls-type)]
    (cond
      (nio-type? cls-type)
      `(buf-put ~item ~idx ~pos ~value)
      (list-type? cls-type)
      `(.set ~item ~idx ~value)
      :else
      (throw (ex-info (format "Failed to discern correct write function: %s" cls-type) {})))))


(defmacro cls-type->pos-fn
  [cls-type item]
  (let [cls-type (if (symbol? cls-type)
                   (resolve cls-type)
                   cls-type)]
    (cond
      (nio-type? cls-type)
      `(.position ~item)
      :else
      `0)))



(defmacro unchecked-full-cast
  [value value-datatype intermediate-datatype result-datatype]
  (if (and (= value-datatype intermediate-datatype)
           (= value-datatype result-datatype))
    `~value
    `(casting/datatype->unchecked-cast-fn
      ~intermediate-datatype
      ~result-datatype
      (casting/datatype->unchecked-cast-fn
       ~value-datatype
       ~intermediate-datatype
       ~value))))

(defmacro checked-full-write-cast
  [value value-datatype intermediate-datatype result-datatype]
  (if (and (= value-datatype intermediate-datatype)
           (= value-datatype result-datatype))
    `~value
    `(casting/datatype->unchecked-cast-fn
      ~intermediate-datatype
      ~result-datatype
      (casting/datatype->cast-fn
       ~value-datatype
       ~intermediate-datatype
       ~value))))

(defmacro checked-full-read-cast
  [value value-datatype intermediate-datatype result-datatype]
  (if (and (= value-datatype intermediate-datatype)
           (= value-datatype result-datatype))
    `~value
    `(casting/datatype->cast-fn
      ~intermediate-datatype
      ~result-datatype
      (casting/datatype->unchecked-cast-fn
       ~value-datatype
       ~intermediate-datatype
       ~value))))
