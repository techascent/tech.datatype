(ns tech.v2.datatype.comparator
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting])
  (:import [it.unimi.dsi.fastutil.bytes ByteArrays ByteComparator]
           [it.unimi.dsi.fastutil.shorts ShortArrays ShortComparator]
           [it.unimi.dsi.fastutil.ints IntArrays IntComparator]
           [it.unimi.dsi.fastutil.longs LongArrays LongComparator]
           [it.unimi.dsi.fastutil.floats FloatArrays FloatComparator]
           [it.unimi.dsi.fastutil.doubles DoubleArrays DoubleComparator]
   [tech.v2.datatype
            Comparator$ByteComp
            Comparator$ShortComp
            Comparator$IntComp
            Comparator$LongComp
            Comparator$FloatComp
    Comparator$DoubleComp]
   [java.util Comparator]))


(defn datatype->comparator-type
  [datatype]
  (case datatype
    :int8 'it.unimi.dsi.fastutil.bytes.ByteComparator
    :int16 'it.unimi.dsi.fastutil.shorts.ShortComparator
    :int32 'it.unimi.dsi.fastutil.ints.IntComparator
    :int64 'it.unimi.dsi.fastutil.longs.LongComparator
    :float32 'it.unimi.dsi.fastutil.floats.FloatComparator
    :float64 'it.unimi.dsi.fastutil.doubles.DoubleComparator
    :object 'java.util.Comparator))


(defmacro datatype->comp-impl
  [datatype comparator]
  `(if (instance? ~(resolve (datatype->comparator-type datatype)) ~comparator)
     ~comparator
     (throw (ex-info (format "Comparator is not of correct type: %s" ~comparator) {}))))


(defn int8-comparator ^ByteComparator [item] (datatype->comp-impl :int8 item))
(defn int16-comparator ^ShortComparator [item] (datatype->comp-impl :int16 item))
(defn int32-comparator ^IntComparator [item] (datatype->comp-impl :int32 item))
(defn int64-comparator ^LongComparator [item] (datatype->comp-impl :int64 item))
(defn float32-comparator ^FloatComparator [item] (datatype->comp-impl :float32 item))
(defn float64-comparator ^DoubleComparator [item] (datatype->comp-impl :float64 item))
(defn object-comparator ^Comparator [item] (datatype->comp-impl :object item))


(defmacro datatype->comparator
  [datatype comp-item]
  (case datatype
    :int8 `(int8-comparator ~comp-item)
    :int16 `(int16-comparator ~comp-item)
    :int32 `(int32-comparator ~comp-item)
    :int64 `(int64-comparator ~comp-item)
    :float32 `(float32-comparator ~comp-item)
    :float64 `(float64-comparator ~comp-item)
    :object `(object-comparator ~comp-item)))


(defn datatype->tech-comparator-type
  [datatype]
  (case datatype
    :int8 'tech.v2.datatype.Comparator$ByteComp
    :int16 'tech.v2.datatype.Comparator$ShortComp
    :int32 'tech.v2.datatype.Comparator$IntComp
    :int64 'tech.v2.datatype.Comparator$LongComp
    :float32 'tech.v2.datatype.Comparator$FloatComp
    :float64 'tech.v2.datatype.Comparator$DoubleComp
    :object 'java.util.Comparator))

(defn datatype->tech-comparator-fn-name
  [datatype]
  (case datatype
    :int8 'compareBytes
    :int16 'compareShorts
    :int32 'compareInts
    :int64 'compareLongs
    :float32 'compareFloats
    :float64 'compareDoubles
    :object 'compare))


(defmacro make-comparator
  "Make typed comparator of a given type.  Available types are the base numeric signed
  types and object (boolean is excluded).  Arguments to the comparator are exported into
  the local scope by the name of 'lhs' and 'rhs'.

  (make-comparator :int32 (Integer/compareTo lhs rhs))"
  [datatype comp-body]
  `(reify
     ~(datatype->tech-comparator-type datatype)
     (~(datatype->tech-comparator-fn-name datatype)
      [item# ~'lhs ~'rhs]
      ~comp-body)))


(defmacro default-compare-fn
  [datatype lhs rhs]
  (case datatype
    :int8 `(Byte/compare ~lhs ~rhs)
    :int16 `(Short/compare ~lhs ~rhs)
    :int32 `(Integer/compare ~lhs ~rhs)
    :int64 `(Long/compare ~lhs ~rhs)
    :float32 `(Float/compare ~lhs ~rhs)
    :float64 `(Double/compare ~lhs ~rhs)
    :object `(compare ~lhs ~rhs)))


(defn default-comparator
  [datatype]
  (let [datatype (casting/safe-flatten datatype)]
    (case datatype
      :object (make-comparator :object (default-compare-fn :object lhs rhs))
      :int8 (make-comparator :int8 (default-compare-fn :int8 lhs rhs))
      :int16 (make-comparator :int16 (default-compare-fn :int16 lhs rhs))
      :int32 (make-comparator :int32 (default-compare-fn :int32 lhs rhs))
      :int64 (make-comparator :int64 (default-compare-fn :int64 lhs rhs))
      :float32 (make-comparator :float32 (default-compare-fn :float32 lhs rhs))
      :float64 (make-comparator :float64 (default-compare-fn :float64 lhs rhs)))))


(extend-protocol dtype-proto/PDatatype
  ByteComparator
  (get-datatype [_] :int8)
  ShortComparator
  (get-datatype [_] :int16)
  IntComparator
  (get-datatype [_] :int32)
  LongComparator
  (get-datatype [_] :int64)
  FloatComparator
  (get-datatype [_] :float32)
  DoubleComparator
  (get-datatype [_] :float32)
  Comparator
  (get-datatype [_] :object))
