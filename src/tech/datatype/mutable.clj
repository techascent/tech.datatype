(ns tech.datatype.mutable
  (:require [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.list :as dtype-list]
            [tech.datatype.nio-access :as nio-access])
  (:import [tech.datatype ObjectMutable ByteMutable
            ShortMutable IntMutable LongMutable
            FloatMutable DoubleMutable BooleanMutable]
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



(defmacro make-mutable
  [mutable-cls mutable-dtype intermediate-dtype list-dtype list-data])
