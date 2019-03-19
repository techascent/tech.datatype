(ns tech.datatype
  "Generalized efficient manipulations of sequences of primitive datatype.  Includes
  specializations for java arrays and nio buffers.  There are specializations to allow
  implementations to provide efficient full typed copy functions when the types can be
  ascertained.  Usually this involves a double-dispatch on both the src and dest
  arguments:

  https://en.wikipedia.org/wiki/Double_dispatch.

  Generic operations include:
  1. datatype of this sequence.
  2. Writing to, reading from.
  3. Construction.
  4. Efficient mutable copy from one sequence to another."
  (:require [clojure.core.matrix.macros :refer [c-for]]
            [clojure.core.matrix.protocols :as mp]
            [clojure.core.matrix :as m]
            [tech.datatype.base :as base]
            [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.array :as dtype-array]
            [tech.datatype.nio-buffer :as dtype-nio]
            [tech.datatype.typed-buffer :as dtype-tbuf]
            [tech.datatype.jna :as dtype-jna]
            [tech.parallel :as parallel])
  (:refer-clojure :exclude [cast]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn get-datatype
  [item]
  (base/get-datatype item))

(defn set-value! [item offset value]
  (base/set-value! item offset value))


(defn set-constant! [item offset value elem-count]
  (base/set-constant! item offset value elem-count))


(defn get-value [item offset]
  (base/get-value item offset))


(defn ecount
  "Type hinted ecount so numeric expressions run faster.
Calls clojure.core.matrix/ecount."
  ^long [item]
  (if (nil? item)
    0
    (base/ecount item)))


(defn shape
  "m/shape with fallback to m/ecount if m/shape is not available."
  [item]
  (base/shape item))


(defn shape->ecount
  ^long [shape-or-num]
  (if (nil? shape-or-num)
    0
    (base/shape->ecount shape-or-num)))


(defn copy-raw->item!
  "Copy raw data into an array.  Returns a tuple of
  [ary-target result-offset-after-copy]"
  ([raw-data ary-target target-offset options]
   (dtype-proto/copy-raw->item! raw-data ary-target target-offset options))
  ([raw-data ary-target target-offset]
   (dtype-proto/copy-raw->item! raw-data ary-target target-offset {})))


(defn datatype->byte-size
  ^long [datatype]
  (base/datatype->byte-size datatype))


(defn add-cast-fn
  [datatype cast-fn]
  (casting/add-cast-fn datatype cast-fn))


(defn add-unchecked-cast-fn
  [datatype cast-fn]
  (casting/add-unchecked-cast-fn datatype cast-fn))


(defn cast
  [value datatype]
  (casting/cast value datatype))


(defn unchecked-cast
  [value datatype]
  (casting/unchecked-cast value datatype))


(defn ->vector
  "Conversion to persistent vector"
  [item]
  (base/->vector item))


(defn from-prototype
  [item & {:keys [datatype shape]}]
  (dtype-proto/from-prototype item
                       (or datatype (get-datatype item))
                       (or shape (base/shape item))))


(defn clone
  [item & {:keys [datatype]}]
  (dtype-proto/clone item (or datatype (get-datatype item))))


(defn copy!
  ([src src-offset dst dst-offset n-elems options]
   (base/copy! src src-offset dst dst-offset n-elems options))
  ([src src-offset dst dst-offset n-elems]
   (base/copy! src src-offset dst dst-offset n-elems {}))
  ([src dst]
   (base/copy! src 0 dst 0 (ecount src) {})))


;; Primitive functions.  Would be different for clojurescript.

(defn make-array-of-type
  [datatype elem-count-or-seq & [options]]
  (dtype-array/make-array-of-type datatype elem-count-or-seq
                                (or options {})))


(defn ->array
  "Returns nil of item does not share a backing store with an array."
  [item]
  (dtype-proto/->array item))


(defn ->sub-array
  "Returns map of the backing array plus a length and offset
  of nil of this item is not array backed.
  {:array-data :offset :length}"
  [item]
  (dtype-proto/->sub-array item))


(defn ->array-copy
  "Copy the data into an array that can correctly hold the datatype.  This
  array may not have the same datatype as the source item"
  [item]
  (dtype-proto/->array-copy item))

(defn make-buffer-of-type
  [datatype elem-count-or-seq & [options]]
  (dtype-nio/make-buffer-of-type datatype elem-count-or-seq
                                 (or options {})))


(defn ->buffer-backing-store
  "Convert to nio buffer that stores the data for the object.  This may have
  a different datatype than the object, so for instance the backing store for
  the uint8 datatype is a nio buffer of type int8."
  [src-ary]
  (dtype-proto/->buffer-backing-store src-ary))


(defn make-typed-buffer
  "Support for unsigned datatypes comes via the typed buffer mechanism"
  [datatype elem-count-or-seq & [options]]
  (dtype-tbuf/make-typed-buffer datatype elem-count-or-seq (or options {})))


(defn ->typed-buffer
  "Conversion of a thing to a typed buffer"
  [item & {:keys [datatype]}]
  (let [datatype (or datatype (get-datatype item))]
    (-> (dtype-tbuf/->typed-buffer item)
        (assoc :datatype datatype))))


(defn make-container
  "Generically make a container of the given type.  Built in types are:
  - Support for host primitive types, boolean, and all object types:
  :java-array

  - Support for host primitive types
  :nio-buffer

  - Support for all (including unsigned) primitive numeric types
  :native-buffer

  (support all known datatypes, java array or list backed)
  :typed-buffer"
  [container-type datatype elem-count-or-seq & [options]]
  (dtype-proto/make-container container-type datatype
                              elem-count-or-seq options))
