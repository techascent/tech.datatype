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
            [tech.datatype.java-primitive :as primitive])
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


(defn container-type [item]
  (base/container-type item))


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
  (if (nil? item)
    nil
    (base/shape item)))


(defn shape->ecount
  ^long [shape-or-num]
  (if (nil? shape-or-num)
    0
    (base/shape->ecount shape-or-num)))


(defn copy-raw->item!
  "Copy raw data into an array.  Returns a tuple of
  [ary-target result-offset-after-copy]"
  ([raw-data ary-target target-offset options]
   (base/copy-raw->item! raw-data ary-target target-offset options))
  ([raw-data ary-target target-offset]
   (base/copy-raw->item! raw-data ary-target target-offset {})))


(defn add-datatype->size-mapping
  [datatype byte-size]
  (base/add-datatype->size-mapping datatype byte-size))


(defn datatype->byte-size
  ^long [datatype]
  (base/datatype->byte-size datatype))


(defn add-cast-fn
  [datatype cast-fn]
  (base/add-cast-fn datatype cast-fn))


(defn add-unchecked-cast-fn
  [datatype cast-fn]
  (base/add-unchecked-cast-fn datatype cast-fn))


(defn cast
  [value datatype]
  (base/cast value datatype))


(defn unchecked-cast
  [value datatype]
  (base/unchecked-cast value datatype))


(defn ->vector
  "Conversion to persistent vector"
  [item]
  (base/->vector item))


(defn from-prototype
  [item & {:keys [datatype shape]}]
  (base/from-prototype item
                       (or datatype (get-datatype item))
                       (or shape (base/shape item))))


(defn clone
  [item & {:keys [datatype]}]
  (base/clone item (or datatype (get-datatype item))))


(defn add-container-conversion-fn
  "Add a container->container conversion.  Function takes a dst-datatype and the src-container
and returns a tuple of [result-container result-offset]."
  [src-container-type dst-container-type convert-fn]
  (base/add-container-conversion-fn src-container-type dst-container-type convert-fn))


(defn add-copy-operation
  "Add a new copy operation.  Note that this is a single point in a 5 dimensional space
of operations."
  [src-container-type dst-container-type src-dtype dst-dtype unchecked? copy-fn]
  (base/add-copy-operation src-container-type dst-container-type src-dtype dst-dtype unchecked? copy-fn))


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
  (primitive/make-array-of-type datatype elem-count-or-seq
                                (or options {})))


(defn ->array
  "Returns nil of item does not share a backing store with an array."
  [item]
  (primitive/->array item))


(defn ->array-copy
  "Copy the data into an array that can correctly hold the datatype.  This
  array may not have the same datatype as the source item"
  [item]
  (primitive/->array-copy item))

(defn make-buffer-of-type
  [datatype elem-count-or-seq & [options]]
  (primitive/make-buffer-of-type datatype elem-count-or-seq
                                 (or options {})))


(defn ->buffer-backing-store
  "Convert to nio buffer that stores the data for the object.  This may have
  a different datatype than the object, so for instance the backing store for
  the uint8 datatype is a nio buffer of type int8."
  [src-ary]
  (primitive/->buffer-backing-store src-ary))


(defn make-typed-buffer
  "Support for unsigned datatypes comes via the typed buffer mechanism"
  [datatype elem-count-or-seq & [options]]
  ;;Dynamic require because this auto-generates quite a bit of code and
  ;;I think most people will not need this.
  (require '[tech.datatype.java-unsigned :as unsigned])
  ((resolve 'tech.datatype.java-unsigned/make-typed-buffer)
   datatype elem-count-or-seq (or options {})))


(defn ->typed-buffer
  "Conversion of a thing to a typed buffer"
  [item & {:keys [datatype]}]
  (require '[tech.datatype.java-unsigned :as unsigned])
  (let [datatype (or datatype (get-datatype item))
        retval ((resolve 'tech.datatype.java-unsigned/->typed-buffer) item)]
    (assoc retval :dtype datatype)))
