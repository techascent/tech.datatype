(ns tech.datatype.base
  "Datatype library primitives shared between clojurescript and clojure.  The datatype
  system is an extensible system to provide understanding of and access to an undefined
  set of datatypes and containers that hold contiguous sections of those datatypes."

  (:require [clojure.core.matrix :as m])
  (:refer-clojure :exclude [cast])
  #?(:clj (:require [tech.datatype.base-macros :as base-macros]
                    [clojure.core.matrix.macros :refer [c-for]])
     :cljs (:require-macros [tech.datatype.base-macros :as base-macros]
                            [clojure.core.matrix.macros :refer [c-for]])))


(defprotocol PDatatype
  (get-datatype [item]))


(defprotocol PAccess
  (set-value! [item offset value])
  (set-constant! [item offset value elem-count])
  (get-value [item offset]))


(defprotocol PContainerType
  (container-type [item]))


(defprotocol PCopyRawData
  "Given a sequence of data copy it as fast as possible into a target item."
  (copy-raw->item! [raw-data ary-target target-offset options]))


(defprotocol PPersistentVector
  "Conversion to a persistent vector of numbers."
  (->vector [item]))


(defprotocol PPrototype
  (from-prototype [item datatype shape]))


(defprotocol PClone
  (clone [item datatype]))


(defn shape->ecount
  ^long [shape-or-num]
  (if (number? shape-or-num)
    (long shape-or-num)
    (do
      (when-not (seq shape-or-num)
        (throw (ex-info "Shape appears to not be a shape"
                        {:shape shape-or-num})))
      (when-not (> (count shape-or-num) 0)
        (throw (ex-info "Empty shape is meaningless" {:shape shape-or-num})))
      (long (apply * shape-or-num)))))


;;Map of keyword datatype to size in bytes.
(def ^:dynamic *datatype->size-map* (atom {}))

;;Container Conversion Table
;;{src-container-type {dst-container-type (fn [src dst-container-type] [dst dst-offset])
;;}}
;;Conversion map is a map of all destination conversions available
;;given a source container type.
(def ^:dynamic *container-conversion-table* (atom {}))

;;Copy table maps tuple of:
;;[src-container dst-container src-dtype dst-dtype unchecked?]
;;to a function:
;;(fn [src src-offset dst dst-offset n-elems options] (do-the-copy ...)
(def ^:dynamic *copy-table* (atom {}))


;;Set to true to catch when copy operations are falling back to the slow generic
;;copy path.
(def ^:dynamic *error-on-slow-path* false)


;;Map of datatype to functions that will cast to that datatype.
(def ^:dynamic *cast-table* (atom {}))
(def ^:dynamic *unchecked-cast-table* (atom {}))


(defn add-datatype->size-mapping
  [datatype byte-size]
  (swap! *datatype->size-map* assoc datatype byte-size))


(defn datatype->byte-size
  ^long [datatype]
  (if-let [retval (get @*datatype->size-map* datatype)]
    (long retval)
    (throw (ex-info "Failed to find datatype->size mapping"
                    {:datatype datatype
                     :available-datatypes (keys @*datatype->size-map*)}))))


(defn add-cast-fn
  [datatype cast-fn]
  (swap! *cast-table* assoc datatype cast-fn))


(defn add-unchecked-cast-fn
  [datatype cast-fn]
  (swap! *unchecked-cast-table* assoc datatype cast-fn))


(defn cast
  [value datatype]
  (if-let [cast-fn (@*cast-table* datatype)]
    (cast-fn value)
    (throw (ex-info "No cast available" {:datatype datatype}))))


(defn unchecked-cast
  [value datatype]
  (if-let [cast-fn (@*unchecked-cast-table* datatype)]
    (cast-fn value)
    (throw (ex-info "No unchecked-cast available" {:datatype datatype}))))




(defn ecount
  "Type hinted ecount."
  ^long [item]
  (m/ecount item))


(defn shape
  [item]
  (or (m/shape item) [(ecount item)]))


#?(:cljs
   (do
    (defn- alloc-buffer
      [datatype elem-count]
      (new js/ArrayBuffer (* (datatype->byte-size datatype)
                             elem-count)))

    (defn- normalize-elem-count-or-seq
      "returns [elem-count data-seq]
  where data-seq may be nil."
      [elem-count-or-seq]
      (if (number? elem-count-or-seq)
        [(long elem-count-or-seq) nil]
        (let [array-data (new js/Array)]
          (doseq [item elem-count-or-seq]
            (.push array-data item))
          [(count array-data) array-data])))

    (defn- setup-array
      [elem-count-or-seq constructor dtype]
      (let [[data-len data-buf] (normalize-elem-count-or-seq elem-count-or-seq)
            buffer (alloc-buffer dtype data-len)
            retval (constructor buffer)]
        (when data-buf
          (loop [idx 0]
            (when (< idx data-len)
              (aset retval idx (aget data-buf idx))
              (recur (inc idx)))))
        retval))

    (defn byte-array
      [elem-count-or-seq]
      (setup-array elem-count-or-seq #(new js/Int8Array %) :int8))

    (defn short-array
      [elem-count-or-seq]
      (setup-array elem-count-or-seq #(new js/Int16Array %) :int16))

    (defn int-array
      [elem-count-or-seq]
      (setup-array elem-count-or-seq #(new js/Int32Array %) :int32))

    (defn long-array
      [elem-count-or-seq]
      (throw (ex-info "No int64 support in js" {})))

    (defn float-array
      [elem-count-or-seq]
      (setup-array elem-count-or-seq #(new js/Float32Array %) :float32))

    (defn double-array
      [elem-count-or-seq]
      (setup-array elem-count-or-seq #(new js/Float64Array %) :float64))))



(defn generic-copy!
  "Copy using PAccess protocol.  Extremely slow for large buffers."
  [item item-offset dest dest-offset elem-count options]
  (when *error-on-slow-path*
    (throw (ex-info "should not hit slow path"
                    {:src-type (type item)
                     :dest-type (type dest)})))
  (let [item-offset (long item-offset)
        dest-offset (long dest-offset)
        elem-count (long elem-count)]
    (c-for [idx 0 (< idx elem-count) (inc idx)]
           (set-value! dest (+ dest-offset idx)
                       (get-value item (+ item-offset idx))))
    dest))


(defn add-container-conversion-fn
  [src-container-type dst-container-type convert-fn]
  (swap! *container-conversion-table*
         (fn [convert-map]
           (assoc-in convert-map [src-container-type dst-container-type] convert-fn))))



(defn add-copy-operation
  "Add a new copy operation.  Note that this is a single point in a 5 dimensional space
of operations."
  [src-container-type dst-container-type src-dtype dst-dtype unchecked? copy-fn]
  (swap! *copy-table* assoc [src-container-type dst-container-type
                             src-dtype dst-dtype unchecked?]
         copy-fn))


(defn- find-copy-fn
  [src-container dst-container src-dtype dst-dtype unchecked?]
  (let [cache-fn-key [src-container
                      dst-container
                      src-dtype
                      dst-dtype
                      unchecked?]]
    (if-let [cache-fn (get @*copy-table* cache-fn-key)]
      cache-fn
      (let [copy-table @*copy-table*
            conversion-table @*container-conversion-table*
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
                     (when-let [copy-fn (get copy-table
                                             [src-conv-cont dst-conv-cont
                                              src-dtype dst-dtype unchecked?])]
                       [copy-fn
                        (when src-conv
                          (partial src-conv src-conv-cont))
                        (when dst-conv
                          (partial dst-conv dst-conv-cont))])))
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
                                n-elems options)))))

          generic-copy!)))))


(defn copy!
  "copy elem-count src items to dest items.  Options may contain unchecked in which you
  get unchecked operations."
  ([src src-offset dest dest-offset elem-count options]
   (let [src-dtype (get-datatype src)
         src-offset (long src-offset)
         dest-dtype (get-datatype dest)
         dest-offset (long dest-offset)
         elem-count (long elem-count)
         copy-fn (find-copy-fn (container-type src) (container-type dest)
                               (get-datatype src) (get-datatype dest)
                               (boolean (:unchecked? options)))]
     (base-macros/check-range src src-offset elem-count)
     (base-macros/check-range dest dest-offset elem-count)
     (copy-fn src src-offset dest dest-offset elem-count options)
     dest))
  ([src src-offset dst dst-offset elem-count]
   (copy! src src-offset dst dst-offset elem-count {:unchecked? false}))
  ([src dest]
   (copy! src 0 dest 0 (min (ecount dest) (ecount src)))))


(defn copy-raw-seq->item!
  [raw-data-seq ary-target target-offset options]
  (reduce (fn [[ary-target target-offset] new-raw-data]
            (copy-raw->item! new-raw-data ary-target target-offset options))
          [ary-target target-offset]
          raw-data-seq))


(extend-protocol PCopyRawData
  Number
  (copy-raw->item! [raw-data ary-target ^long target-offset options]
    (set-value! ary-target target-offset raw-data)
    [ary-target (+ target-offset 1)])

  clojure.lang.PersistentVector
  (copy-raw->item! [raw-data ary-target ^long target-offset options]
    (let [num-elems (count raw-data)]
     (if (= 0 num-elems)
       [ary-target target-offset]
       (if (number? (raw-data 0))
         (do
          (c-for [idx 0 (< idx num-elems) (inc idx)]
                 (set-value! ary-target (+ idx target-offset) (raw-data idx)))
          [ary-target (+ target-offset num-elems)])
         (copy-raw-seq->item! raw-data ary-target target-offset options)))))

  clojure.lang.ISeq
  (copy-raw->item! [raw-data ary-target target-offset options]
    (copy-raw-seq->item! raw-data ary-target target-offset options)))
