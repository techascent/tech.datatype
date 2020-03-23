(ns tech.v2.tensor.impl
    (:require [tech.v2.datatype.protocols :as dtype-proto]
              [tech.v2.datatype.base :as dtype-base]
              [tech.v2.datatype.casting :as casting]
              [tech.v2.datatype.sparse.protocols :as sparse-proto]
              [tech.v2.datatype.sparse.reader :as sparse-reader]
              [tech.v2.tensor.dimensions :as dims]
              [tech.v2.tensor.dimensions.analytics :as dims-analytics]
              [tech.v2.tensor.dimensions.shape :as shape]
              [tech.v2.datatype.readers.indexed :as indexed-reader]
              [tech.v2.datatype.writers.indexed :as indexed-writer]
              [tech.v2.datatype.unary-op :as unary-op]
              [tech.v2.datatype.binary-op :as binary-op]
              [tech.v2.datatype.reduce-op :as reduce-op]
              [tech.v2.datatype.boolean-op :as boolean-op]
              [tech.v2.datatype.typecast :as typecast]
              [tech.v2.datatype :as dtype]
              [tech.v2.datatype.functional :as dtype-fn]
              [tech.v2.datatype.jna :as dtype-jna]
              [tech.v2.tensor.typecast :as tens-typecast]
              [tech.v2.tensor.protocols :as tens-proto]
              [tech.v2.libs.blas :as blas]
              [tech.parallel.for :as parallel-for]
              [tech.jna :as jna])
    (:import [tech.v2.datatype
              IndexingSystem$Backward
              ObjectReader
              LongReader]
             [com.sun.jna Pointer]
             [java.io Writer]
             [java.util List]
             [clojure.lang Indexed Counted]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(def ^:dynamic *datatype* :float64)
(def ^:dynamic *container-type* :typed-buffer)

(defmacro with-datatype
  [dtype & body]
  `(with-bindings {#'*datatype* ~dtype}
     ~@body))


(defn default-datatype
  [& [dtype-or-nil]]
  (or dtype-or-nil *datatype*))


(defn default-container-type
  [& [container-type]]
  (or container-type *container-type*))


(defn- dimensions->index-reader
  [dimensions]
  ^LongReader (dims/->global->local dimensions))


(defn- dimensions->index-inverter
  ^IndexingSystem$Backward [dimensions]
  (dims/->local->global dimensions))


(defn simple-dimensions?
  [dimensions]
  (dims/native? dimensions))


(defn dims-suitable-for-desc?
  [dimensions]
  (dims/direct? dimensions))


(defn- simple-vector-dimensions?
  [dimensions]
  (and (simple-dimensions? dimensions)
       (= 1 (count (:shape dimensions)))))

(defn- global-address->global-shape
  [global-address global-strides]
  (first
   (reduce (fn [[shape global-address] global-stride]
             (let [shape-idx (quot (int global-address)
                                   (int global-stride))]
               [(conj shape shape-idx)
                (rem (int global-address)
                     (int global-stride))]))
           [[] global-address]
           global-strides)))


(defn- sparse-reader->index-seq
  [sparse-reader shape & [strides]]
  (let [strides (or strides (dims-analytics/shape-ary->strides shape))]
    (->> (when sparse-reader
           (sparse-proto/index-seq sparse-reader))
         (map (fn [{:keys [global-index] :as item}]
                (assoc item
                       :global-dims
                       (global-address->global-shape global-index strides)))))))



(defmacro make-tensor-reader
  [reader-datatype datatype item-shape indexer reader sparse-reader
   base-tensor]
  `(let [^tech.v2.tensor.LongTensorReader indexer# ~indexer
         data# (typecast/datatype->reader ~reader-datatype ~reader)
         shape# ~item-shape
         n-elems# (long (apply * 1 shape#))
         n-dims# (count shape#)
         base-tensor# ~base-tensor]
     (reify
       ~(tens-typecast/datatype->tensor-reader-type reader-datatype)
       (getDatatype [item#] ~datatype)
       (lsize [item#] n-elems#)
       (read [item# idx#]
         (.read data# (.read indexer# idx#)))
       (read2d [reader# row# col#]
         (.read data# (.read2d indexer# row# col#)))
       (read3d [reader# row# col# chan#]
         (.read data# (.read3d indexer# row# col# chan#)))
       (tensorRead [reader# indexes#]
         (.read data# (.tensorRead indexer# indexes#)))
       (applyTo [item# arglist#]
         (.read data# (apply indexer# arglist#)))

       dtype-proto/PShape
       (shape [m] shape#)

       tens-proto/PTensor
       (is-tensor? [item#] true)
       (dimensions [item#] (tens-proto/dimensions base-tensor#))
       (buffer [item#] (tens-proto/buffer base-tensor#))

       tens-proto/PToTensor
       (convertible-to-tensor? [item#] true)
       (convert-to-tensor [item#] base-tensor#)

       dtype-proto/PPrototype
       (from-prototype [m# datatype# shape#]
         (dtype-proto/from-prototype base-tensor# datatype# shape#))

       dtype-proto/PToNioBuffer
       (convertible-to-nio-buffer? [item#]
         (dtype-proto/convertible-to-nio-buffer?  base-tensor#))
       (->buffer-backing-store [item#]
         (dtype-proto/->buffer-backing-store base-tensor#))

       dtype-proto/PToList
       (convertible-to-fastutil-list? [item#]
         (dtype-proto/convertible-to-fastutil-list? base-tensor#))
       (->list-backing-store [item#]
         (dtype-proto/->list-backing-store base-tensor#))

       jna/PToPtr
       (is-jna-ptr-convertible? [item#]
         (jna/is-jna-ptr-convertible? base-tensor#))
       (->ptr-backing-store [item#]
         (jna/->ptr-backing-store base-tensor#))


       dtype-proto/PToBufferDesc
       (convertible-to-buffer-desc? [item#]
         (dtype-proto/convertible-to-buffer-desc? base-tensor#))
       (->buffer-descriptor [item#]
         (dtype-proto/->buffer-descriptor base-tensor#))


       dtype-proto/PBuffer
       (sub-buffer [item# offset# length#]
         (dtype-proto/sub-buffer base-tensor# offset# length#))

       dtype-proto/PSetConstant
       (set-constant! [item# offset# value# elem-count#]
         (dtype-proto/set-constant! base-tensor# offset# value# elem-count#))

       dtype-proto/PWriteIndexes
       (write-indexes! [item# indexes# values# options#]
         (dtype-proto/write-indexes! base-tensor# indexes# values# options#))


       dtype-proto/PToWriter
       (convertible-to-writer? [item#]
         (dtype-proto/convertible-to-writer? base-tensor#))
       (->writer [item# options#]
         (dtype-proto/->writer base-tensor# options#))


       dtype-proto/PBufferType
       (buffer-type [item#]
         (dtype-proto/buffer-type base-tensor#))

       Object
       (toString [item]
         ;;Can't think of a better way of doing this.
         ;;pprint requires the tensor methods *but* tostring requires
         ;;pprint...
         (.toString base-tensor#))
       #_(comment
         sparse-proto/PSparse
         (index-seq [item#]
                    (sparse-reader->index-seq ~sparse-reader shape# strides#))
         (sparse-value [item#] (sparse-proto/sparse-value ~sparse-reader))
         (sparse-ecount [item#] (sparse-proto/sparse-ecount ~sparse-reader))
         (readers [item#] (sparse-proto/readers ~sparse-reader))
         (iterables [item] (sparse-proto/iterables ~sparse-reader))


         sparse-proto/PToSparse
         (convertible-to-sparse? [item#] (not (nil? ~sparse-reader)))
         (->sparse [item#] item#)))))


(defmacro make-tensor-writer
  [writer-datatype datatype item-shape indexer writer base-tensor]
  `(let [^tech.v2.tensor.LongTensorReader indexer# ~indexer
         data# (typecast/datatype->writer ~writer-datatype ~writer)
         shape# ~item-shape
         n-elems# (long (apply * 1 shape#))
         n-dims# (count shape#)
         base-tensor# ~base-tensor]
     (reify
       ~(tens-typecast/datatype->tensor-writer-type writer-datatype)
       (getDatatype [item#] ~datatype)
       (lsize [item#] n-elems#)
       (write [item# idx# value#]
         (.write data# (.read indexer# idx#) value#))
       (write2d [writer# row# col# value#]
         (.write data# (.read2d indexer# row# col#) value#))
       (write3d [writer# row# col# chan# value#]
         (.write data# (.read3d indexer# row# col# chan#) value#))
       (tensorWrite [writer# indexes# value#]
         (.write data# (.tensorRead indexer# indexes#) value#))
       (applyTo [item# arglist#]
         (.write data# (apply indexer# (butlast arglist#))
                 (casting/datatype->cast-fn :unknown ~writer-datatype
                                            (last arglist#))))

       dtype-proto/PShape
       (shape [m] shape#)

       tens-proto/PTensor
       (is-tensor? [item#] true)
       (dimensions [item#] (tens-proto/dimensions base-tensor#))
       (buffer [item#] (tens-proto/buffer base-tensor#))

       tens-proto/PToTensor
       (convertible-to-tensor? [item#] true)
       (convert-to-tensor [item#] base-tensor#)

       dtype-proto/PPrototype
       (from-prototype [m# datatype# shape#]
         (dtype-proto/from-prototype base-tensor# datatype# shape#))

       dtype-proto/PToNioBuffer
       (convertible-to-nio-buffer? [item#]
         (dtype-proto/convertible-to-nio-buffer?  base-tensor#))
       (->buffer-backing-store [item#]
         (dtype-proto/->buffer-backing-store base-tensor#))

       dtype-proto/PToList
       (convertible-to-fastutil-list? [item#]
         (dtype-proto/convertible-to-fastutil-list? base-tensor#))
       (->list-backing-store [item#]
         (dtype-proto/->list-backing-store base-tensor#))

       jna/PToPtr
       (is-jna-ptr-convertible? [item#]
         (jna/is-jna-ptr-convertible? base-tensor#))
       (->ptr-backing-store [item#]
         (jna/->ptr-backing-store base-tensor#))


       dtype-proto/PToBufferDesc
       (convertible-to-buffer-desc? [item#]
         (dtype-proto/convertible-to-buffer-desc? base-tensor#))
       (->buffer-descriptor [item#]
         (dtype-proto/->buffer-descriptor base-tensor#))


       dtype-proto/PBuffer
       (sub-buffer [item# offset# length#]
         (dtype-proto/sub-buffer base-tensor# offset# length#))

       dtype-proto/PSetConstant
       (set-constant! [item# offset# value# elem-count#]
         (dtype-proto/set-constant! base-tensor# offset# value# elem-count#))

       dtype-proto/PWriteIndexes
       (write-indexes! [item# indexes# values# options#]
         (dtype-proto/write-indexes! base-tensor# indexes# values# options#))


       dtype-proto/PToWriter
       (convertible-to-writer? [item#]
         (dtype-proto/convertible-to-writer? base-tensor#))
       (->writer [item# options#]
         (dtype-proto/->writer base-tensor# options#))


       dtype-proto/PBufferType
       (buffer-type [item#]
         (dtype-proto/buffer-type base-tensor#))

       Object
       (toString [item]
         ;;Can't think of a better way of doing this.
         ;;pprint requires the tensor methods *but* tostring requires
         ;;pprint...
         (.toString base-tensor#)))))


#_(defn- make-tensor-base-sparse-reader
  [buffer dimensions]
  (when-let [reader (sparse-proto/as-sparse buffer)]
    (if (simple-dimensions? dimensions)
      reader
      ;;Else we have a somewhat significant translation step from local
      ;;sparse to global sparse.
      (let [{:keys [indexes data]} (sparse-proto/readers reader)
            addr-inverter (dimensions->index-inverter dimensions)
            direct? (dims/direct? dimensions)
            raw-shape (dtype/shape dims)
            broadcasting? (not= (:max-shape dimensions)
                                raw-shape)
            access-increasing? (dims/access-increasing? dimensions)
            dense? (dims/dense? dimensions)
            index-seq (seq (map-indexed vector indexes))
            index-seq (when index-seq
                        (if (and dense? (not broadcasting?))
                          (map (fn [[data-index global-index]]
                                 [data-index (first (.localToGlobal
                                                     addr-inverter global-index))])
                               index-seq)
                          (mapcat (fn [[data-index global-index]]
                                    (->> (.localToGlobal addr-inverter global-index)
                                         (map #(vector data-index %))))
                                  index-seq)))
            index-seq (when index-seq
                        (if (or broadcasting? (not access-increasing?))
                          (sort-by second index-seq)
                          index-seq))]
        (sparse-reader/make-sparse-reader
         (dtype-proto/make-container :list :int32
                                     (map second index-seq)
                                     {:unchecked? true})
         (indexed-reader/make-indexed-reader (dtype-proto/make-container
                                              :list :int32 (map first index-seq)
                                              {:unchecked? true})
                                             data
                                             {})
         (dims/ecount dimensions))))))



(declare slice construct-tensor select)

(deftype Tensor [buffer dimensions buffer-type]
   dtype-proto/PDatatype
   (get-datatype [item] (dtype-base/get-datatype buffer))


   dtype-proto/PCountable
   (ecount [item] (dims/ecount dimensions))


   dtype-proto/PShape
   (shape [m] (dims/shape dimensions))


   dtype-proto/PPrototype
   (from-prototype [m datatype shape]
     (construct-tensor
      (dtype-proto/from-prototype buffer datatype shape)
      dimensions))


   dtype-proto/PToNioBuffer
   (convertible-to-nio-buffer? [item]
     (dtype-proto/nio-convertible? buffer))
   (->buffer-backing-store [item]
     (when (simple-dimensions? dimensions)
       (typecast/as-nio-buffer buffer)))


   dtype-proto/PToList
   (convertible-to-fastutil-list? [item]
     (dtype-proto/list-convertible? buffer))
   (->list-backing-store [item]
     (when (simple-dimensions? dimensions)
       (typecast/as-list buffer)))


   dtype-proto/PToJNAPointer
   (convertible-to-data-ptr? [item]
     (dtype-proto/convertible-to-data-ptr? buffer))
   (->jna-ptr [item] (dtype-proto/->jna-ptr buffer))


   dtype-proto/PToBufferDesc
   (convertible-to-buffer-desc? [item]
     (and (jna/ptr-convertible? buffer)
          (dims-suitable-for-desc? dimensions)))
   (->buffer-descriptor [item]
     {:ptr (dtype-proto/->jna-ptr buffer)
      :datatype (dtype/get-datatype buffer)
      :shape (dtype/shape item)
      :strides (mapv (partial * (casting/numeric-byte-width
                                 (dtype/get-datatype buffer)))
                     (:strides dimensions))})


   dtype-proto/PToArray
   (->sub-array [item]
     (when (simple-dimensions? dimensions)
       (dtype-proto/->sub-array buffer)))

   (->array-copy [item]
     (if (simple-dimensions? dimensions)
       (dtype-proto/->array-copy buffer)
       (dtype-proto/->array-copy (dtype-proto/->writer item {}))))


   dtype-proto/PBuffer
   (sub-buffer [item offset length]
     (if (simple-dimensions? dimensions)
       (dtype-proto/sub-buffer buffer offset length)
       (throw (ex-info "Cannot sub-buffer tensors with complex addressing" {}))))


   dtype-proto/PSetConstant
   (set-constant! [item offset value elem-count]
     (if (simple-dimensions? dimensions)
       (dtype-proto/set-constant! buffer offset value elem-count)
       (if (= :sparse (dtype/buffer-type buffer))
         (dtype-proto/write-indexes! buffer
                                     (-> (dimensions->index-reader dimensions)
                                         (dtype-base/sub-buffer offset elem-count))
                                     (sparse-reader/const-sparse-reader
                                      value
                                      (dtype-base/get-datatype item)
                                      elem-count)
                                     {:indexes-in-order?
                                      (dims/access-increasing? dimensions)})
         (dtype-proto/set-constant! (indexed-writer/make-indexed-writer
                                     (dimensions->index-reader dimensions)
                                     buffer
                                     {})
                                    offset value elem-count))))


   dtype-proto/PWriteIndexes
   (write-indexes! [item indexes values options]
     (if (simple-dimensions? dimensions)
       (dtype-proto/write-indexes! buffer indexes values options)
       (dtype-proto/write-indexes! buffer (indexed-reader/make-indexed-reader
                                           indexes
                                           (dimensions->index-reader dimensions)
                                           {:datatype :int32})
                                   values options)))


   dtype-proto/PToReader
   (convertible-to-reader? [item] (dtype-proto/convertible-to-reader? buffer))
   (->reader [item options]
     (let [data-reader (dtype-proto/->reader buffer options)]
       (if (simple-dimensions? dimensions)
         data-reader
         (tens-proto/->tensor-reader item options))))


   dtype-proto/PToWriter
   (convertible-to-writer? [item] (dtype-proto/convertible-to-writer? buffer))
   (->writer [item options]
     (let [data-writer (dtype-proto/->writer buffer options)]
       (if (simple-dimensions? dimensions)
         data-writer
         (indexed-writer/make-indexed-writer (dimensions->index-reader dimensions)
                                             data-writer
                                             options))))


   dtype-proto/PBufferType
   (buffer-type [item]
     (or buffer-type (dtype-proto/buffer-type buffer)))

   ;;Tensors implement only a small subset of the sparse protocols
   ;;For the full set, calling sparse-proto/as-sparse is your only option.
   #_(comment
     sparse-proto/PSparse
     (index-seq [item]
                (sparse-reader->index-seq (sparse-proto/as-sparse item) (dtype/shape item)))
     (sparse-value [item]
                   (when (sparse-proto/as-sparse buffer)
                     (sparse-proto/sparse-value buffer)))

     sparse-proto/PToSparse
     (convertible-to-sparse? [item]
                             (sparse-proto/sparse-convertible? buffer))
     (->sparse [item]
               (if (simple-dimensions? dimensions)
                 (sparse-proto/as-sparse buffer)
                 (make-tensor-base-sparse-reader buffer dimensions))))

   tens-proto/PTensor
   (is-tensor? [item] true)
   (dimensions [item] dimensions)
   (buffer [item] buffer)

   tens-proto/PToTensor
   (convertible-to-tensor? [item] true)
   (convert-to-tensor [item] item)

   tens-proto/PToTensorReader
   (convertible-to-tensor-reader? [item]
     (dtype-proto/convertible-to-reader? buffer))
   (->tensor-reader [item options]
     (let [{:keys [datatype]} options]
       (if (and (simple-dimensions? dimensions)
                (instance? (resolve (tens-typecast/datatype->tensor-reader-type
                                     datatype))
                           buffer))
         buffer
         (let [sparse-data nil #_(make-tensor-base-sparse-reader buffer dimensions)
               data-reader (dtype-proto/->reader buffer options)
               indexes (dimensions->index-reader dimensions)
               item-shape (dtype-proto/shape item)]
           (case (casting/safe-flatten datatype)
             :int8 (make-tensor-reader :int8 datatype item-shape
                                       indexes data-reader sparse-data
                                       item)
             :int16 (make-tensor-reader :int16 datatype item-shape
                                        indexes data-reader sparse-data
                                        item)
             :int32 (make-tensor-reader :int32 datatype item-shape
                                        indexes data-reader sparse-data
                                        item)
             :int64 (make-tensor-reader :int64 datatype item-shape
                                        indexes data-reader sparse-data
                                        item)
             :float32 (make-tensor-reader :float32 datatype item-shape
                                          indexes data-reader sparse-data
                                          item)
             :float64 (make-tensor-reader :float64 datatype item-shape
                                          indexes data-reader sparse-data
                                          item)
             :boolean (make-tensor-reader :boolean datatype item-shape
                                          indexes data-reader sparse-data
                                          item)
             :object (make-tensor-reader :object datatype item-shape
                                         indexes data-reader sparse-data
                                         item))))))
   tens-proto/PToTensorWriter
   (convertible-to-tensor-writer? [item]
     (dtype-proto/convertible-to-writer? buffer))
   (->tensor-writer [item options]
     (let [{:keys [datatype]} options]
       (if (and (simple-dimensions? dimensions)
                (instance? (resolve (tens-typecast/datatype->tensor-writer-type
                                     datatype))
                           buffer))
         buffer
         (let [data-writer (dtype-proto/->writer buffer options)
               indexes (dimensions->index-reader dimensions)
               item-shape (dtype-proto/shape item)]
           (case (casting/safe-flatten datatype)
             :int8 (make-tensor-writer :int8 datatype item-shape
                                       indexes data-writer
                                       item)
             :int16 (make-tensor-writer :int16 datatype item-shape
                                        indexes data-writer
                                        item)
             :int32 (make-tensor-writer :int32 datatype item-shape
                                        indexes data-writer
                                        item)
             :int64 (make-tensor-writer :int64 datatype item-shape
                                        indexes data-writer
                                        item)
             :float32 (make-tensor-writer :float32 datatype item-shape
                                          indexes data-writer
                                          item)
             :float64 (make-tensor-writer :float64 datatype item-shape
                                          indexes data-writer
                                          item)
             :boolean (make-tensor-writer :boolean datatype item-shape
                                          indexes data-writer
                                          item)
             :object (make-tensor-writer :object datatype item-shape
                                         indexes data-writer
                                         item))))))
   Iterable
   (iterator [item]
     (.iterator ^Iterable (slice item 1)))


   ;;We can't implement ObjectReader because then the tensor definition is ambiguous
   ;;because when tensors are interpreted as readers they are simply a flat list of
   ;;things in row major format.
   ;;We can, however, implement enough stuff so you can destructure tensors and
   ;;so clojure can deal with them intelligently.
   Counted
   (count [item] (int (first (dims/shape dimensions))))

   Indexed
   (nth [item idx]
     (if (= 1 (count (:shape dimensions)))
       (.read (typecast/datatype->reader :object (dtype/->reader item :object)) idx)
       (apply select item idx (repeat (dec (count (:shape dimensions)))
                                      :all))))
   (nth [item idx def-val]
    (let [shape (dims/shape dimensions)]
      (if (< idx (long (first shape)))
        (nth item idx)
        def-val)))

   Object
   (toString [item]
     ;;Can't think of a better way of doing this.
     ;;pprint requires the tensor methods *but* tostring requires
     ;;pprint...
     (tens-proto/print-tensor item)))


(defn construct-tensor
  [buffer dimensions & [buffer-type]]
  (let [buffer-type (or buffer-type :tensor)]
    (Tensor. buffer dimensions buffer-type)))


(defn tensor?
  [item]
  (tens-proto/is-tensor? item))


(defn tensor-buffer-type
  [tens]
  (if (tensor? tens)
    (dtype/buffer-type (tens-proto/buffer tens))
    (dtype/buffer-type tens)))


(defn tensor-container-type
  [tens]
  (if (tensor? tens)
    (dtype/container-type (tens-proto/buffer tens))
    (dtype/container-type tens)))


(declare ->tensor)


(defn ensure-tensor
  "If you can, make a tensor without copying the data.  If you can't,
  make a new tensor from the data."
  [item]
  (if (tensor? item)
    item
    (if-let [retval (tens-proto/as-tensor item)]
      retval
      (->tensor item))))


(defn tensor->buffer
  [tens]
  (tens-proto/buffer tens))


(defn tensor->dimensions
  [tens]
  (tens-proto/dimensions tens))


(defn mutable?
  "Does this tensor have the ability to be written to."
  [tens]
  (satisfies? dtype-proto/PToWriter (tensor->buffer tens)))


(defn tensor->base-buffer-type
  [tens]
  (if (tensor? tens)
    (construct-tensor (tens-proto/buffer tens)
                      (tens-proto/dimensions tens)
                      (dtype/buffer-type
                       (tensor->buffer tens)))
    tens))


(defn explicit-make-tensor
  [data {:keys [datatype container-type] :as options}]
  (let [data-shape (dtype/shape data)
        datatype (default-datatype datatype)
        container-type (default-container-type container-type)
        n-elems (apply * 1 data-shape)]
    (when (= container-type :sparse)
      (throw (Exception. "Sparse tensors are not yet updated.")))
    (construct-tensor
     (first
      (dtype/copy-raw->item!
       data
       (dtype/make-container container-type datatype n-elems options)
       0 options))
     (dims/dimensions data-shape))))


(defn ->tensor
  [data & {:keys [datatype container-type]
           :as options}]
  (explicit-make-tensor data options))


(defn new-tensor
  [shape & {:keys [datatype container-type]
            :as options}]
  (let [datatype (default-datatype datatype)
        container-type (default-container-type container-type)
        n-elems (apply * 1 shape)]
    (construct-tensor
     (dtype/make-container container-type datatype n-elems options)
     (dims/dimensions shape))))


(defn clone
  [tens & {:keys [datatype
                  container-type]}]
  (let [datatype (or datatype (dtype/get-datatype tens))
        container-type (default-container-type (or container-type
                                                   (dtype/container-type tens)))
        new-buffer (if (and (satisfies? dtype-proto/PPrototype (tensor->buffer tens))
                            (= container-type (dtype/container-type tens)))
                     (dtype/from-prototype (tensor->buffer tens)
                                           :datatype datatype
                                           :shape (dtype/shape tens))
                     (dtype/make-container (default-container-type container-type)
                                           datatype
                                           (dtype/ecount tens)))
        new-tens (construct-tensor
                  new-buffer
                  (dims/dimensions (dtype/shape tens)))]
    (dtype/copy! tens new-tens)))


(defn tensor-force
  "Ensure any delayed operations happen for this and reads from this tensor
  happen reasonably fast.  For sparse this probably means cloning."
  [tens]
  (let [buffer-type (dtype/buffer-type (tens-proto/buffer tens))
        new-tens (if (= :sparse buffer-type)
                   (construct-tensor
                    (sparse-proto/as-sparse tens)
                    (dims/dimensions (dtype/shape tens)))
                   ;;force a potentially deep reader chain.
                   (if (or (not (simple-dimensions? (tens-proto/dimensions tens)))
                           ;;In the case of a reader chain, we will no longer
                           ;;be able to get the buffer back from the tensor.
                           (not (dtype-proto/as-nio-buffer tens)))
                     (clone tens)
                     tens))]
    ;;force actual creation of dimension transforms
    (dims/->global->local (tens-proto/dimensions new-tens))
    ;;Sparse always needs the inverse transform
    (when (= :sparse buffer-type)
      (dims/->local->global (tens-proto/dimensions new-tens)))
    new-tens))


(defn broadcast?
  [tens]
  (let [dimensions (tens-proto/dimensions tens)]
    (not= (shape/shape->count-vec (:shape dimensions))
          (:max-shape dimensions))))



(defn rotate
  [tens rotate-vec]
  (let [tens (ensure-tensor tens)]
    (construct-tensor (tens-proto/buffer tens)
                      (dims/rotate (tensor->dimensions tens)
                                   (mapv #(* -1 (long %)) rotate-vec)))))


(defn reshape
  [tens new-shape]
  (let [tens (ensure-tensor tens)
        new-dims (dims/in-place-reshape (tens-proto/dimensions tens)
                                        new-shape)
        buffer-ecount (dims/buffer-ecount new-dims)
        new-buffer (if buffer-ecount
                     (-> (tensor->buffer tens)
                         (dtype/sub-buffer 0 buffer-ecount))
                     (tensor->buffer tens))]
    (construct-tensor new-buffer new-dims)))


(defn transpose
  [tens transpose-vec]
  (let [tens (ensure-tensor tens)]
    (construct-tensor (tens-proto/buffer tens)
                      (dims/transpose (tens-proto/dimensions tens)
                                      transpose-vec))))


(defn select
  [tens & args]
  (let [tens (ensure-tensor tens)
        {buf-offset :elem-offset
         buf-len :buffer-ecount
         :as new-dims}
        (apply dims/select (tens-proto/dimensions tens) args)]
    (construct-tensor (-> (tensor->buffer tens)
                          (dtype/sub-buffer buf-offset buf-len))
                      new-dims)))


(defn broadcast
  "Create a larger tensor by repeating dimensions of a smaller tensor."
  [tens bcast-shape]
  (let [tens (ensure-tensor tens)
        tens-shape (dtype/shape tens)
        n-tens-elems (dtype/ecount tens)
        n-bcast-elems (shape/ecount bcast-shape)
        num-tens-shape (count tens-shape)
        {:keys [shape strides] :as original-dims} (tens-proto/dimensions tens)]
    (when-not (every? number? bcast-shape)
      (throw (ex-info "Broadcast shapes must only be numbers" {})))
    (when-not (>= n-bcast-elems
                  n-tens-elems)
      (throw (ex-info
              (format "Improper broadcast shape (%s), smaller than tens (%s)"
                              bcast-shape tens-shape)
                      {})))
    (when-not (every? (fn [[item-dim bcast-dim]]
                        (= 0 (rem (int bcast-dim)
                                  (int item-dim))))
                      (map vector tens-shape (take-last num-tens-shape bcast-shape)))
      (throw (ex-info
              (format "Broadcast shape (%s) is not commensurate with tensor shape %s"
                              bcast-shape tens-shape)
              {})))
    (construct-tensor (tens-proto/buffer tens)
                      (dims/broadcast original-dims bcast-shape))))


(defn slice
  "Return a sequence of tensors of reduced dimensionality.  n-dims indicates the number
  of leading dimensions to remove.  For example, if you have an item of shape [3 4] and
  1 is one you get a sequence of 3 vectors of length 4.  Returns a :object reader
  where each index maps to a tensor."
  ([tens]
   (slice tens 1))
  ([tens slice-dims]
   (let [t-shape (dtype/shape tens)
         n-shape (count t-shape)
         slice-dims (long slice-dims)]
     (when-not (<= slice-dims n-shape)
       (throw (ex-info (format "Slice operator n-dims out of range: %s:%s"
                               slice-dims t-shape)
                       {})))
     (if (== slice-dims n-shape)
       (dtype/->reader tens)
       (let [{:keys [dimensions offsets]} (dims/slice (tensor->dimensions tens)
                                                      slice-dims)
             ^LongReader offsets offsets
             n-offsets (.lsize offsets)
             tens-buf (tensor->buffer tens)
             buf-len (dtype/ecount tens-buf)]
         (reify ObjectReader
           (lsize [rdr] n-offsets)
           (read [rdr idx]
             (construct-tensor (dtype/sub-buffer
                                tens-buf
                                (.read offsets idx)
                                (:buffer-ecount dimensions))
                               dimensions))))))))


(defn slice-right
  "Return a sequence of tensors of reduced dimensionality.  n-dims indicates the number
  of leading dimensions to remove.  For example, if you have an item of shape [3 4] and
  1 is one you get a sequence of 3 vectors of length 4.  Returns a :object reader
  where each index maps to a tensor."
  ([tens]
   (slice-right tens 1))
  ([tens slice-dims]
   (let [t-shape (dtype/shape tens)
         n-shape (count t-shape)
         slice-dims (long slice-dims)]
     (when-not (<= slice-dims n-shape)
       (throw (ex-info (format "Slice operator n-dims out of range: %s:%s"
                               slice-dims t-shape)
                       {})))
     (let [{:keys [dimensions offsets]} (dims/slice-right
                                         (tensor->dimensions tens)
                                         slice-dims)
           ^LongReader offsets offsets
           n-offsets (.lsize offsets)
           tens-buf (tensor->buffer tens)
           buf-len (dtype/ecount tens-buf)]
       (if (== slice-dims n-shape)
         (dtype/->reader (construct-tensor tens-buf dimensions))
         (reify ObjectReader
           (lsize [rdr] n-offsets)
           (read [rdr idx]
             (construct-tensor (dtype/sub-buffer
                                tens-buf
                                (.read offsets idx)
                                (:buffer-ecount dimensions))
                               dimensions))))))))


(defn ensure-buffer-descriptor
  "Get a buffer descriptor from the tensor.  This may copy the data.  If you want to
  ensure sharing, use the protocol ->buffer-descriptor function."
  [tens]
  (let [tens (ensure-tensor tens)]
    (if (dtype-proto/convertible-to-buffer-desc? tens)
      (dtype-proto/->buffer-descriptor tens)
      (-> (clone tens :container-type :native-buffer)
          dtype-proto/->buffer-descriptor))))


(defn buffer-descriptor->tensor
  "Given a buffer descriptor, produce a tensor"
  [{:keys [ptr datatype shape strides]}]
  (when (or (not ptr)
            (= 0 (Pointer/nativeValue ptr)))
    (throw (ex-info "Cannot create tensor from nil pointer."
                    {:ptr ptr})))
  (let [dtype-size (casting/numeric-byte-width datatype)]
    (when-not (every? #(= 0 (rem (long %)
                                 dtype-size))
                      strides)
      (throw (ex-info "Strides are not commensurate with datatype size." {})))
    (let [max-stride-idx (dtype-fn/argmax strides)
          buffer-len (* (long (dtype/get-value shape max-stride-idx))
                        (long (dtype/get-value strides max-stride-idx)))
          ;;Move strides into elem-count instead of byte-count
          strides (mapv #(quot (long %) dtype-size)
                        strides)]
      (-> (dtype-jna/unsafe-ptr->typed-pointer
           ptr buffer-len datatype)
          (construct-tensor (dims/dimensions shape strides))))))


(defmethod unary-op/unary-reader-map :tensor
  [options un-op item]
  (construct-tensor (unary-op/unary-reader-map
                     options un-op
                     (tensor->base-buffer-type item))
                    (dims/dimensions (dtype/shape item))))


(defmethod boolean-op/boolean-unary-reader-map :tensor
  [options bool-op item]
  (construct-tensor (boolean-op/boolean-unary-reader-map
                     options bool-op
                     (tensor->base-buffer-type item))
                    (dims/dimensions (dtype/shape item))))


(defn default-tensor-binary-reader-map
  "Anything times a tensor returns a thing in the shape of
  the tensor.  ecounts must match."
  [options bin-op lhs rhs]
  (when-not (= (dtype-base/ecount lhs)
               (dtype-base/ecount rhs))
    (throw (ex-info "Ecounts don't match" {})))
  (let [lhs-shape (dtype-base/shape lhs)
        lhs-tensor? (tensor? lhs)
        lhs (if (tensor? lhs)
              (tensor->base-buffer-type lhs)
              lhs)
        rhs-shape (dtype-base/shape rhs)
        rhs (if (tensor? rhs)
              (tensor->base-buffer-type rhs)
              rhs)]
    (construct-tensor
     (binary-op/binary-reader-map options bin-op lhs rhs)
     (if lhs-tensor?
       (dims/dimensions lhs-shape)
       (dims/dimensions rhs-shape)))))

;; Next up
(defmethod binary-op/binary-reader-map [:dense :tensor]
  [options bin-op lhs rhs]
  (default-tensor-binary-reader-map options bin-op lhs rhs))

(defmethod binary-op/binary-reader-map [:tensor :dense]
  [options bin-op lhs rhs]
  (default-tensor-binary-reader-map options bin-op lhs rhs))

(defmethod binary-op/binary-reader-map [:tensor :tensor]
  [options bin-op lhs rhs]
  (default-tensor-binary-reader-map options bin-op lhs rhs))

(defmethod binary-op/binary-reader-map [:sparse :tensor]
  [options bin-op lhs rhs]
  (default-tensor-binary-reader-map options bin-op lhs rhs))


(defmethod binary-op/binary-reader-map [:tensor :sparse]
  [options bin-op lhs rhs]
  (default-tensor-binary-reader-map options bin-op lhs rhs))


(defn default-tensor-binary-boolean-reader-map
  "Anything times a tensor returns a thing in the shape of
  the tensor.  ecounts must match."
  [options bin-op lhs rhs]
  (when-not (= (dtype-base/ecount lhs)
               (dtype-base/ecount rhs))
    (throw (ex-info "Ecounts don't match" {})))
  (let [lhs-shape (dtype-base/shape lhs)
        lhs-tensor? (tensor? lhs)
        lhs (if (tensor? lhs)
              (tensor->base-buffer-type lhs)
              lhs)
        rhs-shape (dtype-base/shape rhs)
        rhs (if (tensor? rhs)
              (tensor->base-buffer-type rhs)
              rhs)]
    (construct-tensor
     (boolean-op/boolean-binary-reader-map options bin-op lhs rhs)
     (if lhs-tensor?
       (dims/dimensions lhs-shape)
       (dims/dimensions rhs-shape)))))

;; Next up
(defmethod boolean-op/boolean-binary-reader-map [:dense :tensor]
  [options bin-op lhs rhs]
  (default-tensor-binary-boolean-reader-map options bin-op lhs rhs))

(defmethod boolean-op/boolean-binary-reader-map [:tensor :dense]
  [options bin-op lhs rhs]
  (default-tensor-binary-boolean-reader-map options bin-op lhs rhs))

(defmethod boolean-op/boolean-binary-reader-map [:tensor :tensor]
  [options bin-op lhs rhs]
  (default-tensor-binary-boolean-reader-map options bin-op lhs rhs))

(defmethod boolean-op/boolean-binary-reader-map [:sparse :tensor]
  [options bin-op lhs rhs]
  (default-tensor-binary-boolean-reader-map options bin-op lhs rhs))


(defmethod boolean-op/boolean-binary-reader-map [:tensor :sparse]
  [options bin-op lhs rhs]
  (default-tensor-binary-boolean-reader-map options bin-op lhs rhs))


(defn ->jvm
  "Conversion to storage that is efficient for the jvm.
  Base storage is either jvm-array or persistent-vector."
  [item & {:keys [datatype base-storage]
           :or {base-storage :persistent-vector}}]
  ;;Get the data off the device
  (let [item-shape (dtype-base/shape item)
        item-ecount (dtype-base/ecount item)
        column-len (long (last item-shape))
        n-columns (quot item-ecount column-len)
        datatype (or datatype (dtype-base/get-datatype item))
        data-array (dtype-proto/->reader item {:datatype datatype})
        base-data
        (->> (range n-columns)
             (map (fn [col-idx]
                    (let [col-offset (* column-len (long col-idx))]
                      (case base-storage
                        :java-array
                        (let [retval (dtype/make-array-of-type datatype
                                                               column-len)]
                          (dtype/copy! data-array col-offset
                                       retval 0 column-len {:unchecked? true}))
                        :persistent-vector
                        (mapv #(dtype/get-value data-array (+ (long %1)
                                                              col-offset))
                              (range column-len)))))))
        partitionv (fn [& args]
                     (map vec (apply partition args)))
        partition-shape (->> (rest item-shape)
                             drop-last
                             reverse)]
    (if (> (count item-shape) 1)
      (->> partition-shape
           (reduce (fn [retval part-value]
                     (partitionv part-value retval))
                   base-data)
           vec)
      (first base-data))))


(defmethod dtype-proto/copy! [:tensor :dense]
  [dst src options]
  (dtype-proto/copy! dst (tensor->base-buffer-type src) options)
  dst)


(defmethod dtype-proto/copy! [:tensor :sparse]
  [dst src options]
  (dtype-proto/copy! dst (tensor->base-buffer-type src) options)
  dst)


(defmethod dtype-proto/copy! [:dense :tensor]
  [dst src options]
  (dtype-proto/copy! (tensor->base-buffer-type dst) src options)
  dst)


(defmethod dtype-proto/copy! [:sparse :tensor]
  [dst src options]
  (dtype-proto/copy! (tensor->base-buffer-type dst) src options)
  dst)


(defmacro impl-dot-product
  [match-criteria]
  `(defmethod reduce-op/dot-product ~match-criteria
     [options# lhs# rhs# bin-op# reduce-op#]
     (when (or (not= 1 (count (dtype-base/shape lhs#)))
               (not= 1 (count (dtype-base/shape rhs#))))
       (throw (ex-info "Dot product called incorrectly"
                       {:lhs-shape (dtype-base/shape lhs#)
                        :rhs-shape (dtype-base/shape rhs#)})))
     (reduce-op/dot-product
      options#
      (tensor->base-buffer-type lhs#)
      (tensor->base-buffer-type rhs#)
      bin-op#
      reduce-op#)))


(def all-tensor-combos
  [[:tensor :tensor]
   [:tensor :dense]
   [:dense :tensor]
   [:tensor :sparse]
   [:sparse :tensor]])


(defmacro impl-all-tensor-combos
  [target-macro]
  `(do
     ~@(->> all-tensor-combos
            (map (fn [combo]
                   `(~target-macro ~combo))))))


(impl-all-tensor-combos impl-dot-product)



(defmulti matrix-matrix-dispatch
  (fn [_alpha lhs rhs bin-op reduce-op _options]
    [(tensor-buffer-type lhs)
     (tensor-buffer-type rhs)
     (dtype-base/op-name bin-op)
     (dtype-base/op-name reduce-op)]))

(defn mmul-check
  [lhs rhs]
  (let [lhs-shape (dtype/shape lhs)
        rhs-shape (dtype/shape rhs)
        rhs-shape (if (= 1 (count rhs-shape))
                    [(first rhs-shape) 1]
                    rhs-shape)]
    (when-not (and (= 2 (count lhs-shape))
                   (= 2 (count rhs-shape)))
      (throw (ex-info "Both items must have shape count 2" {})))
    (when-not (= (second lhs-shape) (first rhs-shape))
      (throw (ex-info "Inner dimensions don't match"
                      {:lhs-shape lhs-shape
                       :rhs-shape rhs-shape})))
    [lhs-shape rhs-shape]))


(defn- default-matrix-matrix
  [alpha lhs rhs bin-op reduce-op options]
  (let [[lhs-shape rhs-shape] (mmul-check lhs rhs)
        lhs-rows (->> (range (first lhs-shape))
                      (mapv #(select lhs % :all)))
        rhs-trans (-> (transpose rhs [1 0])
                      (tensor-force))
        rhs-columns (->> (range (second rhs-shape))
                         (mapv #(select rhs-trans % :all)))
        result-container-type (or (:container-type options)
                                  :list)
        datatype (or (:datatype options)
                     (dtype/get-datatype lhs))
        new-tens (construct-tensor
                  (->> (for [row lhs-rows
                             col rhs-columns]
                         [row col])
                       (pmap #(dtype-fn/dot-product (first %)
                                                    (second %)
                                                    bin-op reduce-op
                                                    {:datatype datatype}))
                       (dtype/make-container result-container-type datatype))
                  (dims/dimensions [(first lhs-shape) (second rhs-shape)]))]
    (cond->> new-tens
      alpha
      (dtype-fn/apply-binary-op {:datatype datatype} bin-op alpha))))


(defmethod matrix-matrix-dispatch :default
  [alpha lhs rhs bin-op reduce-op options]
  (default-matrix-matrix alpha lhs rhs bin-op reduce-op options))


(defn- external-force-dense
  "Extern fn calls can take any stride order but they cannot take
  offsets or a reader chain."
  [tens]
  (let [dimensions (tens-proto/dimensions tens)
        any-offsets? (not (every? #(= 0 %) (:offsets dimensions)))
        nio-access? (dtype-proto/as-nio-buffer (tens-proto/buffer tens))
        broadcast? (broadcast? tens)
        min-stride (apply min (get dimensions :strides))
        tens-shape (dtype/shape tens)]
    (if (or any-offsets?
            (not nio-access?)
            broadcast?
            ;;Matrixes can only have a minimum stride of 1.
            ;;Vectors can have any stride, however.
            (and (= 2 (count tens-shape))
                 (not= 1 min-stride)))
      (clone tens :container-type :native-buffer)
      tens)))

(defn blas-matrix-matrix
  [alpha lhs rhs bin-op reduce-op options]
    (let [lhs-dtype (dtype/get-datatype lhs)
          rhs (ensure-tensor rhs)]
    (if (and (or (= lhs-dtype :float32)
                 (= lhs-dtype :float64))
             (blas/has-blas?))
      (let [[lhs-shape rhs-shape] (mmul-check lhs rhs)
            ;;It is worth it to copy because copy is a O(N) while matrix*matrix is
            ;;O(N^3).
            lhs (external-force-dense lhs)
            rhs (external-force-dense rhs)
            alpha (if alpha (double alpha) 1.0)
            beta 0.0
            C (new-tensor [(first lhs-shape) (second rhs-shape)]
                          :datatype lhs-dtype
                          :container-type :typed-buffer)
            lhs-dims (tens-proto/dimensions lhs)
            rhs-dims (tens-proto/dimensions rhs)
            C-dims (tens-proto/dimensions C)
            lhs-strides (get lhs-dims :strides)
            rhs-strides (get rhs-dims :strides)
            lhs-min-stride (int (apply min lhs-strides))
            rhs-min-stride (int (apply min rhs-strides))
            lhs-max-stride (int (apply max lhs-strides))
            rhs-max-stride (int (apply max rhs-strides))
            c-max-stride (int (apply max (get C-dims :strides)))
            lhs-trans? (= lhs-min-stride (first lhs-strides))
            rhs-trans? (= rhs-min-stride (first rhs-strides))
            gemv? (= 1 (second rhs-shape))]
        (if gemv?
          ((case lhs-dtype
             :float32 blas/cblas_sgemv
             :float64 blas/cblas_dgemv)
           :row-major lhs-trans?
           (first lhs-shape)  (second lhs-shape)
           alpha
           (tens-proto/buffer lhs) lhs-max-stride
           (tens-proto/buffer rhs) rhs-min-stride
           beta (tens-proto/buffer C) 1)
          ((case lhs-dtype
             :float32 blas/cblas_sgemm
             :float64 blas/cblas_dgemm)
           :row-major lhs-trans? rhs-trans?
           (first lhs-shape) (second rhs-shape) (first rhs-shape)
           alpha
           (tens-proto/buffer lhs)
           lhs-max-stride
           (tens-proto/buffer rhs)
           rhs-max-stride
           beta
           (tens-proto/buffer C)
           c-max-stride))
        C)
      (default-matrix-matrix alpha lhs rhs bin-op reduce-op options))))


(defmethod matrix-matrix-dispatch [:dense :dense :* :+]
  [alpha lhs rhs bin-op reduce-op options]
  (blas-matrix-matrix alpha lhs rhs bin-op reduce-op options))


(defmethod matrix-matrix-dispatch [:sparse :dense :* :+]
  [alpha lhs rhs bin-op reduce-op options]
  (blas-matrix-matrix alpha lhs rhs bin-op reduce-op options))


(defmethod matrix-matrix-dispatch [:dense :sparse :* :+]
  [alpha lhs rhs bin-op reduce-op options]
  (blas-matrix-matrix alpha lhs rhs bin-op reduce-op options))


(defmethod matrix-matrix-dispatch [:sparse :sparse :* :+]
  [alpha lhs rhs bin-op _reduce-op options]
  (let [op-datatype (or (:datatype options)
                        (dtype/get-datatype lhs))
        [lhs-shape rhs-shape] (mmul-check lhs rhs)
        ;;Simplify as much as possible.
        lhs (tensor-force lhs)
        rhs-trans (tensor-force (transpose rhs [1 0]))
        lhs-row-indexes (->> lhs
                             sparse-proto/index-seq
                             (map (comp first :global-dims))
                             distinct)
        rhs-col-indexes (->> rhs-trans
                             sparse-proto/index-seq
                             (map (comp first :global-dims))
                             distinct)
        result-rows (int (first lhs-shape))
        result-columns (int (second rhs-shape))
        new-tens (new-tensor [result-rows result-columns]
                             :datatype op-datatype
                             :container-type :sparse)
        tens-writer (dtype/->writer new-tens op-datatype)
        rhs-columns (mapv  #(vector % (select rhs-trans % :all))
                           rhs-col-indexes)]
    ;;do the thing
    (->> (for [row-index lhs-row-indexes
               [col-index column] rhs-columns]
           (let [row-index (int row-index)
                 col-index (int col-index)]
             (tens-writer (+ (* row-index result-columns)
                             col-index)
                          (dtype-fn/dot-product (select lhs row-index :all)
                                                column))))
         (pmap identity)
         dorun)
    (cond->> new-tens
      alpha
      (dtype-fn/apply-binary-op {:datatype op-datatype} bin-op alpha))))


;;Object overrides we can now do because we have a tensor definition.
(extend-type Object
  tens-proto/PToTensor
  (convertible-to-tensor? [item]
    (or (dtype-proto/convertible-to-buffer-desc? item)
        (dtype-proto/convertible-to-reader? item)))
  (convert-to-tensor [item]
    (cond
      (dtype-proto/convertible-to-buffer-desc? item)
      (-> (dtype-proto/->buffer-descriptor item)
          (buffer-descriptor->tensor))
      (dtype-proto/convertible-to-reader? item)
      (let [item-shape (dtype-proto/shape item)
            shape-ecount (apply * item-shape)
            item-reader (dtype/->reader item)
            reader-ecount (dtype/ecount item-reader)]
        (when (= shape-ecount reader-ecount)
          (construct-tensor item
                            (dims/dimensions (dtype/shape item)))))
      :else
      (throw (Exception. "Item is not convertible to tensor.")))))
