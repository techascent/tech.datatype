(ns tech.sparse.sparse-base
  (:require [tech.datatype.reader :as reader]
            [tech.datatype :as dtype]
            [tech.datatype.binary-search :as dtype-search]
            [tech.datatype.unary-op :as unary-op]
            [tech.datatype.binary-op :as binary-op]
            [tech.datatype.reduce-op :as reduce-op]
            [tech.datatype.iterator :as iterator]
            [tech.datatype.boolean-op :as boolean-op]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting :as casting]
            [tech.datatype.typecast :as typecast]
            [tech.datatype.base :as dtype-base]
            [tech.sparse.protocols :as sparse-proto]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.functional.impl :as impl]
            [tech.datatype.functional :as functional]
            [tech.datatype.argsort :as argsort]
            [clojure.core.matrix.protocols :as mp]))


(defrecord IndexSeqRec [^long data-index ^long global-index])


(defn get-index-seq
  "Given the offset and stride of the buffer
  return an index sequence that contains a sequence of
  tuples that contain"
  [data-stride index-iterable]
  (let [data-stride (int data-stride)
        index-seq (->> (dtype/->iterable-of-type index-iterable :int32)
                       (map-indexed #(->IndexSeqRec %1 %2)))]

    (if (= 1 data-stride)
      index-seq
      (->> index-seq
           ;; only return indexes which are commensurate with the stride.
           (map (fn [record]
                  (let [global-index (int (:global-index record))]
                    (when (= 0 (rem global-index data-stride))
                      (assoc record :global-index
                             (quot global-index data-stride))))))
           (remove nil?)))))


(defn index-seq->readers
  [index-seq & [data-reader]]
  (merge {:indexes (impl/->reader (map :global-index index-seq) :int32)}
         (when data-reader
           {:data (reader/make-indexed-reader
                   (impl/->reader (map :data-index index-seq) :int32)
                   (impl/->reader data-reader)
                   {})})))


(defn index-seq->iterables
  [index-seq & [data-reader]]
  (merge {:indexes (map :global-index index-seq)}
         (when data-reader
           {:data (reader/make-iterable-indexed-iterable
                   (map :data-index index-seq)
                   (impl/->iterable data-reader))})))


(defn make-sparse-value
  [datatype]
  (case (casting/host-flatten datatype)
    :int8 (casting/datatype->sparse-value :int8)
    :int16 (casting/datatype->sparse-value :int16)
    :int32 (casting/datatype->sparse-value :int32)
    :int64 (casting/datatype->sparse-value :int64)
    :float32 (casting/datatype->sparse-value :float32)
    :float64 (casting/datatype->sparse-value :float64)
    :boolean (casting/datatype->sparse-value :boolean)
    :object (casting/datatype->sparse-value :object)))


(declare make-sparse-reader)



(defmacro make-indexed-data-reader
  [datatype]
  `(fn [index-reader# data-reader# n-elems# zero-val#]
     (let [local-data-reader# (typecast/datatype->reader ~datatype data-reader# true)
           n-elems# (int n-elems#)
           idx-count# (dtype-base/ecount index-reader#)
           zero-val# (casting/datatype->cast-fn :unknown ~datatype zero-val#)]
       (reify
         ~(typecast/datatype->reader-type datatype)
         (getDatatype [reader#] (dtype-proto/get-datatype data-reader#))
         (size [reader#] n-elems#)
         (read [reader# idx#]
           (let [[found?# data-idx#] (dtype-search/binary-search
                                      index-reader# (int idx#) {})]
             (if found?#
               (.read local-data-reader# (int data-idx#))
               zero-val#)))
         (invoke [reader# idx#]
           (.read reader# (int idx#)))
         (iterator [reader#] (typecast/reader->iterator reader#))
         sparse-proto/PSparse
         (index-seq [reader#]
           (get-index-seq 1 index-reader#))
         (sparse-value [reader#] zero-val#)
         (sparse-ecount [reader#] (- n-elems# idx-count#))
         (set-stride [item# new-stride#]
           (when-not (= 0 (rem n-elems# new-stride#))
             (throw (ex-info (format
                              "Element count %s is not commensurate with stride %s."
                              n-elems# new-stride#)
                             {})))
           (if (= 1 (int new-stride#))
             item#
             (let [new-idx-seq# (get-index-seq new-stride# index-reader#)
                   {indexes# :indexes
                    data# :data} (index-seq->readers new-idx-seq# data-reader#)]
               (make-sparse-reader indexes# data#
                                   (quot n-elems# (int new-stride#))
                                   :sparse-value zero-val#))))
         (stride [item#] 1)
         (readers [item#]
           {:indexes index-reader#
            :data data-reader#})
         (iterables [item#]
           (sparse-proto/readers item#))
         sparse-proto/PToSparseReader
         (->sparse-reader [item#] item#)
         dtype-proto/PBuffer
         (sub-buffer [item# offset# length#]
           (when-not (<= (+ offset# length#)
                         n-elems#)
             (throw (ex-info (format "Sub-buffer out of range: %s > %s"
                                     (+ offset# length#)
                                     n-elems#)
                             {})))
           (let [start-elem# (int (second (dtype-search/binary-search
                                           index-reader# offset# {})))
                 end-elem# (int (second (dtype-search/binary-search
                                         index-reader# (+ offset# length#) {})))
                 sub-len# (- end-elem# start-elem#)
                 new-idx-reader# (->> (dtype-proto/sub-buffer
                                       index-reader# start-elem# sub-len#)
                                      (unary-op/unary-reader-map
                                       {:datatype :int32}
                                       (unary-op/make-unary-op
                                        :int32 (- ~'arg offset#))))
                 new-data-reader# (dtype-proto/sub-buffer
                                   data-reader# start-elem# sub-len#)]
             (make-sparse-reader new-idx-reader# new-data-reader# length#
                                 :sparse-value zero-val#)))
         ;;These two queries are irrelevant with readers
         (alias? [lhs# rhs#] false)
         (partially-alias? [lhs# rhs#] false)))))


(defmacro make-indexed-reader-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-indexed-data-reader ~dtype)])]
        (into {})))


(def indexed-reader-table (make-indexed-reader-table))


(defn make-sparse-reader
  "A sparse reader has no stride or offset but satisfies all of the necessary protocols.
  It does not satisfy the writer or mutable protocols."
  [index-reader data-reader n-elems & {:keys [datatype
                                              sparse-value]}]
  (let [datatype (casting/safe-flatten
                  (or datatype (dtype-base/get-datatype data-reader)))
        create-fn (get indexed-reader-table datatype)
        sparse-value (or sparse-value (make-sparse-value datatype))]
    (create-fn (impl/->reader index-reader datatype)
               (impl/->reader data-reader datatype)
               n-elems
               sparse-value)))


(defmacro make-sparse-data-filter
  [datatype]
  `(fn [data-seq# sparse-value#]
     (let [sparse-value# (casting/datatype->cast-fn :unkown ~datatype sparse-value#)]
       (boolean-op/boolean-unary-iterable
        {:datatype ~datatype}
        (boolean-op/make-boolean-unary-op ~datatype (not= ~'arg sparse-value#))
        data-seq#))))


(defmacro make-sparse-data-filter-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes]
             [dtype `(make-sparse-data-filter ~dtype)])]
        (into {})))


(def sparse-data-filter-table (make-sparse-data-filter-table))


(defn data->sparse-reader
  [data-seq {:keys [datatype sparse-value]}]
  (let [datatype (or datatype (dtype-base/get-datatype data-seq))
        sparse-value (or sparse-value (make-sparse-value datatype))
        sparse-filter-fn (get sparse-data-filter-table
                              (casting/safe-flatten datatype))
        ;;cache the filter, we will need ecount anyway
        filter-iter (->> (sparse-filter-fn data-seq sparse-value)
                         (dtype-base/make-container :list :boolean))
        indexes (->> (iterator/iterable-mask {:datatype :int32} filter-iter (range))
                     (dtype-base/make-container :list :int32))
        data (->> (iterator/iterable-mask {:datatype datatype} filter-iter data-seq)
                  (dtype-base/make-container :list datatype))]
    (make-sparse-reader indexes data (dtype-base/ecount filter-iter)
                        :datatype datatype
                        :sparse-value sparse-value)))


(defn sparse-unary-map
  [options un-op sparse-item]
  (let [datatype (or (:datatype options)
                     (dtype-base/get-datatype sparse-item))]
    (make-sparse-reader (sparse-proto/index-reader sparse-item)
                        (unary-op/apply-unary-op {} un-op
                                                 (sparse-proto/data-reader sparse-item))
                        (dtype-base/ecount sparse-item)
                        :sparse-value
                        ((dtype-proto/->unary-op un-op datatype false)
                         (sparse-proto/sparse-value sparse-item)))))


(defn sparse-boolean-unary-map
  [options un-op sparse-item]
  (let [datatype (or (:datatype options)
                     (dtype-base/get-datatype sparse-item))]
    (make-sparse-reader (sparse-proto/index-reader sparse-item)
                        (boolean-op/apply-unary-op {} un-op
                                                   (sparse-proto/data-reader
                                                    sparse-item))
                        (dtype-base/ecount sparse-item)
                        :sparse-value
                        ((dtype-proto/->unary-boolean-op un-op datatype false)
                         (sparse-proto/sparse-value sparse-item))
                        :datatype :boolean)))


(defmacro make-sparse-union-reader
  [input-datatype output-datatype]
  `(fn [~'bin-op sparse-lhs# sparse-rhs# ~'unchecked?
        final-datatype# op-type#]
     (let [lhs-indexes# (typecast/datatype->iter
                         :int32 (sparse-proto/index-reader sparse-lhs#))
           rhs-indexes# (typecast/datatype->iter
                         :int32 (sparse-proto/index-reader sparse-rhs#))
           lhs-values# (typecast/datatype->iter
                        ~input-datatype (sparse-proto/data-reader sparse-lhs#)
                        ~'unchecked?)
           rhs-values# (typecast/datatype->iter
                        ~input-datatype (sparse-proto/data-reader sparse-rhs#)
                        ~'unchecked?)
           bin-op# ~(if (= :boolean output-datatype)
                      `(boolean-op/datatype->boolean-binary ~input-datatype
                                                            ~'bin-op ~'unchecked?)
                      `(binary-op/datatype->binary-op ~input-datatype
                                                      ~'bin-op ~'unchecked?))
           new-indexes# (dtype-proto/make-container :list :int32 0 {})
           new-data# (dtype-proto/make-container :list ~output-datatype 0 {})
           idx-mutable# (typecast/datatype->mutable :int32 new-indexes#)
           value-mutable# (typecast/datatype->mutable ~output-datatype new-data#)
           lhs-zero#  (casting/datatype->cast-fn
                       :unknown
                       ~input-datatype
                       (sparse-proto/sparse-value sparse-lhs#))
           rhs-zero# (casting/datatype->cast-fn
                      :unknown
                      ~input-datatype
                       (sparse-proto/sparse-value sparse-rhs#))
           sparse-value# (.op bin-op# lhs-zero# rhs-zero#)
           op-elem-count# (min (dtype-base/ecount sparse-lhs#)
                               (dtype-base/ecount sparse-rhs#))
           union?# (= op-type# :union)]
       (loop [lhs-valid?# (.hasNext lhs-indexes#)
              rhs-valid?# (.hasNext rhs-indexes#)]
         (when (or (and union?# (or lhs-valid?# rhs-valid?#))
                   (and lhs-valid?# rhs-valid?#))
           (cond
             (and lhs-valid?# rhs-valid?#)
             (let [lhs-idx# (.current lhs-indexes#)
                   rhs-idx# (.current rhs-indexes#)]
               (cond
                 (= lhs-idx# rhs-idx#)
                 (let [op-result# (.op bin-op#
                                       (typecast/datatype->iter-next-fn
                                        ~input-datatype
                                        lhs-values#)
                                       (typecast/datatype->iter-next-fn
                                        ~input-datatype
                                        rhs-values#))]
                   (.nextInt lhs-indexes#)
                   (.nextInt rhs-indexes#)
                   (when-not (= op-result# sparse-value#)
                     (.append idx-mutable# lhs-idx#)
                     (.append value-mutable# op-result#)))

                 (< lhs-idx# rhs-idx#)
                 (let [op-result# (.op bin-op#
                                       (typecast/datatype->iter-next-fn
                                        ~input-datatype
                                        lhs-values#)
                                       rhs-zero#)
                       op-idx# (.nextInt lhs-indexes#)]
                   (when (and union?# (not= op-result# sparse-value#))
                     (.append idx-mutable# op-idx#)
                     (.append value-mutable# op-result#)))

                 :else
                 (let [op-result# (.op bin-op#
                                       lhs-zero#
                                       (typecast/datatype->iter-next-fn
                                        ~input-datatype
                                        rhs-values#))
                       op-idx# (.nextInt rhs-indexes#)]
                   (when (and union?# (not= op-result# sparse-value#))
                     (.append idx-mutable# op-idx#)
                     (.append value-mutable# op-result#)))))
             lhs-valid?#
             (while (.hasNext lhs-indexes#)
               (let [op-result# (.op bin-op#
                                     (typecast/datatype->iter-next-fn
                                      ~input-datatype
                                      lhs-values#)
                                     rhs-zero#)
                     op-idx# (.nextInt lhs-indexes#)]
                 (when-not (= op-result# sparse-value#)
                   (.append idx-mutable# op-idx#)
                   (.append value-mutable# op-result#))))
             :else
             (while (.hasNext rhs-indexes#)
               (let [op-result# (.op bin-op#
                                     lhs-zero#
                                     (typecast/datatype->iter-next-fn
                                      ~input-datatype
                                      rhs-values#))
                     op-idx# (.nextInt rhs-indexes#)]
                 (when-not (= op-result# sparse-value#)
                   (.append idx-mutable# op-idx#)
                   (.append value-mutable# op-result#)))))
           (recur (.hasNext lhs-indexes#)
                  (.hasNext rhs-indexes#))))
       (make-sparse-reader new-indexes# new-data# op-elem-count#
                           :datatype final-datatype#
                           :sparse-value sparse-value#))))


(defmacro make-sparse-binary-op-table
  []
  `(->> [~@(for [dtype casting/base-host-datatypes
                 bool-op? [true false]]
             (let [output-dtype (if bool-op?
                                  :boolean
                                  dtype)]
               [[dtype output-dtype] `(make-sparse-union-reader ~dtype ~output-dtype)]))]
        (into {})))

(def sparse-binary-op-table (make-sparse-binary-op-table))



(defn unordered-global-space->ordered-local-space
  [new-indexes new-data b-offset b-stride indexes-in-order?]
  (let [b-offset (int b-offset)
        b-stride (int b-stride)
        [new-indexes new-values] (if-not indexes-in-order?
                                   (let [ordered-indexes (argsort/argsort new-indexes {:datatype :int32})]
                                     [(reader/make-indexed-reader ordered-indexes new-indexes)
                                      (reader/make-indexed-reader ordered-indexes new-data)])
                                   [new-indexes new-data])
        new-indexes (unary-op/unary-reader-map
                     {:datatype :int32}
                     (unary-op/make-unary-op :int32 (-> (* arg b-stride)
                                                        (+ b-offset)))
                     new-indexes)]
    {:indexes new-indexes
     :data new-data}))




(defn- bound-sparse-item
  [sparse-item start-idx-val end-idx-val]
  (let [start-idx-val (int start-idx-val)
        item-len (- (int end-idx-val) start-idx-val)
        sub-item (dtype-proto/sub-buffer sparse-item (int start-idx-val) item-len)]
    (make-sparse-reader (unary-op/unary-reader-map
                         {:datatype :int32}
                         (unary-op/make-unary-op :int32 (+ arg start-idx-val))
                         (sparse-proto/index-reader sub-item))
                        (sparse-proto/data-reader sub-item)
                        (dtype-base/ecount sparse-item))))


(defn dense-sparse-intersection
  "Make a new sparse item from the dense item that has the indexes from the original
  sparse item."
  [sparse-item dense-item & {:keys [datatype sparse-value] :as options}]
  (let [sparse-indexes (sparse-proto/index-reader sparse-item)]
    (make-sparse-reader sparse-indexes
                        (reader/make-indexed-reader
                         sparse-indexes
                         dense-item
                         {})
                        (dtype-base/ecount sparse-item)
                        :datatype datatype
                        :sparse-value sparse-value)))


(defn sparse-binary-map
  [options bin-op sparse-lhs sparse-rhs]
  (let [datatype (or (:datatype options)
                     (dtype-base/get-datatype sparse-lhs))
        flat-dtype (casting/safe-flatten datatype)
        union-fn (get sparse-binary-op-table [datatype datatype])
        sparse-map-type (or (:sparse-map-type options)
                            :union)
        [sparse-lhs sparse-rhs]
        (if (= :intersection sparse-map-type)
          (let [lhs-indexes (sparse-proto/index-reader sparse-lhs)
                rhs-indexes (sparse-proto/index-reader sparse-rhs)
                [lhs-start-val lhs-end-val]
                (dtype-base/item-inclusive-range lhs-indexes)
                [rhs-start-val rhs-end-val]
                (dtype-base/item-inclusive-range rhs-indexes)
                start-val (max (int rhs-start-val) (int lhs-start-val))
                end-val (+ 1 (min (int rhs-end-val) (int lhs-end-val)))]
            [(bound-sparse-item sparse-lhs start-val end-val)
             (bound-sparse-item sparse-rhs start-val end-val)])
          [sparse-lhs sparse-rhs])]
    (union-fn bin-op sparse-lhs sparse-rhs (:unchecked? options)
              datatype sparse-map-type)))


(defn sparse-boolean-binary-map
  [options bin-op sparse-lhs sparse-rhs]
  (let [datatype (or (:datatype options)
                     (dtype-base/get-datatype sparse-lhs))
        flat-dtype (casting/safe-flatten datatype)
        union-fn (get sparse-binary-op-table [datatype :boolean])]
    (union-fn bin-op sparse-lhs sparse-rhs (:unchecked? options) :boolean)))


(defn sparse-elemwise-*
  [options lhs rhs]
  (when-not (= (dtype-base/ecount lhs)
               (dtype-base/ecount rhs))
    (throw (ex-info (format "lhs ecount (%s) does not match rhs ecount (%s)"
                            (dtype-base/ecount lhs)
                            (dtype-base/ecount rhs))
                    {})))
  (let [sparse-lhs (when (satisfies? sparse-proto/PToSparseReader lhs)
                     (sparse-proto/->sparse-reader lhs))
        sparse-rhs (when (satisfies? sparse-proto/PToSparseReader rhs)
                     (sparse-proto/->sparse-reader rhs))
        any-dense? (or (nil? sparse-lhs)
                       (nil? sparse-rhs))
        op-datatype (or (:datatype options) (dtype-base/get-datatype lhs))
        options (assoc options :datatype op-datatype)
        bin-op (:* binary-op/builtin-binary-ops)]
    (if (or sparse-lhs sparse-rhs)
      (let [sparse-lhs-val (when sparse-lhs
                             (casting/cast (sparse-proto/sparse-value sparse-lhs)
                                           op-datatype))
            sparse-rhs-val (when sparse-rhs
                             (casting/cast (sparse-proto/sparse-value sparse-rhs)
                                           op-datatype))
            sparse-value (make-sparse-value op-datatype)
            sparse-zero? (or (= (make-sparse-value op-datatype) sparse-lhs-val)
                             (= (make-sparse-value op-datatype) sparse-rhs-val))]
        (if sparse-zero?
          (let [sparse-lhs (if sparse-lhs
                             sparse-lhs
                             (dense-sparse-intersection sparse-rhs lhs))
                sparse-rhs (if sparse-rhs
                             sparse-rhs
                             (dense-sparse-intersection sparse-lhs rhs))]
            (if any-dense?
              (do
                (make-sparse-reader (sparse-proto/index-reader sparse-lhs)
                                    (binary-op/binary-reader-map
                                     options bin-op
                                     (sparse-proto/data-reader sparse-lhs)
                                     (sparse-proto/data-reader sparse-rhs))
                                    (dtype-base/ecount sparse-lhs)
                                    :datatype op-datatype))
              (sparse-binary-map options bin-op
                                 sparse-lhs sparse-rhs)))
          (binary-op/binary-reader-map options bin-op lhs rhs)))
      (binary-op/binary-reader-map options bin-op lhs rhs))))


(defn sparse-reduce-+
  [options sparse-vec]
  (let [base-value (functional/* (sparse-proto/sparse-value sparse-vec)
                                 (sparse-proto/sparse-ecount sparse-vec))
        item-dtype (or (:datatype options)
                       (dtype-base/get-datatype sparse-vec))]
    (-> (functional/+ base-value (reduce-op/apply-reduce-op
                                  (assoc options :datatype item-dtype)
                                  (:+ binary-op/builtin-binary-ops)
                                  (sparse-proto/data-reader sparse-vec)))
        (casting/cast item-dtype))))


(defn sparse-reduce-*
  [options sparse-vec]
  (let [n-sparse (long (sparse-proto/sparse-ecount sparse-vec))
        item-dtype (or (:datatype options)
                       (dtype-base/get-datatype sparse-vec))
        sparse-val (sparse-proto/sparse-value sparse-vec)]
    (cond
      (and (> n-sparse 0)
           (= (make-sparse-value item-dtype)
              (sparse-proto/sparse-value sparse-vec)))
      (sparse-proto/sparse-value sparse-vec)
      (and (casting/numeric-type? item-dtype)
           (= 1.0 (dtype/cast sparse-val :float64)))
      (reduce-op/apply-reduce-op
       (assoc options :datatype item-dtype)
       (:+ binary-op/builtin-binary-ops)
       (sparse-proto/data-reader sparse-vec))
      :else
      (reduce-op/apply-reduce-op
       (assoc options :datatype item-dtype)
       (:+ binary-op/builtin-binary-ops)
       sparse-vec))))


(defn sparse-dot
  [options lhs rhs]
  (->> (sparse-elemwise-* options lhs rhs)
       (sparse-reduce-+ options)))


(defn sparse-magnitude-sq
  [options lhs]
  (->> (sparse-unary-map options (:sq unary-op/builtin-unary-ops) lhs)
       (sparse-reduce-+ options)))

(defn sparse-distance-sq
  [options lhs rhs]
  (->> (sparse-binary-map options (:- binary-op/builtin-binary-ops) lhs rhs)
       (sparse-magnitude-sq options)))
