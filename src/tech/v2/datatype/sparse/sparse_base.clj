(ns tech.v2.datatype.sparse.sparse-base
  (:require [tech.v2.datatype.readers.indexed :as indexed-reader]
            [tech.v2.datatype.sparse.reader
             :refer [make-sparse-value
                     make-sparse-reader]]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.binary-op :as binary-op]
            [tech.v2.datatype.reduce-op :as reduce-op]
            [tech.v2.datatype.iterable.masked :as masked-iterable]
            [tech.v2.datatype.boolean-op :as boolean-op]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.sparse.protocols :as sparse-proto]
            [tech.v2.datatype.argsort :as argsort]
            [tech.v2.datatype.functional.impl :as impl]
            [tech.v2.datatype.functional :as functional]))



(defmacro make-sparse-data-filter
  [datatype]
  `(fn [data-seq# sparse-value#]
     (let [sparse-value# (casting/datatype->cast-fn :unkown ~datatype sparse-value#)]
       (boolean-op/boolean-unary-iterable
        ~datatype
        (not= ~'x sparse-value#)
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
        indexes (->> (masked-iterable/iterable-mask {:datatype :int32}
                                                    filter-iter (range))
                     (dtype-base/make-container :list :int32))
        data (->> (masked-iterable/iterable-mask {:datatype datatype}
                                                 filter-iter data-seq)
                  (dtype-base/make-container :list datatype))]
    (make-sparse-reader indexes data (dtype-base/ecount filter-iter)
                        :datatype datatype
                        :sparse-value sparse-value)))


(defn sparse-unary-map
  [options un-op sparse-item]
  (let [datatype (or (:datatype options)
                     (dtype-base/get-datatype sparse-item))
        sparse-item (sparse-proto/->sparse sparse-item)]
    (make-sparse-reader (sparse-proto/index-reader sparse-item)
                        (impl/apply-unary-op {} un-op
                                             (sparse-proto/data-reader sparse-item))
                        (dtype-base/ecount sparse-item)
                        :sparse-value
                        ((dtype-proto/->unary-op un-op {:datatype datatype})
                         (sparse-proto/sparse-value sparse-item)))))


(defmethod unary-op/unary-reader-map :sparse
  [options un-op sparse-item]
  (sparse-unary-map options un-op sparse-item))


(defn sparse-boolean-unary-map
  [options un-op sparse-item]
  (let [datatype (or (:datatype options)
                     (dtype-base/get-datatype sparse-item))
        sparse-item (sparse-proto/->sparse sparse-item)]
    (make-sparse-reader (sparse-proto/index-reader sparse-item)
                        (impl/apply-unary-boolean-op {:datatype datatype}
                                                     un-op
                                                     (sparse-proto/data-reader
                                                      sparse-item))
                        (dtype-base/ecount sparse-item)
                        :sparse-value (un-op (sparse-proto/sparse-value sparse-item))
                        :datatype :boolean)))


(defmethod boolean-op/boolean-unary-reader-map :sparse
  [options un-op sparse-item]
  (sparse-boolean-unary-map options un-op sparse-item))


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
               [[dtype output-dtype]
                `(make-sparse-union-reader ~dtype ~output-dtype)]))]
        (into {})))

(def sparse-binary-op-table (make-sparse-binary-op-table))

(defn unordered-global-space->ordered-local-space
  [new-indexes new-data b-offset indexes-in-order?]
  (let [b-offset (int b-offset)
        [new-indexes new-values]
        (if-not indexes-in-order?
          (let [ordered-indexes (argsort/argsort new-indexes {:datatype :int32})]
            [(indexed-reader/make-indexed-reader ordered-indexes new-indexes {})
             (indexed-reader/make-indexed-reader ordered-indexes new-data {})])
          [new-indexes new-data])
        new-indexes (unary-op/unary-reader
                     :int32
                     (+ x b-offset)
                     new-indexes)]
    {:indexes new-indexes
     :data new-values}))




(defn- bound-sparse-item
  [sparse-item start-idx-val end-idx-val]
  (let [start-idx-val (int start-idx-val)
        item-len (- (int end-idx-val) start-idx-val)
        sub-item (dtype-proto/sub-buffer sparse-item (int start-idx-val) item-len)]
    (make-sparse-reader (unary-op/unary-reader
                         :int32
                         (+ x start-idx-val)
                         (sparse-proto/index-reader sub-item))
                        (sparse-proto/data-reader sub-item)
                        (dtype-base/ecount sparse-item))))


(defn dense-sparse-intersection
  "Make a new sparse item from the dense item that has the indexes from the original
  sparse item."
  [sparse-item dense-item & {:keys [datatype sparse-value]}]
  (let [sparse-indexes (sparse-proto/index-reader sparse-item)]
    (make-sparse-reader sparse-indexes
                        (indexed-reader/make-indexed-reader
                         sparse-indexes
                         dense-item
                         {})
                        (dtype-base/ecount sparse-item)
                        :datatype datatype
                        :sparse-value sparse-value)))


(defn sparse-binary-map
  [options bin-op sparse-lhs sparse-rhs]
  (let [sparse-lhs (sparse-proto/->sparse sparse-lhs)
        sparse-rhs (sparse-proto/->sparse sparse-rhs)
        datatype (or (:datatype options)
                     (dtype-base/get-datatype sparse-lhs))
        flat-dtype (casting/safe-flatten datatype)
        union-fn (get sparse-binary-op-table [flat-dtype flat-dtype])
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
        union-fn (get sparse-binary-op-table [flat-dtype :boolean])]
    (union-fn bin-op sparse-lhs sparse-rhs (:unchecked? options) :boolean)))



(defn sparse-elemwise-*
  [options lhs rhs]
  (when-not (= (dtype-base/ecount lhs)
               (dtype-base/ecount rhs))
    (throw (ex-info (format "lhs ecount (%s) does not match rhs ecount (%s)"
                            (dtype-base/ecount lhs)
                            (dtype-base/ecount rhs))
                    {})))
  (let [sparse-lhs (when (sparse-proto/sparse-convertible? lhs)
                     (sparse-proto/->sparse lhs))
        sparse-rhs (when (sparse-proto/sparse-convertible? rhs)
                     (sparse-proto/->sparse rhs))
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
            sparse-zero? (or (= sparse-value sparse-lhs-val)
                             (= sparse-value sparse-rhs-val))]
        (if sparse-zero?
          (let [sparse-lhs (if sparse-lhs
                             sparse-lhs
                             (dense-sparse-intersection sparse-rhs lhs))
                sparse-rhs (if sparse-rhs
                             sparse-rhs
                             (dense-sparse-intersection sparse-lhs rhs))]
            (if any-dense?
              (make-sparse-reader (sparse-proto/index-reader sparse-lhs)
                                  (binary-op/binary-reader-map
                                   options bin-op
                                   (sparse-proto/data-reader sparse-lhs)
                                   (sparse-proto/data-reader sparse-rhs))
                                  (dtype-base/ecount sparse-lhs)
                                  :datatype op-datatype)
              (sparse-binary-map options bin-op
                                 sparse-lhs sparse-rhs)))
          (binary-op/binary-reader-map options bin-op lhs rhs)))
      (binary-op/binary-reader-map options bin-op lhs rhs))))


(defn general-sparse-reader-map
  [options bin-op lhs rhs]
  (cond
    (= :* (dtype-base/op-name bin-op))
    (sparse-elemwise-* options lhs rhs)
    (and (= :sparse (dtype-base/buffer-type lhs))
         (= :sparse (dtype-base/buffer-type rhs)))
    (sparse-binary-map options bin-op lhs rhs)
    :else
    (binary-op/default-binary-reader-map options bin-op lhs rhs)))


(defmethod binary-op/binary-reader-map [:sparse :sparse]
  [options bin-op lhs rhs]
  (general-sparse-reader-map options bin-op lhs rhs))


(defmethod binary-op/binary-reader-map [:dense :sparse]
  [options bin-op lhs rhs]
  (general-sparse-reader-map options bin-op lhs rhs))


(defmethod binary-op/binary-reader-map [:sparse :dense]
  [options bin-op lhs rhs]
  (general-sparse-reader-map options bin-op lhs rhs))


(defmethod boolean-op/boolean-binary-reader-map [:sparse :sparse]
  [options bin-op sparse-lhs sparse-rhs]
  (sparse-boolean-binary-map options bin-op sparse-lhs sparse-rhs))


(defn sparse-reduce-+
  [options sparse-vec]
  (let [base-value (functional/* (sparse-proto/sparse-value sparse-vec)
                                 (sparse-proto/sparse-ecount sparse-vec))
        item-dtype (or (:datatype options)
                       (dtype-base/get-datatype sparse-vec))
        rhs (impl/apply-reduce-op
             (assoc options :datatype item-dtype)
             (:+ binary-op/builtin-binary-ops)
             (sparse-proto/data-reader sparse-vec))]
    (-> (functional/+ base-value rhs)
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
           (= 1.0 (casting/cast sparse-val :float64)))
      (impl/apply-reduce-op
       (assoc options :datatype item-dtype)
       (:* binary-op/builtin-binary-ops)
       (sparse-proto/data-reader sparse-vec))
      :else
      (impl/apply-reduce-op
       (assoc options :datatype item-dtype)
       (:* binary-op/builtin-binary-ops)
       sparse-vec))))


(defmethod reduce-op/iterable-reduce-map :sparse
  [options reduce-op values]
  (case (dtype-base/op-name reduce-op)
    :+ (sparse-reduce-+ options values)
    :* (sparse-reduce-* options values)
    (reduce-op/default-iterable-reduce-map options reduce-op values)))


(defn default-sparse-dot-product
  [options lhs rhs bin-op reduce-op]
  (let [bin-map (binary-op/binary-reader-map options bin-op lhs rhs)]
    (reduce-op/iterable-reduce-map options reduce-op bin-map)))


(defmethod reduce-op/dot-product [:sparse :dense]
  [options lhs rhs bin-op reduce-op]
  (default-sparse-dot-product options lhs rhs bin-op reduce-op))


(defmethod reduce-op/dot-product [:dense :sparse]
  [options lhs rhs bin-op reduce-op]
  (default-sparse-dot-product options lhs rhs bin-op reduce-op))


(defmethod reduce-op/dot-product [:sparse :sparse]
  [options lhs rhs bin-op reduce-op]
  (default-sparse-dot-product options lhs rhs bin-op reduce-op))
