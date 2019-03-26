(ns tech.datatype.iterator
  (:require [tech.datatype.typecast :as typecast]
            [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.nio-access
             :refer [unchecked-full-cast
                     checked-full-write-cast]])
  (:import [tech.datatype ObjectIter ByteIter ShortIter
            IntIter LongIter FloatIter DoubleIter
            BooleanIter IteratorObjectIter]))



(defmacro make-marshal-iterator
  [src-dtype intermediate-dtype dest-dtype public-dtype src-iterator unchecked?]
  `(if ~unchecked?
     (reify ~(typecast/datatype->iter-type intermediate-dtype)
       (getDatatype [item#] ~public-dtype)
       (hasNext [item#] (.hasNext ~src-iterator))
       (~(typecast/datatype->iter-next-fn-name intermediate-dtype) [item#]
        (let [temp-val#  (typecast/datatype->iter-next-fn ~src-dtype ~src-iterator)]
          (-> temp-val#
              (unchecked-full-cast ~src-dtype
                                   ~intermediate-dtype
                                   ~dest-dtype))))
       (current [item#]
         (-> (.current ~src-iterator)
             (unchecked-full-cast ~src-dtype
                                  ~intermediate-dtype
                                  ~dest-dtype))))
     (reify ~(typecast/datatype->iter-type intermediate-dtype)
       (getDatatype [item#] ~public-dtype)
       (hasNext [item#] (.hasNext ~src-iterator))
       (~(typecast/datatype->iter-next-fn-name intermediate-dtype) [item#]
        (let [temp-val# (typecast/datatype->iter-next-fn ~src-dtype ~src-iterator)]
          (checked-full-write-cast temp-val#
                                   ~src-dtype
                                   ~intermediate-dtype
                                   ~dest-dtype)))
       (current [item#]
         (-> (.current ~src-iterator)
             (checked-full-write-cast ~src-dtype
                                      ~intermediate-dtype
                                      ~dest-dtype))))))

(defmacro make-marshal-iterator-table
  []
  `(->> [~@(for [src-dtype casting/all-host-datatypes
                 dst-dtype casting/base-datatypes]
             [[src-dtype dst-dtype]
              `(fn [iterator# datatype# unchecked?#]
                 (let [iterator# (typecast/datatype->iter ~src-dtype iterator#
                                                          unchecked?#)]
                   (make-marshal-iterator ~src-dtype ~dst-dtype
                                          ~(casting/datatype->safe-host-type
                                            dst-dtype)
                                          datatype#
                                          iterator# unchecked?#)))])]
        (into {})))


(def marshal-iterator-table (make-marshal-iterator-table))


(defn make-marshal-iterator
  [item datatype unchecked?]
  (let [item-dtype (dtype-proto/get-datatype item)]
    (if (= datatype item-dtype)
      item
      (let [iter-fn (get marshal-iterator-table [(casting/flatten-datatype item-dtype)
                                                 (casting/flatten-datatype datatype)])]
        (iter-fn item datatype unchecked?)))))


(extend-type Iterable
  dtype-proto/PToIterable
  (->iterable-of-type [item datatype unchecked?]
    (reify
      Iterable
      (iterator [iter-item]
        (-> (IteratorObjectIter. (.iterator item) :object)
            (make-marshal-iterator datatype unchecked?)))
      dtype-proto/PDatatype
      (get-datatype [item] datatype))))


(defmacro make-const-iter
  [datatype]
  (let [host-type (casting/datatype->safe-host-type datatype)]
    `(fn [item#]
       (let [item# (checked-full-write-cast
                    item# :unknown ~datatype
                    ~host-type)]
         (reify ~(typecast/datatype->iter-type host-type)
           (getDatatype [iter#] ~datatype)
           (hasNext [iter#] true)
           (~(typecast/datatype->iter-next-fn-name host-type) [iter#] item#)
           (current [iter#] item#))))))


(defmacro make-const-iter-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             [dtype `(make-const-iter ~dtype)])]
        (into {})))


(def const-iter-table (make-const-iter-table))


(defn make-const-iterator
  [item datatype]
  (if-let [iter-fn (get const-iter-table (casting/flatten-datatype datatype))]
    (iter-fn item)
    (throw (ex-info (format "Failed to find iter for datatype %s" datatype) {}))))


(defmacro make-masked-iterable-impl
  [datatype]
  `(fn [datatype# values# mask# unchecked?#]
     (reify
       dtype-proto/PDatatype
       (get-datatype [item#] datatype#)
       Iterable
       (iterator [item#]
         (let [values# (typecast/datatype->iter ~datatype values# unchecked?#)
               mask# (typecast/datatype->iter :boolean mask# true)]
           (while (and (.hasNext mask#)
                       (= false (.current mask#)))
             (.nextBoolean mask#)
             (typecast/datatype->iter-next-fn ~datatype values#))
           (reify ~(typecast/datatype->iter-type datatype)
             (hasNext [item#] (and (.hasNext mask#)
                                   (.current mask#)
                                   (.hasNext values#)))
             (~(typecast/datatype->iter-next-fn-name datatype)
              [item#]
              (let [retval# (.current values#)]
                (when (.hasNext mask#)
                  (.nextBoolean mask#)
                  (typecast/datatype->iter-next-fn ~datatype values#)
                  (while (and (.hasNext mask#)
                              (= false (.current mask#)))
                    (.nextBoolean mask#)
                    (typecast/datatype->iter-next-fn ~datatype values#)))
                retval#))
             (current [item#] (.current values#))))))))


(defmacro make-masked-iterable-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             [dtype `(make-masked-iterable-impl ~dtype)])]
        (into {})))


(def masked-iterable-table (make-masked-iterable-table))


(defn iterable-mask
  [{:keys [datatype unchecked?]} mask-iter values]
  (let [datatype (or datatype (dtype-proto/get-datatype values))
        mask-fn (get masked-iterable-table (casting/flatten-datatype datatype))]
    (mask-fn datatype values mask-iter unchecked?)))


(defmacro make-concat-iterable-impl
  [datatype]
  `(fn [datatype# concat-args#]
     (reify
       dtype-proto/PDatatype
       (get-datatype [item#] datatype#)
       Iterable
       (iterator [item#]
         (let [concat-args# (map #(typecast/datatype->iter ~datatype %) concat-args#)
               algo-data# (object-array [(first concat-args#)
                                         (rest concat-args#)])]
           (reify ~(typecast/datatype->iter-type datatype)
             (hasNext [item#]
               (boolean (not= nil (aget algo-data# 0))))
             (~(typecast/datatype->iter-next-fn-name datatype)
              [item#]
              (let [src-iter# (typecast/datatype->fast-iter ~datatype (aget algo-data# 0))
                    retval# (typecast/datatype->iter-next-fn ~datatype src-iter#)]
                (when-not (.hasNext src-iter#)
                  (aset algo-data# 0 (first (aget algo-data# 1)))
                  (aset algo-data# 1 (rest (aget algo-data# 1))))
                retval#))
             (current [item#]
               (let [src-iter# (typecast/datatype->fast-iter ~datatype (aget algo-data# 0))]
                 (.current src-iter#)))))))))


(defmacro make-concat-iterable-table
  []
  `(->> [~@(for [dtype casting/base-datatypes]
             [dtype `(make-concat-iterable-impl ~dtype)])]
        (into {})))


(def concat-iterable-table (make-concat-iterable-table))


(defn iterable-concat
  [{:keys [datatype unchecked?]} & args]
  (let [datatype (or datatype
                     (when-let [first-arg (first args)]
                       (dtype-proto/get-datatype (first args)))
                     :float64)
        create-fn (get concat-iterable-table (casting/flatten-datatype datatype))]
    (create-fn datatype args)))


;;take, drop
;;take-last drop-last
