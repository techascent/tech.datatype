(ns tech.datatype.reader
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.casting :as casting]
            [tech.parallel :as parallel]
            [tech.jna :as jna]
            [tech.datatype.nio-access
             :refer [buf-put buf-get
                     datatype->pos-fn
                     datatype->read-fn
                     datatype->write-fn
                     unchecked-full-cast
                     checked-full-read-cast
                     checked-full-write-cast
                     nio-type? list-type?
                     cls-type->read-fn
                     cls-type->write-fn
                     cls-type->pos-fn]]
            [clojure.core.matrix.macros :refer [c-for]]
            [clojure.core.matrix.protocols :as mp]
            [clojure.core.matrix :as m]
            [tech.datatype.typecast :refer :all]
            [tech.datatype.fast-copy :as fast-copy]
            [clojure.core.matrix.protocols :as mp]
            [tech.datatype.io :as dtype-io]
            [tech.datatype.typecast :as typecast])
  (:import [tech.datatype ObjectReader ByteReader
            ShortReader IntReader LongReader
            FloatReader DoubleReader BooleanReader]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn ecount
  "Type hinted ecount."
  ^long [item]
  (m/ecount item))


(defmacro make-buffer-reader
  [reader-type buffer-type buffer
   reader-datatype
   intermediate-datatype
   buffer-datatype
   unchecked?]
  `(if ~unchecked?
     (reify ~reader-type
       (read [reader# idx#]
         (-> (cls-type->read-fn ~buffer-type ~buffer-datatype ~buffer idx#
                                (cls-type->pos-fn ~buffer-type ~buffer))
             (unchecked-full-cast ~buffer-datatype ~intermediate-datatype ~reader-datatype)))
       (readBlock [reader# ~'offset ~'dest]
         (let [~'buf-pos (cls-type->pos-fn ~buffer-type ~buffer)
               ~'dest-pos (datatype->pos-fn ~reader-datatype ~'dest)
               ~'count (base/ecount ~'dest)]
           ~(cond
              ;;nio is fastest path
              (and (nio-type? (resolve buffer-type))
                   (= buffer-datatype (casting/datatype->host-datatype reader-datatype)))
              `(fast-copy/copy! (dtype-proto/sub-buffer ~buffer ~'offset ~'count) ~'dest)
              (and (list-type? (resolve buffer-type))
                   (= buffer-datatype (casting/datatype->host-datatype reader-datatype))
                   `(if-let [ary-data# (dtype-proto/->sub-array ~'dest)]
                      (.getElements ~buffer ~'offset
                                    (datatype->array-cast-fn ~buffer-datatype (:array-data ary-data#)
                                                             (int (:offset ary-data#))
                                                             (int (:length ary-data#))))
                      (parallel/parallel-for
                       idx# ~'count
                       (datatype->write-fn
                        ~reader-datatype ~'dest idx# ~'dest-pos
                        (-> (cls-type->read-fn ~buffer-type ~buffer-datatype ~buffer (+ ~'offset idx#) ~'dest-pos)
                            (unchecked-full-cast ~buffer-datatype ~intermediate-datatype ~reader-datatype))))))
              `(parallel/parallel-for
                idx# ~'count
                (datatype->write-fn
                 ~reader-datatype ~'dest idx# ~'dest-pos
                 (-> (cls-type->read-fn ~buffer-type ~buffer-datatype ~buffer (+ ~'offset idx#) ~'dest-pos)
                     (unchecked-full-cast ~buffer-datatype ~intermediate-datatype ~reader-datatype)))))))
       (readIndexes [reader# indexes# dest#]
         (let [idx-pos# (.position indexes#)
               dest-pos# (datatype->pos-fn ~reader-datatype dest#)
               buf-pos# (cls-type->pos-fn ~buffer-type ~buffer)
               n-elems# (base/ecount dest#)]
           (parallel/parallel-for
            idx#
            n-elems#
            (datatype->write-fn
             ~reader-datatype dest# idx# dest-pos#
             (-> (cls-type->read-fn ~buffer-type ~buffer-datatype ~buffer (buf-get indexes# idx# idx-pos#) buf-pos#)
                 (unchecked-full-cast ~buffer-datatype ~intermediate-datatype ~reader-datatype)))))))
     (reify ~reader-type
       (read [reader# idx#]
         (-> (cls-type->read-fn ~buffer-type ~buffer-datatype ~buffer idx#
                                (cls-type->pos-fn ~buffer-type ~buffer))
             (checked-full-read-cast ~buffer-datatype ~intermediate-datatype ~reader-datatype)))
       (readBlock [reader# ~'offset ~'dest]
         (let [~'buf-pos (cls-type->pos-fn ~buffer-type ~buffer)
               ~'dest-pos (datatype->pos-fn ~reader-datatype ~'dest)
               ~'count (base/ecount ~'dest)]
           ~(cond
              ;;nio is fastest path
              (and (nio-type? (resolve buffer-type))
                   (= buffer-datatype reader-datatype))
              `(fast-copy/copy! (dtype-proto/sub-buffer ~buffer ~'offset ~'count) ~'dest)
              (and (list-type? (resolve buffer-type))
                   (= buffer-datatype reader-datatype)
                   `(if-let [ary-data# (dtype-proto/->sub-array ~'dest)]
                      (.getElements ~buffer ~'offset
                                    (datatype->array-cast-fn ~buffer-datatype (:array-data ary-data#)
                                                             (int (:offset ary-data#))
                                                             (int (:length ary-data#))))
                      (parallel/parallel-for
                       idx# ~'count
                       (datatype->write-fn
                        ~reader-datatype ~'dest idx# ~'dest-pos
                        (-> (cls-type->read-fn ~buffer-type ~buffer-datatype ~buffer (+ ~'offset idx#) ~'dest-pos)
                            (unchecked-full-cast ~buffer-datatype ~intermediate-datatype ~reader-datatype))))))
              `(parallel/parallel-for
                idx# ~'count
                (datatype->write-fn
                 ~reader-datatype ~'dest idx# ~'dest-pos
                 (-> (cls-type->read-fn ~buffer-type ~buffer-datatype ~buffer (+ ~'offset idx#) ~'dest-pos)
                     (checked-full-read-cast ~buffer-datatype ~intermediate-datatype ~reader-datatype)))))))
       (readIndexes [reader# indexes# dest#]
         (let [idx-pos# (.position indexes#)
               dest-pos# (datatype->pos-fn ~reader-datatype dest#)
               buf-pos# (cls-type->pos-fn ~buffer-type ~buffer)
               n-elems# (base/ecount dest#)]
           (parallel/parallel-for
            idx#
            n-elems#
            (datatype->write-fn
             ~reader-datatype dest# idx# dest-pos#
             (-> (cls-type->read-fn ~buffer-type ~buffer-datatype ~buffer (buf-get indexes# idx# idx-pos#) buf-pos#)
                 (checked-full-read-cast ~buffer-datatype ~intermediate-datatype ~reader-datatype)))))))))


(defmacro make-marshalling-reader
  [src-reader src-dtype intermediate-dtype result-dtype dst-reader-type unchecked?]
  `(if ~unchecked?
     (reify ~dst-reader-type
       (read [item# idx#]
         (-> (.read ~src-reader idx#)
             (unchecked-full-cast ~src-dtype ~intermediate-dtype ~result-dtype)))
       (readBlock [item# offset# ~'dest]
         (let [~'temp-dest ~(if (= (casting/datatype->host-datatype src-dtype)
                                   (casting/datatype->host-datatype result-dtype))
                              `~'dest
                              `(typecast/datatype->buffer-cast-fn
                                ~src-dtype (dtype-proto/from-prototype
                                            ~'dest ~src-dtype
                                            [(mp/element-count ~'dest)])))]
           (.readBlock ~src-reader offset# ~'temp-dest)
           ~(if (= (casting/datatype->host-datatype src-dtype)
                   (casting/datatype->host-datatype result-dtype))
              `~'temp-dest
              `(dtype-io/dense-copy! ~'dest ~'temp-dest ~unchecked? true))))
       (readIndexes [item# indexes# ~'dest]
         (let [~'temp-dest ~(if (= (casting/datatype->host-datatype src-dtype)
                                   (casting/datatype->host-datatype result-dtype))
                              `~'dest
                              `(typecast/datatype->buffer-cast-fn
                                ~src-dtype (dtype-proto/from-prototype
                                            ~'dest ~src-dtype
                                            [(mp/element-count ~'dest)])))]
           (.readIndexes ~src-reader indexes# ~'temp-dest)
           ~(if (= (casting/datatype->host-datatype src-dtype)
                   (casting/datatype->host-datatype result-dtype))
              `~'temp-dest
              `(dtype-io/dense-copy! ~'dest ~'temp-dest ~unchecked? true)))))
     (reify ~dst-reader-type
       (read [item# idx#]
         (-> (.read ~src-reader idx#)
             (checked-full-read-cast ~src-dtype ~intermediate-dtype ~result-dtype)))
       (readBlock [item# offset# ~'dest]
         (let [~'temp-dest (typecast/datatype->buffer-cast-fn
                            ~src-dtype (dtype-proto/from-prototype
                                        ~'dest ~src-dtype
                                        [(mp/element-count ~'dest)]))]
           (.readBlock ~src-reader offset# ~'temp-dest)
           (dtype-io/dense-copy! ~'dest ~'temp-dest ~unchecked? true)))
       (readIndexes [item# indexes# ~'dest]
         (let [~'temp-dest (typecast/datatype->buffer-cast-fn
                            ~src-dtype (dtype-proto/from-prototype
                                        ~'dest ~src-dtype
                                        [(mp/element-count ~'dest)]))]
           (.readIndexes ~src-reader indexes# ~'temp-dest)
           (dtype-io/dense-copy! ~'dest ~'temp-dest ~unchecked? true))))))


(defmacro extend-reader-type
  [reader-type datatype]
  `(clojure.core/extend
       ~reader-type
     dtype-proto/PDatatype
     {:get-datatype (fn [_#] ~datatype)}
     dtype-proto/PToReader
     {:->reader-of-type
      (fn [item# dtype# unchecked?#]
        (if (= dtype# ~datatype)
          item#
          (let [src-reader# (datatype->reader ~datatype item# true)]
            (let [item# (datatype->reader ~datatype item# true)]
              (case dtype#
                :int8 (make-marshalling-reader src-reader# ~datatype
                                               :int8 :int8 ByteReader unchecked?#)
                :uint8 (make-marshalling-reader src-reader# ~datatype
                                                :uint8 :int16 ShortReader unchecked?#)
                :int16 (make-marshalling-reader src-reader# ~datatype
                                                :int16 :int16 ShortReader unchecked?#)
                :uint16 (make-marshalling-reader src-reader# ~datatype
                                                 :uint16 :int32 IntReader unchecked?#)
                :int32 (make-marshalling-reader src-reader# ~datatype
                                                :int32 :int32 IntReader unchecked?#)
                :uint32 (make-marshalling-reader src-reader# ~datatype
                                                 :uint32 :int64 LongReader unchecked?#)
                :int64 (make-marshalling-reader src-reader# ~datatype
                                                :int64 :int64 LongReader unchecked?#)
                :uint64 (make-marshalling-reader src-reader# ~datatype
                                                 :uint64 :int64 LongReader unchecked?#)
                :float32 (make-marshalling-reader src-reader# ~datatype
                                                  :float32 :float32 FloatReader
                                                  unchecked?#)
                :float64 (make-marshalling-reader src-reader# ~datatype
                                                  :float64 :float64 DoubleReader
                                                  unchecked?#)
                :boolean (make-marshalling-reader src-reader# ~datatype
                                                  :boolean :boolean BooleanReader
                                                  unchecked?#)
                :object (make-marshalling-reader src-reader# ~datatype
                                                 :object :object ObjectReader
                                                 unchecked?#))))))}))


(extend-reader-type ByteReader :int8)
(extend-reader-type ShortReader :int16)
(extend-reader-type IntReader :int32)
(extend-reader-type LongReader :int64)
(extend-reader-type FloatReader :float32)
(extend-reader-type DoubleReader :float64)
(extend-reader-type BooleanReader :boolean)
(extend-reader-type ObjectReader :object)


(defn ->marshalling-reader
  [src-item dest-dtype unchecked?]
  (let [src-dtype (dtype-proto/get-datatype src-item)
        src-reader (dtype-proto/->reader-of-type src-item src-dtype false)]
    (if (= src-dtype dest-dtype)
      src-reader
      (dtype-proto/->reader-of-type src-reader dest-dtype unchecked?))))
