(ns tech.datatype.writer
  (:require [tech.datatype.casting :as casting]
            [tech.datatype.protocols :as dtype-proto]
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
            [tech.parallel :as parallel]
            [tech.jna :as jna]
            [clojure.core.matrix :as m]
            [tech.datatype.typecast :refer :all]
            [tech.datatype.fast-copy :as fast-copy]
            [tech.datatype.io :as dtype-io]
            [tech.datatype.typecast :as typecast])
  (:import [tech.datatype ObjectWriter ByteWriter
            ShortWriter IntWriter LongWriter
            FloatWriter DoubleWriter BooleanWriter]
           [com.sun.jna Pointer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn ecount
  "Type hinted ecount."
  ^long [item]
  (m/ecount item))


(declare ->marshalling-writer)


(jna/def-jna-fn "c" memset
  "Set a block of memory to a value"
  Pointer
  [data ensure-ptr-like]
  [val int]
  [num-bytes int])


(defmacro make-buffer-writer
  "Make a writer from a nio buffer or a fastutil list backing store.  "
  [writer-type buffer-type buffer
   writer-datatype
   intermediate-datatype
   buffer-datatype
   unchecked?]
  `(if ~unchecked?
     (reify ~writer-type
       (write [writer# idx# value#]
         (cls-type->write-fn ~buffer-type ~buffer idx#
          (cls-type->pos-fn ~buffer-type ~buffer)
          (unchecked-full-cast value# ~writer-datatype
                               ~intermediate-datatype
                               ~buffer-datatype)))
       (writeConstant [writer# ~'offset value# ~'n-elems]
         (let [~'value (unchecked-full-cast value# ~writer-datatype
                                            ~intermediate-datatype
                                            ~buffer-datatype)]
           ~(cond
              (nio-type? (resolve buffer-type))
              `(if (or (= ~'value (casting/datatype->unchecked-cast-fn :unknown ~buffer-datatype 0))
                       (= :int8 ~buffer-datatype))
                 (memset (dtype-proto/sub-buffer ~buffer ~'offset ~'n-elems)
                         (int ~'value) (* ~'n-elems (casting/numeric-byte-width
                                                     ~buffer-datatype)))
                 (let [pos# (+ (.position ~buffer) ~'offset)]
                   (parallel/parallel-for
                    idx# ~'n-elems
                    (buf-put ~buffer idx# pos# ~'value))))
              (list-type? (resolve buffer-type))
              `(parallel/parallel-for
                idx# ~'n-elems
                (.set ~buffer (+ ~'offset idx#) ~'value))
              :else
              (throw (ex-info (format "Unrecognized datatype %s" (type buffer-type)) {})))))
       (writeBlock [writer# ~'offset ~'values]
         (let [~'buf-pos (cls-type->pos-fn ~buffer-type ~buffer)
               ~'values-pos (datatype->pos-fn ~writer-datatype ~'values)
               ~'count (base/ecount ~'values)]
           ~(if (and (nio-type? (resolve buffer-type))
                     (= buffer-datatype (casting/datatype->host-type writer-datatype)))
              `(fast-copy/copy! (dtype-proto/sub-buffer ~buffer ~'offset ~'count)
                                ~'values)
              `(parallel/parallel-for
                idx# ~'count
                (cls-type->write-fn ~buffer-type ~buffer (+ ~'offset idx#) ~'buf-pos
                                    (-> (datatype->read-fn ~writer-datatype ~'values idx# ~'values-pos)
                                        (unchecked-full-cast ~writer-datatype
                                                             ~intermediate-datatype
                                                             ~buffer-datatype)))))))
       (writeIndexes [writer# indexes# values#]
         (let [n-elems# (base/ecount indexes#)
               buf-pos# (cls-type->pos-fn ~buffer-type ~buffer)
               idx-pos# (.position indexes#)
               val-pos# (datatype->pos-fn ~writer-datatype values#)]
           (parallel/parallel-for
            idx#
            n-elems#
            (cls-type->write-fn ~buffer-type ~buffer (buf-get indexes# idx-pos# idx#) buf-pos#
                                (-> (datatype->read-fn ~writer-datatype values# idx# val-pos#)
                                    (unchecked-full-cast ~writer-datatype
                                                         ~intermediate-datatype
                                                         ~buffer-datatype)))))))
     (reify ~writer-type
       (write [writer# idx# value#]
         (cls-type->write-fn ~buffer-type ~buffer idx#
                             (cls-type->pos-fn ~buffer-type ~buffer)
                             (checked-full-write-cast value# ~writer-datatype
                                                  ~intermediate-datatype
                                                  ~buffer-datatype)))
       (writeConstant [writer# ~'offset value# ~'n-elems]
         (let [~'value (checked-full-write-cast value# ~writer-datatype
                                            ~intermediate-datatype
                                            ~buffer-datatype)]
           ~(cond
              (nio-type? (resolve buffer-type))
              `(if (or (= ~'value (casting/datatype->unchecked-cast-fn :unknown ~buffer-datatype 0))
                       (= :int8 ~buffer-datatype))
                 (memset (dtype-proto/sub-buffer ~buffer ~'offset ~'n-elems)
                         (int ~'value) (* ~'n-elems (casting/numeric-byte-width
                                                     ~buffer-datatype)))
                 (let [pos# (+ (.position ~buffer) ~'offset)]
                   (parallel/parallel-for
                    idx# ~'n-elems
                    (buf-put ~buffer idx# pos# ~'value))))
              (list-type? (resolve buffer-type))
              `(parallel/parallel-for
                idx# ~'n-elems
                (.set ~buffer (+ ~'offset idx#) ~'value))
              :else
              (throw (ex-info (format "Unrecognized datatype %s" (type buffer-type)) {})))))
       (writeBlock [writer# ~'offset ~'values]
         (let [~'buf-pos (cls-type->pos-fn ~buffer-type ~buffer)
               ~'values-pos (datatype->pos-fn ~writer-datatype ~'values)
               ~'count (base/ecount ~'values)]
           ~(if (and (nio-type? (resolve buffer-type))
                     (= buffer-datatype writer-datatype))
              `(fast-copy/copy! (dtype-proto/sub-buffer ~buffer ~'offset ~'count)
                                ~'values)
              `(parallel/parallel-for
                idx# ~'count
                (cls-type->write-fn ~buffer-type ~buffer (+ ~'offset idx#) ~'buf-pos
                                    (-> (datatype->read-fn ~writer-datatype ~'values idx# ~'values-pos)
                                        (checked-full-write-cast ~writer-datatype
                                                                 ~intermediate-datatype
                                                                 ~buffer-datatype)))))))
       (writeIndexes [writer# indexes# values#]
         (let [n-elems# (base/ecount indexes#)
               buf-pos# (cls-type->pos-fn ~buffer-type ~buffer)
               idx-pos# (.position indexes#)
               val-pos# (datatype->pos-fn ~writer-datatype values#)]
           (parallel/parallel-for
            idx#
            n-elems#
            (cls-type->write-fn ~buffer-type ~buffer (buf-get indexes# idx-pos# idx#) buf-pos#
                                (-> (datatype->read-fn ~writer-datatype values# idx# val-pos#)
                                    (checked-full-write-cast ~writer-datatype
                                                         ~intermediate-datatype
                                                         ~buffer-datatype)))))))))


(defmacro make-marshalling-writer
  [dst-writer result-dtype intermediate-dtype src-dtype src-writer-type
   unchecked?]
  `(if ~unchecked?
     (reify ~src-writer-type
       (write[item# idx# value#]
         (.write ~dst-writer idx#
                 (unchecked-full-cast value# ~src-dtype ~intermediate-dtype ~result-dtype)))
       (writeConstant [item# idx# value# count#]
         (.writeConstant ~dst-writer idx#
                         (unchecked-full-cast value# ~src-dtype ~intermediate-dtype ~result-dtype)
                         count#))
       (writeBlock [item# offset# ~'values]
         (let [n-elems# (ecount ~'values)
               dst-values# ~(if (= (casting/datatype->host-datatype result-dtype)
                                   (casting/datatype->host-datatype src-dtype))
                              `~'values
                              `(typecast/datatype->buffer-cast-fn
                                ~result-dtype (dtype-proto/clone ~'values ~result-dtype)))]
           (.writeBlock ~dst-writer offset# dst-values#)))
       (writeIndexes [item# indexes# ~'values]
         (let [n-elems# (ecount ~'values)
               dst-values# ~(if (= (casting/datatype->host-datatype result-dtype)
                                   (casting/datatype->host-datatype src-dtype))
                              `~'values
                              `(typecast/datatype->buffer-cast-fn
                                ~result-dtype (dtype-proto/clone ~'values ~result-dtype)))]
           (.writeIndexes ~dst-writer indexes# dst-values#))))
     (reify ~src-writer-type
       (write[item# idx# value#]
         (.write ~dst-writer idx#
                 (checked-full-write-cast value# ~src-dtype ~intermediate-dtype ~result-dtype)))
       (writeConstant [item# idx# value# count#]
         (.writeConstant ~dst-writer idx#
                         (checked-full-write-cast value# ~src-dtype ~intermediate-dtype ~result-dtype)
                         count#))
       (writeBlock [item# offset# ~'values]
         (let [n-elems# (ecount ~'values)
               dst-values# (typecast/datatype->buffer-cast-fn
                            ~result-dtype (dtype-proto/clone ~'values ~result-dtype))]
           (.writeBlock ~dst-writer offset# dst-values#)))
       (writeIndexes [item# indexes# ~'values]
         (let [n-elems# (ecount ~'values)
               dst-values# (typecast/datatype->buffer-cast-fn
                            ~result-dtype (dtype-proto/clone ~'values ~result-dtype))]
           (.writeIndexes ~dst-writer indexes# dst-values#))))))



(defmacro extend-writer-type
  [writer-type datatype]
  `(clojure.core/extend
       ~writer-type
     dtype-proto/PDatatype
     {:get-datatype (fn [_#] ~datatype)}
     dtype-proto/PToWriter
     {:->writer-of-type
      (fn [item# dtype# unchecked?#]
        (let [dst-writer# (datatype->writer ~datatype item# true)]
          (if (= dtype# ~datatype)
            item#
            (case dtype#
              :int8 (make-marshalling-writer dst-writer# ~datatype
                                             :int8 :int8 ByteWriter unchecked?#)
              :uint8 (make-marshalling-writer dst-writer# ~datatype
                                              :uint8 :int16 ShortWriter unchecked?#)
              :int16 (make-marshalling-writer dst-writer# ~datatype
                                              :int16 :int16 ShortWriter unchecked?#)
              :uint16 (make-marshalling-writer dst-writer# ~datatype
                                               :uint16 :int32 IntWriter unchecked?#)
              :int32 (make-marshalling-writer dst-writer# ~datatype
                                              :int32 :int32 IntWriter unchecked?#)
              :uint32 (make-marshalling-writer dst-writer# ~datatype
                                               :uint32 :int64 LongWriter unchecked?#)
              :int64 (make-marshalling-writer dst-writer# ~datatype
                                              :int64 :int64 LongWriter unchecked?#)
              :uint64 (make-marshalling-writer dst-writer# ~datatype
                                               :uint64 :int64 LongWriter unchecked?#)
              :float32 (make-marshalling-writer dst-writer# ~datatype
                                                :float32 :float32 FloatWriter unchecked?#)
              :float64 (make-marshalling-writer dst-writer# ~datatype
                                                :float64 :float64 DoubleWriter unchecked?#)
              :boolean (make-marshalling-writer dst-writer# ~datatype
                                                :boolean :boolean BooleanWriter
                                                unchecked?#)
              :object (make-marshalling-writer dst-writer# ~datatype
                                               :object :object ObjectWriter
                                               unchecked?#)))))}))


(extend-writer-type ByteWriter :int8)
(extend-writer-type ShortWriter :int16)
(extend-writer-type IntWriter :int32)
(extend-writer-type LongWriter :int64)
(extend-writer-type FloatWriter :float32)
(extend-writer-type DoubleWriter :float64)
(extend-writer-type BooleanWriter :boolean)
(extend-writer-type ObjectWriter :object)


(defn ->marshalling-writer
  [src-item dest-dtype unchecked?]
  (let [src-dtype (dtype-proto/get-datatype src-item)
        src-writer (dtype-proto/->writer-of-type src-item src-dtype false)]
    (if (= src-dtype dest-dtype)
      src-writer
      (dtype-proto/->writer-of-type src-writer dest-dtype unchecked?))))
