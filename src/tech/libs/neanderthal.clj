(ns tech.libs.neanderthal
  (:require [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.jna :as jna]
            [tech.v2.tensor.protocols :as dtt-proto])
  (:import [uncomplicate.neanderthal.internal.api Block]
           [uncomplicate.commons.core Info]
           [com.sun.jna Pointer]))


(extend-type Info
  dtype-proto/PDatatype
  (get-datatype [this]
    (let [{:keys [entry-type]} (.info this)]
      (if-let [retval (get {Double/TYPE :float64
                            Float/TYPE :float32
                            Long/TYPE :int64
                            Integer/TYPE :int32}
                           entry-type)]
        retval
        (throw (Exception. "Unrecognized type.")))))
  dtype-proto/PCountable
  (ecount [this] (:dim (.info this)))
  dtype-proto/PShape
  (shape [this] (let [info (.info this)]
                  (if (contains? info :matrix-type)
                    (if (= :column (:layout info))
                      [(:n info) (:m info)]
                      [(:m info) (:n info)])
                    [(:dim info)])))

  dtype-proto/PToBufferDesc
  (convertible-to-buffer-desc? [item] true)
  (->buffer-descriptor [item]
    (let [item-info (.info item)
          item-dtype (dtype-proto/get-datatype item)
          item-shape (dtype-proto/shape item)]
      {:ptr (jna/->ptr-backing-store item)
       :datatype item-dtype
       :shape item-shape
       ;;TVM needs the device type
       :device-type (:device item-info)
       :strides (mapv #(* (casting/numeric-byte-width item-dtype) %)
                      (if (= 2 (count item-shape))
                        (let [item-strides
                              [(:stride item-info)
                               1]]
                          (if (= :column (:layout item-info))
                            (reverse item-strides)
                            item-strides))
                        [(:stride item-info)]))}))


  dtype-proto/PToReader
  (convertible-to-reader? [item] (= :cpu (:device (.info item))))
  (->reader [item options]
    (dtype-proto/->reader (dtt-proto/convert-to-tensor item) options))

  dtype-proto/PToWriter
  (convertible-to-writer? [item] (= :cpu (:device (.info item))))
  (->writer [item options]
    (dtype-proto/->writer (dtt-proto/convert-to-tensor item) options)))


(extend-type Block
  jna/PToPtr
  (is-convertible-to-ptr? [item] true)
  (->ptr-backing-store [item]
    (let [byte-width (casting/numeric-byte-width
                      (dtype-proto/get-datatype item))
          item-offset (* (.offset item) byte-width)
          ^Pointer ptr-val (jna/->ptr-backing-store (.buffer item))]
      (Pointer. (+ item-offset
                   (Pointer/nativeValue ptr-val))))))
