(ns tech.libs.buffered-image
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.typed-buffer :as typed-buffer]
            [tech.v2.datatype.readers.concat :as concat-reader]
            [tech.v2.datatype.writers.concat :as concat-writer]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.tensor :as dtt]
            [clojure.java.io :as io]
            [clojure.set :as c-set])
  (:import [java.awt.image BufferedImage
            DataBufferByte DataBufferDouble DataBufferFloat
            DataBufferInt DataBufferShort DataBufferUShort
            DataBuffer]
           [java.io InputStream]
           [tech.v2.datatype ShortReader ShortWriter]
           [javax.imageio ImageIO])
  (:refer-clojure :exclude [load]))


(def image-types
  {:byte-bgr BufferedImage/TYPE_3BYTE_BGR
   :byte-abgr BufferedImage/TYPE_4BYTE_ABGR
   :byte-abgr-pre BufferedImage/TYPE_4BYTE_ABGR_PRE
   :byte-binary BufferedImage/TYPE_BYTE_BINARY
   :byte-gray BufferedImage/TYPE_BYTE_GRAY
   :byte-indexed BufferedImage/TYPE_BYTE_INDEXED
   :custom BufferedImage/TYPE_CUSTOM
   :int-argb BufferedImage/TYPE_INT_ARGB
   :int-argb-pre BufferedImage/TYPE_INT_ARGB_PRE
   :int-bgr BufferedImage/TYPE_INT_BGR
   :int-rgb BufferedImage/TYPE_INT_RGB
   :ushort-555-rgb BufferedImage/TYPE_USHORT_555_RGB
   :ushort-565-rgb BufferedImage/TYPE_USHORT_565_RGB
   :ushort-gray BufferedImage/TYPE_USHORT_GRAY})


(def image-enum->image-type-map (c-set/map-invert image-types))


(defn image-type
  "Get the image type of a buffered image as a keyword."
  [^BufferedImage img]
  (get image-enum->image-type-map (.getType img)))


(defn image-channel-format
  "Get the image channel format of the buffered image.  Formats returned may be:
  :gray :bgr :rgb :abgr :argb :abgr-pre :argb-pre"
  [^BufferedImage img]
  (case (image-type img)
    :byte-bgr :bgr
    :byte-abgr :abgr
    :byte-abgr-pre :abgr-pre
    :byte-gray :gray
    :int-argb :argb
    :int-argb-pre :argb-pre
    :int-bgr :bgr
    :int-rgb :rgb
    :ushort-555-rgb :rgb
    :ushort-565-rgb :rgb
    :ushort-gray :gray))


(def data-buffer-types
  {:uint8 DataBuffer/TYPE_BYTE
   :float64 DataBuffer/TYPE_DOUBLE
   :float32 DataBuffer/TYPE_FLOAT
   :int32 DataBuffer/TYPE_INT
   :int16 DataBuffer/TYPE_SHORT
   :unknown DataBuffer/TYPE_UNDEFINED
   :uint16 DataBuffer/TYPE_USHORT})


(def data-buffer-type-enum->type-map (c-set/map-invert data-buffer-types))


(defprotocol PDataBufferAccess
  (data-buffer-banks [item]))


(defn data-buffer-as-typed-buffer
  [^DataBuffer data-buf]
  (when-let [nio-buf (dtype-proto/->buffer-backing-store data-buf)]
    (typed-buffer/set-datatype nio-buf (dtype-proto/get-datatype data-buf))))


(extend-type DataBuffer
  dtype-proto/PDatatype
  (get-datatype [item]
    (get data-buffer-type-enum->type-map (.getDataType item)))

  dtype-proto/PCountable
  (ecount [item]
    (long (* (.getSize item)
             (.getNumBanks item))))

  dtype-proto/PToNioBuffer
  (convertible-to-nio-buffer? [item]
    (and (= 1 (.getNumBanks item))
         (= 0 (.getOffset item))))
  (->buffer-backing-store [item]
    (when (dtype-proto/convertible-to-nio-buffer? item)
      (-> (data-buffer-banks item)
          first
          (dtype-proto/->buffer-backing-store))))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (->
     (if-let [nio-buf (data-buffer-as-typed-buffer item)]
       nio-buf
       (let [item-size (.getSize item)
             item-banks (data-buffer-banks item)
             item-offsets (.getOffsets item)
             item-dtype (dtype-proto/get-datatype item)]
         (->> (map (fn [bank offset]
                     (-> (dtype/sub-buffer bank offset item-size)
                         (typed-buffer/set-datatype item-dtype)))
                   item-banks item-offsets)
              (concat-reader/concat-readers))))
     (dtype-proto/->reader options)))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item options]
    (->
     (if-let [nio-buf (data-buffer-as-typed-buffer item)]
       nio-buf
       (let [item-size (.getSize item)
             item-banks (data-buffer-banks item)
             item-offsets (.getOffsets item)
             item-dtype (dtype-proto/get-datatype item)]
         (->> (map (fn [bank offset]
                     (-> (dtype/sub-buffer bank offset item-size)
                         (typed-buffer/set-datatype item-dtype)))
                   item-banks item-offsets)
              (concat-writer/concat-writers))))
     (dtype-proto/->writer options))))


(extend-type DataBufferByte
  PDataBufferAccess
  (data-buffer-banks [item] (.getBankData item)))

(extend-type DataBufferShort
  PDataBufferAccess
  (data-buffer-banks [item] (.getBankData item)))

(extend-type DataBufferUShort
  PDataBufferAccess
  (data-buffer-banks [item] (.getBankData item)))

(extend-type DataBufferInt
  PDataBufferAccess
  (data-buffer-banks [item] (.getBankData item)))

(extend-type DataBufferFloat
  PDataBufferAccess
  (data-buffer-banks [item] (.getBankData item)))

(extend-type DataBufferDouble
  PDataBufferAccess
  (data-buffer-banks [item] (.getBankData item)))


(defn buffered-image->data-buffer
  ^DataBuffer [^BufferedImage img]
  (.. img getData getDataBuffer))


(extend-type BufferedImage
  dtype-proto/PDatatype
  (get-datatype [item]
    (dtype-proto/get-datatype (buffered-image->data-buffer item)))
  dtype-proto/PCountable
  (ecount [item]
    (dtype-proto/ecount
     (buffered-image->data-buffer item)))
  dtype-proto/PShape
  (shape [item]
    (let [height (.getHeight item)
          width (.getWidth item)]
      (case (image-type item)
        :byte-bgr [height width 3]
        :byte-abgr [height width 4]
        :byte-abgr-pre [height width 4]
        :byte-gray [height width 1]
        [height width 1])))
  dtype-proto/PToNioBuffer
  (convertible-to-nio-buffer? [item]
    (dtype-proto/convertible-to-nio-buffer?
     (buffered-image->data-buffer item)))
  (->buffer-backing-store [item]
    (dtype-proto/->buffer-backing-store
     (buffered-image->data-buffer item)))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (dtype-proto/->reader (buffered-image->data-buffer item) options))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item options]
    (dtype-proto/->writer (buffered-image->data-buffer item) options)))


(deftype PackedIntUbyteBuffer [int-buffer n-elems shape n-channels
                               unsynchronized-writes?]
  dtype-proto/PDatatype
  (get-datatype [item] :uint8)
  dtype-proto/PCountable
  (ecount [item] n-elems)
  dtype-proto/PShape
  (shape [item] shape)
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (let [n-channels (long n-channels)
          src-reader (typecast/datatype->reader :int32 int-buffer)]
      (-> (reify ShortReader
            (getDatatype [rdr] :uint8)
            (lsize [rdr] (long n-elems))
            (read [rdr idx]
              (let [pix-idx (quot idx n-channels)
                    chan-idx (rem idx n-channels)
                    src-data (.read src-reader pix-idx)]
                (-> (case chan-idx
                      0 (bit-and 0xFF src-data)
                      1 (-> (bit-and src-data 0xFF00)
                            (bit-shift-right 8))
                      2 (-> (bit-and src-data 0xFF0000)
                            (bit-shift-right 16))
                      3 (-> (bit-and src-data 0xFF000000)
                            (bit-shift-right 24)))
                    (unchecked-short)))))
          (dtype-proto/->reader options))))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item options]
    (let [n-channels (long n-channels)
          src-reader (typecast/datatype->reader :int32 int-buffer)
          src-writer (typecast/datatype->writer :int32 int-buffer)
          unchecked? (:unchecked? options)]
      (-> (reify ShortWriter
            (getDatatype [rdr] :uint8)
            (lsize [rdr] (long n-elems))
            (write [rdr idx value]
              (let [pix-idx (quot idx n-channels)
                    chan-idx (rem idx n-channels)
                    bit-mask (long (case chan-idx
                                     0 0xFF
                                     1 0xFF00
                                     2 0xFF0000
                                     3 0xFF000000))
                    shift-amount (long (case chan-idx
                                         0 0
                                         1 8
                                         2 16
                                         3 24))
                    value (unchecked-int
                           (if unchecked?
                             (-> (casting/datatype->unchecked-cast-fn
                                  :int16 :uint8 value)
                                 (bit-shift-left shift-amount))
                             (-> (casting/datatype->cast-fn :int16 :uint8 value)
                                 (bit-shift-left shift-amount))))]
                (if unsynchronized-writes?
                  (.write src-writer pix-idx
                          (bit-or (bit-and (.read src-reader pix-idx)
                                           (bit-not bit-mask))
                                  value))
                  (locking item
                    (.write src-writer pix-idx
                            (bit-or (bit-and (.read src-reader pix-idx)
                                             (bit-not bit-mask))
                                    value)))))))
          (dtype-proto/->writer options)))))


(defn as-ubyte-tensor
  "Get the buffered image as a uint8 tensor.  Works for byte and integer-buffer
  backed images.  If this is an integer-buffer backend image, then there is an
  option to allow unsynchronized elementwise writes to the image."
  [^BufferedImage img & {:keys [unsynchronized-writes?]}]
  (let [img-width (.getWidth img)
        img-height (.getHeight img)]
    (->
     (case (image-type img)
       :byte-bgr img
       :byte-abgr img
       :byte-abgr-pre img
       :byte-gray img
       :int-argb (PackedIntUbyteBuffer. (buffered-image->data-buffer img)
                                        (* img-width img-height 4)
                                        [img-height img-width 4]
                                        4
                                        unsynchronized-writes?)
       :int-argb-pre (PackedIntUbyteBuffer. (buffered-image->data-buffer img)
                                            (* img-width img-height 4)
                                            [img-height img-width 4]
                                            4
                                            unsynchronized-writes?)
       :int-bgr (PackedIntUbyteBuffer. (buffered-image->data-buffer img)
                                       (* img-width img-height 3)
                                       [img-height img-width 3]
                                       3
                                       unsynchronized-writes?)
       :int-rgb (PackedIntUbyteBuffer. (buffered-image->data-buffer img)
                                       (* img-width img-height 3)
                                       [img-height img-width 3]
                                       3
                                       unsynchronized-writes?))
     (dtt/ensure-tensor))))


(defn new-image
  "Create a new buffered image.  img-type is a keyword and must be one of the
  keys in the image-types map."
  ^BufferedImage [width height img-type]
  (if-let [buf-img-type (get image-types img-type)]
    (BufferedImage. width height buf-img-type)
    (throw (Exception. "Unrecognized image type: %s" img-type))))


(defn load
  "Load an image.  There are better versions of this in tech.io"
  [fname-or-stream]
  (with-open [istream (io/input-stream fname-or-stream)]
    (ImageIO/read ^InputStream istream)))


(defn save
  "Save an image.  Format-str can be things like \"PNG\" or \"JPEG\".
  There are better versions of this in tech.io."
  [img format-str fname-or-stream]
  (with-open [ostream (io/output-stream fname-or-stream)]
    (ImageIO/write img format-str ostream)))
