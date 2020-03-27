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
            [clojure.set :as c-set]
            [clojure.string :as s])
  (:import [java.awt.image BufferedImage
            DataBufferByte DataBufferDouble DataBufferFloat
            DataBufferInt DataBufferShort DataBufferUShort
            DataBuffer]
           [java.awt RenderingHints Graphics2D]
           [java.io InputStream]
           [tech.v2.datatype ShortReader ShortWriter]
           [javax.imageio ImageIO])
  (:refer-clojure :exclude [load]))

(set! *warn-on-reflection* true)


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
  [img]
  (case (image-type img)
    :byte-bgr :bgr
    :byte-abgr :abgr
    :byte-abgr-pre :abgr-pre
    :byte-gray :gray
    :int-argb :bgra
    :int-argb-pre :bgra-pre
    :int-bgr :rgb
    :int-rgb :bgr
    :ushort-555-rgb :rgb
    :ushort-565-rgb :rgb
    :ushort-gray :gray))


(defn image-channel-map
  "Get a map from keyword channel name to channel index in a ubyte tensor"
  [img]
  (case (image-type img)
    :byte-bgr {:b 0 :g 1 :r 2}
    :byte-abgr {:a 0 :b 1 :g 2 :r 3}
    :byte-abgr-pre {:a 0 :b 1 :g 2 :r 3}
    :byte-gray {:gray 0}
    :int-argb {:b 0 :g 1 :r 2 :a 3}
    :int-argb-pre {:b 0 :g 1 :r 2 :a 3}
    :int-bgr {:r 0 :g 1 :b 2}
    :int-rgb {:b 0 :g 1 :r 2}))


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


(defn- data-buffer-as-typed-buffer
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

  dtype-proto/PShape
  (shape [item]
    (if (> (.getNumBanks item) 1)
      [(.getNumBanks item) (.getSize item)]
      [(.getSize item)]))

  dtype-proto/PToNioBuffer
  (convertible-to-nio-buffer? [item]
    (= 1 (.getNumBanks item)))
  (->buffer-backing-store [item]
    (when (dtype-proto/convertible-to-nio-buffer? item)
      (-> (data-buffer-banks item)
          first
          (dtype-proto/->buffer-backing-store)
          (dtype-proto/sub-buffer (.getOffset item) (.getSize item)))))
  dtype-proto/PToArray
  (->sub-array [item]
    (when-let [nio-buf (dtype-proto/->buffer-backing-store item)]
      (dtype-proto/->sub-array nio-buf)))
  (->array-copy [item]
    (dtype-proto/->array-copy (dtype-proto/->reader item nil)))
  dtype-proto/PBuffer
  (sub-buffer [item offset len]
    (if-let [nio-buf (data-buffer-as-typed-buffer item)]
      (dtype-proto/sub-buffer nio-buf offset len)
      (throw (Exception. "Failed to get sub buffer of composite data buffer"))))
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
  (.. img getRaster getDataBuffer))


(declare draw-image! as-ubyte-tensor)


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
  dtype-proto/PPrototype
  (from-prototype [item datatype shape]
    (when-not (= 3 (count shape))
      (throw (Exception. "Shape must be 3 dimensional")))
    (case datatype
      :uint8 (case (long (last shape))
               1 (BufferedImage. (second shape) (first shape)
                                 BufferedImage/TYPE_BYTE_GRAY)
               3 (BufferedImage. (second shape) (first shape)
                                 BufferedImage/TYPE_3BYTE_BGR)
               4 (BufferedImage. (second shape) (first shape)
                                 (BufferedImage/TYPE_4BYTE_ABGR)))
      :int32 (case (long (last shape))
               3 (BufferedImage. (second shape) (first shape)
                                 BufferedImage/TYPE_INT_RGB)
               4 (BufferedImage. (second shape) (first shape)
                                 BufferedImage/TYPE_INT_ARGB))))

  dtype-proto/PClone
  (clone [item]
    (dtype/copy! item
                 (BufferedImage. (.getWidth item) (.getHeight item)
                                 (.getType item))))
  dtype-proto/PToNioBuffer
  (convertible-to-nio-buffer? [item]
    (dtype-proto/convertible-to-nio-buffer?
     (buffered-image->data-buffer item)))
  (->buffer-backing-store [item]
    (dtype-proto/->buffer-backing-store
     (buffered-image->data-buffer item)))
  dtype-proto/PToArray
  (->sub-array [item]
    (dtype-proto/->sub-array (buffered-image->data-buffer item)))
  (->array-copy [item]
    (dtype-proto/->array-copy (buffered-image->data-buffer item)))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (dtype-proto/->reader (buffered-image->data-buffer item) options))
  dtype-proto/PBuffer
  (sub-buffer [item offset len]
    (-> (buffered-image->data-buffer item)
        (dtype-proto/sub-buffer offset len)))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item options]
    (dtype-proto/->writer (buffered-image->data-buffer item) options)))


(deftype SimpleReadWriteBuffer [reader writer]
  dtype-proto/PDatatype
  (get-datatype [item] (dtype-proto/get-datatype reader))
  dtype-proto/PShape
  (shape [item] (dtype-proto/shape reader))
  dtype-proto/PCountable
  (ecount [item] (dtype-proto/ecount reader))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (dtype-proto/->reader reader options))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item options]
    (dtype-proto/->writer writer options))
  dtype-proto/PBuffer
  (sub-buffer [item offset length]
    (SimpleReadWriteBuffer. (dtype-proto/sub-buffer reader offset length)
                            (dtype-proto/sub-buffer writer offset length))))


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
  dtype-proto/PBuffer
  (sub-buffer [item offset len]
    (-> (SimpleReadWriteBuffer. (dtype-proto/->reader item {})
                                (dtype-proto/->writer item {}))
        (dtype-proto/sub-buffer offset len)))
  dtype-proto/PClone
  (clone [item]
    (PackedIntUbyteBuffer. (dtype-proto/clone int-buffer datatype)
                           n-elems
                           shape
                           n-channels
                           unsynchronized-writes?))
  dtype-proto/PPrototype
  (from-prototype [item datatype shape]
    (when-not (= datatype :uint8)
      (throw (Exception. "Cannot create packed buffer for arbitrary datatypes")))
    (when-not (>= (count shape) 2)
      (throw (Exception. "Must have more than 1 shape entry")))
    (let [n-channels (last shape)
          n-pixels (apply * (butlast shape))]
      (when (> n-channels 4)
        (throw (Exception. "More than 4 ubyte entries in 32 byte integer")))

      (PackedIntUbyteBuffer. (dtype-proto/from-prototype int-buffer :int32
                                                         [n-pixels])
                             (apply * shape)
                             shape
                             n-channels
                             unsynchronized-writes?)))
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
  keys in the image-types map.
  ** Arguments are reverse of the buffered image constructor **"
  ^BufferedImage [height width img-type]
  (if-let [buf-img-type (get image-types img-type)]
    (BufferedImage. width height buf-img-type)
    (throw (Exception. "Unrecognized image type: %s" img-type))))


(defn load
  "Load an image.  There are better versions of this in tech.io"
  ^BufferedImage [fname-or-stream]
  (with-open [istream (io/input-stream fname-or-stream)]
    (ImageIO/read ^InputStream istream)))


(defn save!
  "Save an image.  Format-str can be things like \"PNG\" or \"JPEG\".
  There are better versions of this in tech.io."
  ([^BufferedImage img ^String format-str fname-or-stream]
   (with-open [ostream (io/output-stream fname-or-stream)]
     (ImageIO/write img format-str ostream)))
  ([img ^String fname-str]
   (let [format-str (.substring fname-str (inc (.lastIndexOf fname-str ".")))]
     (save! img format-str fname-str))))


(def interpolation-types
  {:bilinear RenderingHints/VALUE_INTERPOLATION_BILINEAR
   :cubic RenderingHints/VALUE_INTERPOLATION_BICUBIC
   :nearest RenderingHints/VALUE_INTERPOLATION_NEAREST_NEIGHBOR})


(defn draw-image!
  "Draw a source image onto a destination image.  This can be used for scaling,
  cropping, or copying images."
  [^BufferedImage src-img ^BufferedImage dst-image
   & {:keys [src-x-offset src-y-offset
             src-rect-width src-rect-height
             dst-x-offset dst-y-offset
             dst-rect-width dst-rect-height
             interpolation-type]}]
  (let [dst-x-offset (long (or dst-x-offset 0))
        dst-y-offset (long (or dst-y-offset 0))
        src-x-offset (long (or src-x-offset 0))
        src-y-offset (long (or src-y-offset 0))
        min-width (min (.getWidth src-img) (.getWidth dst-image))
        min-height (min (.getHeight src-img) (.getHeight dst-image))
        src-rect-width (long (or src-rect-width min-width))
        src-rect-height (long (or src-rect-height min-height))
        dst-rect-width (long (or dst-rect-width min-width))
        dst-rect-height (long (or dst-rect-height min-height))
        g (.getGraphics dst-image)]
    (when interpolation-type
      (if-let [inter-num (get interpolation-types interpolation-type)]
        (.setRenderingHint ^Graphics2D g
                           RenderingHints/KEY_INTERPOLATION
                           inter-num)
        (throw (Exception. (format "Invalid interpolation type: %s"
                                   interpolation-type)))))
    (doto g
      (.drawImage src-img dst-x-offset dst-y-offset
                  (+ dst-rect-width dst-x-offset)
                  (+ dst-rect-height dst-y-offset)
                  src-x-offset src-y-offset
                  (+ src-rect-width src-x-offset)
                  (+ src-rect-height src-y-offset)
                  nil)
      (.dispose))
    dst-image))


(defn downsample-bilinear
  ^BufferedImage [^BufferedImage src-img & {:keys [dst-img-width
                                                   dst-img-height
                                                   dst-img-type]}]
  (let [src-img-width (.getWidth src-img)
        src-img-height (.getHeight src-img)
        dst-img-width (long (or dst-img-width
                                (quot src-img-width 2)))
        dst-img-height (long (or dst-img-height
                                 (quot src-img-height 2)))
        dst-img-type (or dst-img-type (image-type src-img))
        resized (new-image dst-img-height dst-img-width dst-img-type)]
    (draw-image! src-img resized
                 :src-rect-width src-img-width
                 :src-rect-height src-img-height
                 :dst-rect-width dst-img-width
                 :dst-rect-height dst-img-height
                 :interpolation-type :bilinear)))


(defn resize
  [src-img new-width new-height {:keys [resize-algorithm
                                        dst-img-type]}]
  (let [[src-height src-width _n-channels] (dtype/shape src-img)
        retval (new-image new-height new-width
                          (or dst-img-type
                              (image-type src-img)))
         resize-algorithm (or resize-algorithm
                              (if (> (int new-width)
                                     (int src-width))
                                :bilinear
                                :nearest))]
    (draw-image! src-img retval
                 :src-rect-width src-width
                 :src-rect-height src-height
                 :dst-rect-width new-width
                 :dst-rect-height new-height
                 :interpolation-type resize-algorithm)))

(defn clone
  [src-img]
  (dtype/clone src-img))


(defmethod dtype-proto/make-container :buffered-image
  [container-type datatype img-shape options]
  (when-not (= datatype :uint8)
    (throw (Exception. "Only uint8 datatype allowed")))
  (when-not (= 3 (count img-shape))
    (throw (Exception. "Images must have 3 dimensions [h w c]")))
  (let [[height width channels] img-shape
        channels (long channels)]
    (when-not (or (= channels 3)
                  (= channels 4)
                  (= channels 1))
      (throw (Exception. "Byte images only support 1,3, or 4 channels.")))
    (new-image height width (case channels
                              1 :byte-gray
                              3 :byte-bgr
                              4 :byte-abgr))))
