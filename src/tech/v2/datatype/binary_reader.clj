(ns tech.v2.datatype.binary-reader
  (:require [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting])
  (:import [tech.v2.datatype BinaryReader ByteReader ByteConversions]
           [java.nio ByteBuffer Buffer ByteOrder]))


(extend-type Buffer
  dtype-proto/PEndianness
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian)))


(defn default-endianness
  [item]
  (or item :little-endian))


(defn reader->binary-reader
  (^BinaryReader [reader {user-endianness :endianness :as options}]
   (let [user-endianness (default-endianness user-endianness)]
     (if (instance? BinaryReader reader)
       reader
       (let [reader (typecast/datatype->reader :int8 reader)
             lsize (.lsize reader)]
         (case user-endianness
           :little-endian
           (reify
             dtype-proto/PEndianness
             (endianness [rdr] user-endianness)
             dtype-proto/PCountable
             (ecount [rdr] lsize)
             dtype-proto/PToReader
             (convertible-to-reader? [rdr] true)
             (->reader [rdr options]
               (dtype-proto/->reader reader options))
             dtype-proto/PBuffer
             (sub-buffer [item offset len]
               (reader->binary-reader (dtype-proto/sub-buffer reader offset len)
                                      options))
             BinaryReader
             (readBoolean [rdr offset]
               (if (== 0 (.read reader offset))
                 false
                 true))
             (readByte [rdr offset]
               (.read reader offset))
             (readShort [rdr offset]
               (ByteConversions/shortFromReaderLE reader offset))
             (readInt [rdr offset]
               (ByteConversions/intFromReaderLE reader offset))
             (readLong [rdr offset]
               (ByteConversions/longFromReaderLE reader offset))
             (readFloat [rdr offset]
               (ByteConversions/floatFromReaderLE reader offset))
             (readDouble [rdr offset]
               (ByteConversions/doubleFromReaderLE reader offset)))
           :big-endian
           (reify
             dtype-proto/PEndianness
             (endianness [rdr] user-endianness)
             dtype-proto/PCountable
             (ecount [rdr] lsize)
             dtype-proto/PToReader
             (convertible-to-reader? [rdr] true)
             (->reader [rdr options]
               (dtype-proto/->reader reader options))
             dtype-proto/PBuffer
             (sub-buffer [item offset len]
               (reader->binary-reader (dtype-proto/sub-buffer reader offset len)
                                      options))
             BinaryReader
             (readBoolean [rdr offset]
               (if (== 0 (.read reader offset))
                 false
                 true))
             (readByte [rdr offset]
               (.read reader offset))
             (readShort [reader offset]
               (ByteConversions/shortFromReaderBE reader offset))
             (readInt [rdr offset]
               (ByteConversions/intFromReaderBE reader offset))
             (readLong [rdr offset]
               (ByteConversions/longFromReaderBE reader offset))
             (readFloat [rdr offset]
               (ByteConversions/floatFromReaderBE reader offset))
             (readDouble [rdr offset]
               (ByteConversions/doubleFromReaderBE reader offset))))))))
  (^BinaryReader [reader]
   (reader->binary-reader reader {})))


(defn byte-nio-buf->binary-reader
  (^BinaryReader [^ByteBuffer nio-buf {user-endianness :endianness :as options}]
   (let [user-endianness (default-endianness user-endianness)
         buffer-endianness (dtype-proto/endianness nio-buf)
         ^ByteBuffer nio-buf
         (if (= user-endianness buffer-endianness)
           nio-buf
           ;;I know that sub-buffer produces a new nio buffer that is safe to change.
           (let [^ByteBuffer new-buf (dtype-proto/sub-buffer nio-buf 0
                                                             (dtype-proto/ecount
                                                              nio-buf))]
             (case user-endianness
               :little-endian (.order new-buf ByteOrder/LITTLE_ENDIAN)
               :big-endian (.order new-buf ByteOrder/BIG_ENDIAN))
             new-buf))]
     (assert (= (dtype-proto/endianness nio-buf) user-endianness)
             (format "buffer %s user %s"
                     (dtype-proto/endianness nio-buf)
                     user-endianness))
     (reify
       dtype-proto/PEndianness
       (endianness [item] user-endianness)
       dtype-proto/PCountable
       (ecount [item] (dtype-proto/ecount nio-buf))
       dtype-proto/PToNioBuffer
       (convertible-to-nio-buffer? [item] true)
       (->buffer-backing-store [item] nio-buf)
       dtype-proto/PBuffer
       (sub-buffer [item offset len]
         (byte-nio-buf->binary-reader
          (dtype-proto/sub-buffer nio-buf offset len)
          options))
       dtype-proto/PToReader
       (convertible-to-reader? [rdr] true)
       (->reader [rdr options]
         (dtype-proto/->reader nio-buf options))
       BinaryReader
       (readBoolean [rdr offset]
         (if (== 0 (.getByte nio-buf (unchecked-int offset)))
           false
           true))
       (readByte [rdr offset]
         (.getByte nio-buf (unchecked-int offset)))
       (readShort [reader offset]
         (.getShort nio-buf (unchecked-int offset)))
       (readInt [rdr offset]
         (.getInt nio-buf (unchecked-int offset)))
       (readLong [rdr offset]
         (.getLong nio-buf (unchecked-int offset)))
       (readFloat [rdr offset]
         (.getFloat nio-buf (unchecked-int offset)))
       (readDouble [rdr offset]
         (.getDouble nio-buf (unchecked-int offset))))))
  (^BinaryReader [nio-buf]
   (byte-nio-buf->binary-reader nio-buf {})))


(extend-protocol dtype-proto/PConvertibleToBinaryReader
  ByteBuffer
  (convertible-to-binary-reader? [item] true)
  (->binary-reader [item options] (byte-nio-buf->binary-reader item options))
  ByteReader
  (convertible-to-binary-reader? [item] true)
  (->binary-reader [item options] (reader->binary-reader item options)))


(defn ->binary-reader
  (^BinaryReader [item options]
   (when item
     (cond
       (and (instance? BinaryReader item)
            (= (default-endianness (:endianness options))
               (dtype-proto/endianness item)))
       item

       (dtype-proto/convertible-to-binary-reader? item)
       (dtype-proto/->binary-reader item options)

       (and (= (dtype-proto/get-datatype item) :int8)
            (dtype-proto/convertible-to-reader? item))
       (reader->binary-reader (dtype-proto/->reader item {:datatype :int8}))
       )))
  (^BinaryReader [item]
   (->binary-reader item {})))
