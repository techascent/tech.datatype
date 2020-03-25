(ns tech.v2.datatype.binary-reader
  (:require [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.protocols :as dtype-proto])
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

(defmacro short-from-reader
  [endianness reader offset]
  (case endianness
    :little-endian `(ByteConversions/shortFromReaderLE ~reader ~offset)
    :big-endian `(ByteConversions/shortFromReaderBE ~reader ~offset)))

(defmacro int-from-reader
  [endianness reader offset]
  (case endianness
    :little-endian `(ByteConversions/intFromReaderLE ~reader ~offset)
    :big-endian `(ByteConversions/intFromReaderBE ~reader ~offset)))

(defmacro long-from-reader
  [endianness reader offset]
  (case endianness
    :little-endian `(ByteConversions/longFromReaderLE ~reader ~offset)
    :big-endian `(ByteConversions/longFromReaderBE ~reader ~offset)))


(defmacro float-from-reader
  [endianness reader offset]
  (case endianness
    :little-endian `(ByteConversions/floatFromReaderLE ~reader ~offset)
    :big-endian `(ByteConversions/floatFromReaderBE ~reader ~offset)))


(defmacro double-from-reader
  [endianness reader offset]
  (case endianness
    :little-endian `(ByteConversions/doubleFromReaderLE ~reader ~offset)
    :big-endian `(ByteConversions/doubleFromReaderBE ~reader ~offset)))


(defmacro make-reader-binary-reader
  [endianness buffer options]
  `(let [buffer# ~buffer
         reader# (typecast/datatype->reader :int8 buffer#)
         lsize# (.lsize reader#)]
     (reify
       dtype-proto/PEndianness
       (endianness [rdr#] ~endianness)
       dtype-proto/PCountable
       (ecount [rdr#] lsize#)
       dtype-proto/PToReader
       (convertible-to-reader? [rdr#] true)
       (->reader [rdr# options#]
         (dtype-proto/->reader buffer# options#))
       dtype-proto/PBuffer
       (sub-buffer [rdr# offset# len#]
         (reader->binary-reader (dtype-proto/sub-buffer buffer# offset# len#)
                                ~options))
       dtype-proto/PToArray
       (->sub-array [rdr#]
         (dtype-proto/->sub-array buffer#))
       (->array-copy [rdr#]
         (dtype-proto/->array-copy buffer#))
       dtype-proto/PToNioBuffer
       (convertible-to-nio-buffer? [rdr#]
         (dtype-proto/convertible-to-nio-buffer? buffer#))
       (->buffer-backing-store [rdr#]
         (dtype-proto/->buffer-backing-store buffer#))
       dtype-proto/PToJNAPointer
       (convertible-to-data-ptr? [rdr#]
         (dtype-proto/convertible-to-data-ptr? buffer#))
       (->jna-ptr [rdr#]
         (dtype-proto/->jna-ptr buffer#))
       dtype-proto/PConvertibleToBinaryReader
       (convertible-to-binary-reader? [rdr#] true)
       (->binary-reader [rdr# options#]
         (if (= (default-endianness (:endianness options#))
                ~endianness)
           rdr#
           (dtype-proto/->binary-reader buffer# options#)))
       BinaryReader
       (readBoolean [rdr# offset#]
         (if (== 0 (.read reader# offset#))
           false
           true))
       (readByte [rdr# offset#]
         (.read reader# offset#))
       (readShort [rdr# offset#]
         (short-from-reader ~endianness reader# offset#))
       (readInt [rdr# offset#]
         (int-from-reader ~endianness reader# offset#))
       (readLong [rdr# offset#]
         (long-from-reader ~endianness reader# offset#))
       (readFloat [rdr# offset#]
         (float-from-reader ~endianness reader# offset#))
       (readDouble [rdr# offset#]
         (double-from-reader ~endianness reader# offset#)))))


(defn reader->binary-reader
  (^BinaryReader [buffer {user-endianness :endianness :as options}]
   (let [user-endianness (default-endianness user-endianness)]
     (if (instance? BinaryReader buffer)
       buffer
       (case user-endianness
         :little-endian (make-reader-binary-reader :little-endian buffer options)
         :big-endian (make-reader-binary-reader :little-endian buffer options)))))
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
       dtype-proto/PToArray
       (->sub-array [rdr]
         (dtype-proto/->sub-array nio-buf))
       (->array-copy [rdr]
         (dtype-proto/->array-copy nio-buf))
       dtype-proto/PToJNAPointer
       (convertible-to-data-ptr? [rdr]
         (dtype-proto/convertible-to-data-ptr? nio-buf))
       (->jna-ptr [rdr]
         (dtype-proto/->jna-ptr nio-buf))
       dtype-proto/PConvertibleToBinaryReader
       (convertible-to-binary-reader? [rdr] true)
       (->binary-reader [rdr options]
         (if (= (default-endianness (:endianness options))
                user-endianness)
           rdr
           (dtype-proto/->binary-reader nio-buf options)))
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
  "Make a binary reader out of something.  May return nil."
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
