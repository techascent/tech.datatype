(ns tech.v2.datatype.binary-reader
  (:require [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.protocols
             :refer [default-endianness]
             :as dtype-proto]
            [tech.v2.datatype.binary-impl-helper :refer [reify-binary-reader-header]])
  (:import [tech.v2.datatype BinaryReader ByteReader ByteConversions]
           [java.nio ByteBuffer Buffer ByteOrder]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn reader-matches?
  [buffer options]
  (boolean (and (instance? BinaryReader buffer)
                (= (dtype-proto/endianness buffer)
                   (default-endianness (:endianness options))))))


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
     (reify-binary-reader-header
      ~endianness buffer# lsize# ~options reader->binary-reader
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
     (if (reader-matches? buffer options)
       buffer
       (case user-endianness
         :little-endian (make-reader-binary-reader :little-endian buffer options)
         :big-endian (make-reader-binary-reader :big-endian buffer options)))))
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
             new-buf))
         buffer-start (.position nio-buf)
         lsize (long (dtype-proto/ecount nio-buf))]
     (assert (= (dtype-proto/endianness nio-buf) user-endianness)
             (format "buffer %s user %s"
                     (dtype-proto/endianness nio-buf)
                     user-endianness))
     (reify-binary-reader-header
      user-endianness nio-buf lsize options byte-nio-buf->binary-reader
       BinaryReader
       (readBoolean [rdr offset]
         (if (== 0 (.get nio-buf (+ buffer-start (unchecked-int offset))))
           false
           true))
       (readByte [rdr offset]
         (.get nio-buf (+ buffer-start (unchecked-int offset))))
       (readShort [reader offset]
         (.getShort nio-buf (+ buffer-start (unchecked-int offset))))
       (readInt [rdr offset]
         (.getInt nio-buf (+ buffer-start (unchecked-int offset))))
       (readLong [rdr offset]
         (.getLong nio-buf (+ buffer-start (unchecked-int offset))))
       (readFloat [rdr offset]
         (.getFloat nio-buf (+ buffer-start (unchecked-int offset))))
       (readDouble [rdr offset]
         (.getDouble nio-buf (+ buffer-start (unchecked-int offset)))))))
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
       (reader-matches? item options)
       item

       (dtype-proto/convertible-to-binary-reader? item)
       (dtype-proto/->binary-reader item options)

       (and (= (dtype-proto/get-datatype item) :int8)
            (dtype-proto/convertible-to-reader? item))
       (reader->binary-reader (dtype-proto/->reader item {:datatype :int8})))))
  (^BinaryReader [item]
   (->binary-reader item {})))


(comment
  (require '[tech.v2.datatype :as dtype])

  (def test-buf (dtype-proto/->binary-reader (dtype/make-container :nio-buffer :int8 (range 10))
                                             {}))
  (def sub-buffer (dtype-proto/->binary-reader (dtype/sub-buffer test-buf 4 4)
                                               {}))

  (.readShort test-buf 0)
  (.readShort sub-buffer 0)
  )
