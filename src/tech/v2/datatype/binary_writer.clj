(ns tech.v2.datatype.binary-writer
  (:require [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype.protocols
             :refer [default-endianness]
             :as dtype-proto]
            [tech.v2.datatype.binary-impl-helper :refer [reify-binary-writer-header]])
  (:import [tech.v2.datatype BinaryWriter ByteWriter ByteConversions]
           [java.nio ByteBuffer Buffer ByteOrder]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn writer-matches?
  [buffer options]
  (boolean (and (instance? BinaryWriter buffer)
                (= (dtype-proto/endianness buffer)
                   (default-endianness (:endianness options))))))


(defmacro short-to-writer
  [endianness value writer offset]
  (case endianness
    :little-endian `(ByteConversions/shortToWriterLE ~value ~writer ~offset)
    :big-endian `(ByteConversions/shortToWriterBE ~value ~writer ~offset)))

(defmacro int-to-writer
  [endianness value writer offset]
  (case endianness
    :little-endian `(ByteConversions/intToWriterLE ~value ~writer ~offset)
    :big-endian `(ByteConversions/intToWriterBE ~value ~writer ~offset)))

(defmacro long-to-writer
  [endianness value writer offset]
  (case endianness
    :little-endian `(ByteConversions/longToWriterLE ~value ~writer ~offset)
    :big-endian `(ByteConversions/longToWriterBE ~value ~writer ~offset)))


(defmacro float-to-writer
  [endianness value writer offset]
  (case endianness
    :little-endian `(ByteConversions/floatToWriterLE ~value ~writer ~offset)
    :big-endian `(ByteConversions/floatToWriterBE ~value ~writer ~offset)))


(defmacro double-to-writer
  [endianness value writer offset]
  (case endianness
    :little-endian `(ByteConversions/doubleToWriterLE ~value ~writer ~offset)
    :big-endian `(ByteConversions/doubleToWriterBE ~value ~writer ~offset)))



(defmacro make-writer-binary-writer
  [endianness buffer options]
  `(let [buffer# ~buffer
         writer# (typecast/datatype->writer :int8 buffer#)
         lsize# (.lsize writer#)]
     (reify-binary-writer-header
      ~endianness buffer# lsize# ~options writer->binary-writer
       BinaryWriter
       (writeBoolean [wtr# val# offset#]
         (.write writer# offset# (if val# (byte 1) (byte 0))))
       (writeByte [wtr# val# offset#]
         (.write writer# offset# val#))
       (writeShort [wtr# val# offset#]
         (short-to-writer ~endianness val# writer# offset#))
       (writeInt [wtr# val# offset#]
         (int-to-writer ~endianness val# writer# offset#))
       (writeLong [wtr# val# offset#]
         (long-to-writer ~endianness val# writer# offset#))
       (writeFloat [wtr# val# offset#]
         (float-to-writer ~endianness val# writer# offset#))
       (writeDouble [wtr# val# offset#]
         (double-to-writer ~endianness val# writer# offset#)))))


(defn writer->binary-writer
  (^BinaryWriter [buffer {user-endianness :endianness :as options}]
   (let [user-endianness (default-endianness user-endianness)]
     (if (writer-matches? buffer options)
       buffer
       (case user-endianness
         :little-endian (make-writer-binary-writer :little-endian buffer options)
         :big-endian (make-writer-binary-writer :big-endian buffer options)))))
  (^BinaryWriter [writer]
   (writer->binary-writer writer {})))


(defn byte-nio-buf->binary-writer
  (^BinaryWriter [^ByteBuffer nio-buf {user-endianness :endianness :as options}]
   (let [user-endianness (default-endianness user-endianness)
         buffer-endianness (dtype-proto/endianness nio-buf)
         ^ByteBuffer nio-buf
         (if (= user-endianness buffer-endianness)
           nio-buf
           ;;I know that sub-buffer produces a new superficial nio buffer that is safe to change.
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
     (reify-binary-writer-header
      user-endianness nio-buf lsize options byte-nio-buf->binary-writer
       BinaryWriter
       (writeBoolean [wtr val offset]
         (.put nio-buf (+ buffer-start (unchecked-int offset)) (if val (byte 1) (byte 0))))
       (writeByte [wtr val offset]
         (.put nio-buf (+ buffer-start (unchecked-int offset)) val))
       (writeShort [writer val offset]
         (.putShort nio-buf (+ buffer-start (unchecked-int offset)) val))
       (writeInt [wtr val offset]
         (.putInt nio-buf (+ buffer-start (unchecked-int offset)) val))
       (writeLong [wtr val offset]
         (.putLong nio-buf (+ buffer-start (unchecked-int offset)) val))
       (writeFloat [wtr val offset]
         (.putFloat nio-buf (+ buffer-start (unchecked-int offset)) val))
       (writeDouble [wtr val offset]
         (.putDouble nio-buf (+ buffer-start (unchecked-int offset)) val)))))
  (^BinaryWriter [nio-buf]
   (byte-nio-buf->binary-writer nio-buf {})))



(extend-protocol dtype-proto/PConvertibleToBinaryWriter
  ByteBuffer
  (convertible-to-binary-writer? [item] true)
  (->binary-writer [item options] (byte-nio-buf->binary-writer item options))
  ByteWriter
  (convertible-to-binary-writer? [item] true)
  (->binary-writer [item options] (writer->binary-writer item options)))


(defn ->binary-writer
  "Make a binary writer out of something.  May return nil."
  (^BinaryWriter [item options]
   (when item
     (cond
       (writer-matches? item options)
       item

       (dtype-proto/convertible-to-binary-writer? item)
       (dtype-proto/->binary-writer item options)

       (and (= (dtype-proto/get-datatype item) :int8)
            (dtype-proto/convertible-to-writer? item))
       (writer->binary-writer (dtype-proto/->writer item {:datatype :int8})))))
  (^BinaryWriter [item]
   (->binary-writer item {})))


(extend-type Object
  dtype-proto/PConvertibleToBinaryWriter
  (convertible-to-binary-writer? [item]
    (and (= :int8 (dtype-proto/get-datatype item))
         (dtype-proto/convertible-to-writer? item)))
  (->binary-writer [item options]
    (writer->binary-writer (dtype-proto/->writer item {:datatype :int8}))))
