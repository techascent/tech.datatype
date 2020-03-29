(ns tech.v2.datatype.binary-impl-helper
  (:require [tech.v2.datatype.protocols
             :refer [default-endianness]
             :as dtype-proto]))


(defmacro reify-binary-reader-header
  [endianness buffer lsize options constructor-fn & body]
  `(let [buffer# ~buffer
         options# ~options]
     (reify
       dtype-proto/PEndianness
       (endianness [rdr#] ~endianness)
       dtype-proto/PCountable
       (ecount [rdr#] ~lsize)
       dtype-proto/PDatatype
       (get-datatype [rdr] :int8)
       dtype-proto/PClone
       (clone [rdr]
         (-> (dtype-proto/clone buffer#)
             (~constructor-fn ~options)))
       dtype-proto/PBuffer
       (sub-buffer [rdr# offset# len#]
         (-> (dtype-proto/sub-buffer buffer# offset# len#)
             (~constructor-fn ~options)))
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

       dtype-proto/PToReader
       (convertible-to-reader? [rdr#] true)
       (->reader [rdr# options#]
         (dtype-proto/->reader buffer# options#))
       dtype-proto/PConvertibleToBinaryReader
       (convertible-to-binary-reader? [rdr#] true)
       (->binary-reader [rdr# options#]
         (if (= (default-endianness (:endianness options#))
                ~endianness)
           rdr#
           (~constructor-fn buffer# options#)))

       dtype-proto/PToWriter
       (convertible-to-writer? [rdr#]
         (dtype-proto/convertible-to-writer? buffer#))
       (->writer [rdr# options#]
         (dtype-proto/->writer buffer# options#))
       dtype-proto/PConvertibleToBinaryWriter
       (convertible-to-binary-writer? [rdr#]
         (dtype-proto/convertible-to-binary-writer? buffer#))
       (->binary-writer [rdr# options#]
         (dtype-proto/->binary-writer buffer# options#))
       ~@body)))


(defmacro reify-binary-writer-header
  [endianness buffer lsize options constructor-fn & body]
  `(let [buffer# ~buffer
         options# ~options]
     (reify
       dtype-proto/PEndianness
       (endianness [wtr#] ~endianness)
       dtype-proto/PCountable
       (ecount [wtr#] ~lsize)
       dtype-proto/PDatatype
       (get-datatype [rdr] :int8)
       dtype-proto/PClone
       (clone [rdr]
         (-> (dtype-proto/clone buffer#)
             (~constructor-fn ~options)))
       dtype-proto/PBuffer
       (sub-buffer [wtr# offset# len#]
         (-> (dtype-proto/sub-buffer buffer# offset# len#)
             (~constructor-fn ~options)))
       dtype-proto/PToArray
       (->sub-array [wtr#]
         (dtype-proto/->sub-array buffer#))
       (->array-copy [wtr#]
         (dtype-proto/->array-copy buffer#))
       dtype-proto/PToNioBuffer
       (convertible-to-nio-buffer? [wtr#]
         (dtype-proto/convertible-to-nio-buffer? buffer#))
       (->buffer-backing-store [wtr#]
         (dtype-proto/->buffer-backing-store buffer#))
       dtype-proto/PToJNAPointer
       (convertible-to-data-ptr? [wtr#]
         (dtype-proto/convertible-to-data-ptr? buffer#))
       (->jna-ptr [wtr#]
         (dtype-proto/->jna-ptr buffer#))
       dtype-proto/PToWriter
       (convertible-to-writer? [wtr#] true)
       (->writer [wtr# options#]
         (dtype-proto/->writer buffer# options#))
       dtype-proto/PConvertibleToBinaryWriter
       (convertible-to-binary-writer? [wtr#] true)
       (->binary-writer [wtr# options#]
         (if (= (default-endianness (:endianness options#))
                ~endianness)
           wtr#
           (~constructor-fn buffer# options#)))

       dtype-proto/PToReader
       (convertible-to-reader? [wtr#]
         (dtype-proto/convertible-to-reader? buffer#))
       (->reader [wtr# options#]
         (dtype-proto/->reader buffer# options#))
       dtype-proto/PConvertibleToBinaryReader
       (convertible-to-binary-reader? [wtr#]
         (dtype-proto/convertible-to-binary-reader? buffer#))
       (->binary-reader [wtr# options#]
         (dtype-proto/->binary-reader buffer# options#))
       ~@body)))
