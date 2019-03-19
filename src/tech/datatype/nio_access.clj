(ns tech.datatype.nio-access
  (:require [tech.datatype.casting :as casting]))


(defmacro buf-put
  [buffer idx buf-pos value]
  `(.put ~buffer (+ ~idx ~buf-pos) ~value))


(defmacro buf-get
  [buffer idx buf-pos]
  `(.get ~buffer (+ ~idx ~buf-pos)))


(defmacro datatype->pos-fn
  [datatype buffer]
  (if (casting/numeric-type? datatype)
    `(.position ~buffer)
    `0))


(defmacro datatype->read-fn
  [datatype buffer idx pos]
  (case datatype
    :boolean `(.getBoolean ~buffer ~idx)
    :object `(.get ~buffer ~idx)
    `(buf-get ~buffer ~idx ~pos)))

(defmacro datatype->write-fn
  [datatype buffer idx pos value]
  (case datatype
    :boolean `(.set ~buffer ~idx ~value)
    :object `(.set ~buffer ~idx ~value)
    `(buf-put ~buffer ~idx ~pos ~value)))


(defmacro datatype->list-read-fn
  [datatype buffer idx]
  (case datatype
    :int8 `(.getByte ~buffer ~idx)
    :int16 `(.getShort ~buffer ~idx)
    :int32 `(.getInt ~buffer ~idx)
    :int64 `(.getLong ~buffer ~idx)
    :float32 `(.getFloat ~buffer ~idx)
    :float64 `(.getDouble ~buffer ~idx)
    :boolean `(.getBoolean ~buffer ~idx)
    :object `(.get ~buffer ~idx)))
