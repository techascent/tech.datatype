(ns tech.datatype.nio-access)


(defmacro buf-put
  [buffer idx buf-pos value]
  `(.put ~buffer (+ ~idx ~buf-pos) ~value))


(defmacro buf-get
  [buffer idx buf-pos]
  `(.get ~buffer (+ ~idx ~buf-pos)))
