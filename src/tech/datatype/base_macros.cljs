(ns tech.datatype.base-macros)

(defmacro try-catch-any
  [try-body & catch-body]
  `(try
     ~try-body
     (catch :default ~(first catch-body)
       ~@(rest catch-body))))
