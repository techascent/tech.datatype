(ns tech.v2.datatype.object-datatypes
  (:require [tech.v2.datatype.array :as dtype-ary]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.protocols :as dtype-proto])
  (:import [clojure.lang Keyword Symbol]
           [java.util UUID]))



(defmacro add-object-datatype
  [obj-cls datatype constructor]
  (let [ary-type (type (make-array (resolve obj-cls) 0))]
    `(do
       (casting/add-object-datatype! ~datatype ~obj-cls ~constructor)
       ;;Ensure the scalar pathway works
       (clojure.core/extend
           ~obj-cls
         dtype-proto/PDatatype
         {:get-datatype (constantly ~datatype)})
       (dtype-ary/extend-object-array-type ~ary-type)
       (dtype-ary/add-object-array-datatype-override! ~obj-cls ~datatype)
       (dtype-ary/add-array-constructor!
        ~datatype
        #(dtype-ary/make-object-array-of-type ~obj-cls %1
                                              (merge  {:constructor ~constructor}
                                                      %2))))))

(defn- construct-object
  ([] nil)
  ([item] item))

(defn- construct-keyword
  ([] nil)
  ([item] (keyword item)))

(defn- construct-symbol
  ([] nil)
  ([item] (symbol item)))

(defn- construct-uuid
  ([] nil)
  ([item]
   (cond
     (instance? UUID item) item
     (string? item) (UUID/fromString item)
     (nil? item) item
     :else
     (throw (Exception. (format "Unable to construct UUID from %s" item))))))


(add-object-datatype Object :object construct-object)
(add-object-datatype String :string str)
(add-object-datatype Keyword :keyword construct-keyword)
(add-object-datatype Symbol :symbol construct-symbol)
(add-object-datatype UUID :uuid construct-uuid)
