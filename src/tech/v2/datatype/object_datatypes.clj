(ns tech.v2.datatype.object-datatypes
  (:require [tech.v2.datatype.array :as dtype-ary]
            [tech.v2.datatype.protocols :as dtype-proto]))



(defmacro add-object-datatype
  [obj-cls datatype constructor]
  (let [ary-type (type (make-array (resolve obj-cls) 0))]
    `(do
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


(add-object-datatype Object :object construct-object)
(add-object-datatype String :string str)
