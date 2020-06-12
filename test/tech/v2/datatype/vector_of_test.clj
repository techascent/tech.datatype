(ns tech.v2.datatype.vector-of-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dfn]
            [clojure.test :refer [deftest is]]))



(deftest base-as-vector-test
  (let [vec-dtypes [:boolean :byte :short :int :long
                    :float :double]]
    (doseq [dtype vec-dtypes]
      (let [src-seq (if-not (= :boolean dtype)
                      (range 10)
                      (flatten (repeat 5 [true false])))
            lhs (dtype/make-container :java-array dtype src-seq)
            rhs (apply vector-of dtype src-seq)]
        (is (= (dtype/get-datatype lhs)
               (dtype/get-datatype rhs)))
        (is (= (vec lhs)
               (vec (dtype/->reader rhs))))))))
