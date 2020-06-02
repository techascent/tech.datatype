(ns tech.v2.datatype.streams-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.typecast :as typecast]
            [clojure.test :refer [deftest is]]))


(defn typed-stream-sum
  [rdr]
  (case (dtype/get-datatype rdr)
    :boolean (-> (typecast/datatype->reader :boolean rdr)
                 (.typedStream)
                 (.sum))
    :int8 (-> (typecast/datatype->reader :int8 rdr)
              (.typedStream)
              (.sum))
    :int16 (-> (typecast/datatype->reader :int16 rdr)
               (.typedStream)
               (.sum))
    :int32 (-> (typecast/datatype->reader :int32 rdr)
               (.typedStream)
               (.sum))
    :int64 (-> (typecast/datatype->reader :int64 rdr)
               (.typedStream)
               (.sum))
    :float32 (-> (typecast/datatype->reader :float32 rdr)
                 (.typedStream)
                 (.sum))
    :float64 (-> (typecast/datatype->reader :float64 rdr)
                 (.typedStream)
                 (.sum))
    (-> (typecast/datatype->reader :object rdr)
        (.typedStream)
        (.reduce (reify java.util.function.BinaryOperator
                   (apply [this lhs rhs]
                     (+ lhs rhs)))))))


(deftest basic-datatype-streams
  (let [dtype-list [:int8 :int16 :int32 :int64
                    :float32 :float64]
        readers (concat (->> dtype-list
                             (map #(dtype/make-container :java-array % (range 10)))
                             (map dtype/->reader))
                        [(dtype/->reader (vec (range 10)))
                         (dtype/->reader (boolean-array (map even? (range 10))))])]
    (is (= [45
            45
            45
            45
            45.0
            45.0
            (java.util.Optional/of 45)
            5]
           (vec (mapv typed-stream-sum readers))))))
