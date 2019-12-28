(ns tech.v2.datatype.object-arrays-test
  (:require [clojure.test :refer :all]
            [tech.v2.datatype.typed-buffer :as typed-buffer]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.list]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.binary-op :as binary-op]))


(deftest boolean-array-test
  (let [test-ary (dtype/make-array-of-type :boolean 5)]
    (is (= [false false false false false]
           (dtype/->vector test-ary)))
    (is (= :boolean
           (dtype/get-datatype test-ary)))
    (dtype/set-value! test-ary 2 true)
    (is (= [false false true false false]
           (dtype/->vector test-ary)))
    (is (= [false false true false false]
           (-> (dtype/copy! test-ary (dtype/make-array-of-type :boolean 5))
               (dtype/->vector))))))


(deftest string-array-test
  (let [test-ary (dtype/make-array-of-type :string 5)]
    (is (= ["" "" "" "" ""]
           (dtype/->vector test-ary)))
    (is (= :string (dtype/get-datatype test-ary)))
    (dtype/set-value! test-ary 3 "hi")
    (is (= ["" "" "" "hi" ""]
           (dtype/->vector test-ary)))

    (let [sub-buf (dtype-proto/sub-buffer test-ary 2 3)]
      (is (= :string (dtype/get-datatype sub-buf)))
      (dtype/set-value! sub-buf 0 "bye!")
      (is (= ["" "" "bye!" "hi" ""]
             (dtype/->vector test-ary)))
      (is (= ["bye!" "hi" ""]
             (dtype/->vector sub-buf)))))
  (let [test-ary (dtype/make-array-of-type :string ["a" "b" "c"])
        test-rdr (->> test-ary
                      (unary-op/unary-reader
                       :string
                       (.concat x "_str")))
        test-iter (->> test-ary
                       (unary-op/unary-iterable
                        :string
                        (.concat x "_str")))]
    (is (= :string (dtype/get-datatype test-rdr)))
    (is (= ["a_str" "b_str" "c_str"]
           (vec test-rdr)))
    (is (= :string (dtype/get-datatype test-iter)))
    (is (= ["a_str" "b_str" "c_str"]
           (vec test-iter))))
  (let [test-ary (dtype/make-array-of-type :string ["a" "b" "c"])
        test-rdr (binary-op/binary-reader
                  :string
                  (str x "_" y)
                  test-ary test-ary)
        test-iterable (binary-op/binary-iterable
                       :string
                       (str x "_" y)
                       test-ary test-ary)]
    (is (= :string (dtype/get-datatype test-rdr)))
    (is (= :string (dtype/get-datatype test-iterable)))
    (is (= ["a_a" "b_b" "c_c"]
           (vec test-rdr)))
    (is (= ["a_a" "b_b" "c_c"]
           (vec test-iterable)))))


(deftest new-string-container
  (is (= ["a_str" "b_str" "c_str"]
         (->> (dtype/make-array-of-type :string ["a" "b" "c"])
              (unary-op/unary-reader
               :string
               (.concat x "_str"))
              (dtype/make-container :java-array :string)
              vec))))


(deftest object-array-test
  (let [test-ary (dtype/make-array-of-type Object 5)]
    (is (= [nil nil nil nil nil]
           (dtype/->vector test-ary)))
    (is (= :object (dtype/get-datatype test-ary)))
    (dtype/set-value! test-ary 3 "hi")
    (is (= [nil nil nil "hi" nil]
           (dtype/->vector test-ary)))
    (let [sub-buf (dtype-proto/sub-buffer test-ary 2 3)]
      (is (= :object (dtype/get-datatype sub-buf)))
      (dtype/set-value! sub-buf 0 "bye!")
      (is (= [nil nil "bye!" "hi" nil]
             (dtype/->vector test-ary)))
      (is (= ["bye!" "hi" nil]
             (dtype/->vector sub-buf)))))
  (let [test-ary (into-array Object (repeat 10 (set (range 10))))]
    (is (= 10 (dtype/ecount test-ary)))
    (is (dtype/->array test-ary))))
