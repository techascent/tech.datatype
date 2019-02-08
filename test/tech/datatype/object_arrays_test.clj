(ns tech.datatype.object-arrays-test
  (:require [clojure.test :refer :all]
            [tech.datatype.java-primitive :as java-primitive]
            [tech.datatype :as dtype]))


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
           (dtype/->vector test-ary)))))


(deftest object-array-test
  (let [test-ary (dtype/make-array-of-type Object 5)]
    (is (= [nil nil nil nil nil]
           (dtype/->vector test-ary)))
    (is (= Object (dtype/get-datatype test-ary)))
    (dtype/set-value! test-ary 3 "hi")
    (is (= [nil nil nil "hi" nil]
           (dtype/->vector test-ary)))))
