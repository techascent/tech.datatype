(ns tech.v2.datatype.object-arrays-test
  (:require [clojure.test :refer :all]
            [tech.v2.datatype.typed-buffer :as typed-buffer]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.list]
            [tech.v2.datatype.protocols :as dtype-proto]))


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
             (dtype/->vector sub-buf))))))


(deftest object-array-test
  (let [test-ary (dtype/make-array-of-type Object 5)]
    (is (= [nil nil nil nil nil]
           (dtype/->vector test-ary)))
    (is (= Object (dtype/get-datatype test-ary)))
    (dtype/set-value! test-ary 3 "hi")
    (is (= [nil nil nil "hi" nil]
           (dtype/->vector test-ary)))
    (let [sub-buf (dtype-proto/sub-buffer test-ary 2 3)]
      (is (= Object (dtype/get-datatype sub-buf)))
      (dtype/set-value! sub-buf 0 "bye!")
      (is (= [nil nil "bye!" "hi" nil]
             (dtype/->vector test-ary)))
      (is (= ["bye!" "hi" nil]
             (dtype/->vector sub-buf))))))
