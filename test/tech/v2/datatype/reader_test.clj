(ns tech.v2.datatype.reader-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.unary-op :as unary-op]
            [clojure.test :refer :all]))


(deftest reader-sub-buffers
  (is (= [5 6]
         (-> (unary-op/unary-reader :int32 (+ x 2) [1 2 3 4])
             (dtype/sub-buffer 2 2)
             vec))))
