(ns tech.libs.buffered-image-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.tensor :as dtt]
            [tech.v2.tensor.typecast :as tens-typecast]
            [tech.libs.buffered-image :as bufimg]
            [clojure.test :refer :all]))


(deftest base-img-tensor-test
  (let [test-img (bufimg/new-image 4 4 :int-rgb)
        test-writer (tens-typecast/datatype->tensor-writer
                     :uint8
                     (bufimg/as-ubyte-tensor test-img))
        _ (.write3d test-writer 2 2 2 255)]
    (is (= [0 0 0 0 0 0 0 0 0 0 16711680 0 0 0 0 0]
           (dtype/->vector test-img)))))


(deftest tensor-and-draw-image
  (let [test-img (bufimg/load "test/data/test.jpg")
        new-img (bufimg/new-image 512 512 :int-argb)
        _ (bufimg/draw-image! test-img new-img :dst-y-offset 128)
        tens-img (bufimg/new-image 512 512 :int-argb)

        copy-tens (dtt/select (bufimg/as-ubyte-tensor tens-img)
                              (range 128 (+ 128 (.getHeight test-img)))
                              :all
                              [0 1 2])
        alpha-tens (dtt/select (bufimg/as-ubyte-tensor tens-img)
                              (range 128 (+ 128 (.getHeight test-img)))
                              :all
                              3)
        _ (is (= (dtype/shape copy-tens)
                 (dtype/shape test-img)))
        _ (dtype/copy! test-img copy-tens)
        _ (dtype/set-constant! alpha-tens 255)]
    (is (dfn/equals (mapv (comp long dfn/mean) [new-img tens-img])
                    [-5240767 -5240767]))))
