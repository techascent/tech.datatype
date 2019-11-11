(ns tech.v2.tensor.integration-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.tensor :as tens]
            [tech.v2.tensor.typecast :as dtt-typecast]
            [clojure.test :refer :all]))


(deftest tensor->array-and-back
  (let [test-tens (tens/->tensor (partition 3 (range 9)))]
    (doseq [row (tens/rows test-tens)]
      (is (= (tens/->jvm row)
             (vec (dtype/make-container :java-array :float32 row)))))

    (doseq [col (tens/columns test-tens)]
      (is (= (tens/->jvm col)
             (vec (dtype/make-container :java-array :float32 col)))))))


(deftest tensor->list-and-back
  (let [test-tens (tens/->tensor (partition 3 (range 9)))]
    (doseq [row (tens/rows test-tens)]
      (is (= (tens/->jvm row)
             (vec (dtype/make-container :list :float32 row)))))

    (doseq [col (tens/columns test-tens)]
      (is (= (tens/->jvm col)
             (vec (dtype/make-container :list :float32 col)))))))


(deftest block-rows->tensor
  (let [block-rows (repeatedly 4 #(int-array (range 5)))
        tensor (tens/new-tensor (dtype/shape block-rows)
                                :datatype :int32)]
    (dtype/copy-raw->item! block-rows tensor)
    (is (= [[0 1 2 3 4]
            [0 1 2 3 4]
            [0 1 2 3 4]
            [0 1 2 3 4]]
           (tens/->jvm tensor)))))


(deftest modify-time-test
  (let [source-image (tens/new-tensor [512 288 3] :datatype :uint8)
        ;; Reader composition is lazy so the expression below reads from
        ;; the test image (ecount image) times.  It writes to the destination
        ;; once and the byte value is completely transformed from the src image
        ;; to the dest while in cache.  Virtual table lookups happen multiple
        ;; times per byte value.  ;; It is important to realize that under the
        ;; covers the image is stored as bytes.  These are read in a datatype-aware
        ;; way and converted to their appropriate unsigned values automatically
        ;; and when writter they are checked to ensure they are within range.
        ;; There are 2N checks for correct datatype in this pathway; everything else
        ;; is read/operated on as a short integer.
        reader-composition  #(-> source-image
                                 (tens/select :all :all [2 1 0])
                                 (dfn/+ 50)
                                 ;;Clamp top end to 0-255
                                 (dfn/min 255)
                                 (dtype/copy! (dtype/from-prototype source-image)))

        inline-fn #(as-> source-image dest-image
                     (tens/select dest-image :all :all [2 1 0])
                     (unary-op/unary-reader
                      :int16 (-> (+ x 50)
                                 (min 255)
                                 unchecked-short)
                      dest-image)
                     (dtype/copy! dest-image
                                  ;;Note from-prototype fails for reader chains.
                                  ;;So you have to copy or use an actual image.
                                  (dtype/from-prototype source-image)))]
    ;;warmup a little.
    (reader-composition)
    (inline-fn)
    (clojure.pprint/pprint
     {:reader-composition (with-out-str (time (dotimes [iter 10]
                                                (reader-composition))))
      :inline-fn (with-out-str (time (dotimes [iter 10]
                                       (inline-fn))))})))


(deftest buffer-descriptor
  ;; Test that we can get buffer descriptors from tensors.  We should also be able
  ;; to get buffer descriptors from nio buffers if they are direct mapped.
  (let [test-tensor (tens/->tensor (->> (range 9)
                                        (partition 3)))]
    (is (not (dtype/as-buffer-descriptor test-tensor)))
    (is (-> (tens/ensure-buffer-descriptor test-tensor)
            :ptr))
    (let [new-tens (tens/buffer-descriptor->tensor
                    (tens/ensure-buffer-descriptor test-tensor))]
      (is (dfn/equals test-tensor new-tens))
      (let [trans-tens (tens/transpose new-tens [1 0])
            trans-desc (dtype/as-buffer-descriptor trans-tens)]
        (is (= {:datatype :float64, :shape [3 3], :strides [8 24]}
               (dissoc trans-desc :ptr)))))))


(deftest tensor-writers
  []
  (let [test-dim 10
        test-tens (tens/new-tensor [test-dim test-dim 4] :datatype :uint8)
        reshape-tens (tens/reshape test-tens [(* test-dim test-dim) 4])
        test-indexes [35 69 83 58 57 13 64 68 48 55 20 33 2 36
                      21 17 88 94 91 85]
        idx-tens (tens/select reshape-tens (long-array test-indexes) :all)
        writer (dtt-typecast/datatype->tensor-writer :uin8 idx-tens)]
    (dotimes [iter (count test-indexes)]
      (.write2d writer iter 3 255))
    (is (dfn/equals (sort test-indexes)
                    (dfn/argfilter #(not= 0 %)
                                   (tens/select test-tens :all :all 3))))))


(deftest normal-tensor-select
  (let [test-tens (-> (tens/->tensor (partition 3 (range 9)))
                      (tens/select (concat [0 1] [0 2]) :all))]
    (is (dfn/equals (tens/->tensor [[0.000 1.000 2.000]
                                    [3.000 4.000 5.000]
                                    [0.000 1.000 2.000]
                                    [6.000 7.000 8.000]])
                    test-tens))))


(defn strided-tensor-copy-time-test
  []
  (let [src-tens (-> (tens/new-tensor [2048 2048 4] :datatype :uint8)
                     (tens/select (range 256 (* 2 256))
                                  (range 256 (* 2 256))
                                  :all))
        dst-tens (tens/new-tensor [256 256 4] :datatype :uint8)]
    ;; (dtype/copy! src-tens dst-tens)
    (time (dotimes [iter 100]
            (dtype/copy! src-tens dst-tens)))
    :ok
    ))

(comment
  (strided-tensor-copy-time-test)
  )
