;; (ns tech.sparse.buffer-test
;;   (:require [tech.sparse.buffer :as sparse-buf]
;;             [tech.sparse.protocols :as sparse-proto]
;;             [tech.datatype :as dtype]
;;             [tech.compute.driver :as compute-drv]
;;             [clojure.test :refer :all]))


;; (deftest base-sparse-sanity
;;   (let [test-sparse (sparse-buf/make-sparse-buffer :float32 [1 0 1 0 1 0 1])
;;         next-sparse (sparse-buf/make-sparse-buffer :float32 [0 2 0 4 0 6])
;;         ;;Make a strided buffer.
;;         stride-sparse (sparse-proto/set-stride next-sparse 3)]
;;     (is (= [[0 0] [1 2] [2 4] [3 6]] (vec (sparse-proto/index-seq test-sparse))))
;;     (is (= [[0 0] [1 2] [2 4]] (vec (-> (compute-drv/sub-buffer test-sparse 0 6)
;;                                         (sparse-proto/index-seq)))))
;;     (is (= [[0 1] [1 3] [2 5]] (vec (sparse-proto/index-seq next-sparse))))
;;     (is (= [[1 1]] (sparse-proto/index-seq stride-sparse)))
;;     (is (= (mapv float [1 0 1 0 1 0 1]) (dtype/->vector test-sparse)))
;;     (is (= (mapv float [0 2 0 4 0 6]) (dtype/->vector next-sparse)))
;;     (is (= (mapv float [0 4]) (dtype/->vector stride-sparse)))
;;     (let [test-sparse (dtype/copy! next-sparse test-sparse)]
;;       (is (= (mapv float [0 2 0 4 0 6 1])
;;              (dtype/->vector test-sparse))))

;;     (is (= (mapv float [4 0 6])
;;            (-> (compute-drv/sub-buffer test-sparse 3 3)
;;                (compute-drv/sub-buffer 0 3)
;;                dtype/->vector)))
;;     (is (= [[0 0] [1 2]]
;;            (-> (compute-drv/sub-buffer test-sparse 3 3)
;;                (compute-drv/sub-buffer 0 3)
;;                sparse-proto/index-seq)))
;;     (let [new-buffer (dtype/copy! (float-array (range 5 8))
;;                                   (compute-drv/sub-buffer test-sparse 3 3))]
;;       (is (= (mapv float [5 6 7])
;;              (dtype/->vector new-buffer)))
;;       (is (= (mapv float [0 2 0 5 6 7 1])
;;              (dtype/->vector test-sparse)))))
;;   (let [test-sparse (sparse-buf/make-sparse-buffer :float32 [1 0 2 0 3 0 4 0])
;;         new-buffer (-> (sparse-proto/set-stride test-sparse 2)
;;                        (compute-drv/sub-buffer 1 3))]
;;     (is (= (mapv float [2 3 4])
;;            (dtype/->vector new-buffer))))
;;   (let [test-sparse (sparse-buf/make-sparse-buffer :float32 [1 0 1 0 1 0 1 0])
;;         new-buffer (dtype/set-constant! (-> (sparse-proto/set-stride test-sparse 2)
;;                                             (compute-drv/sub-buffer 1 3))
;;                                         0 0 3)]
;;     (is (= (mapv float [0 0 0])
;;            (dtype/->vector new-buffer)))
;;     (is (= (mapv float [1 0 0 0 0 0 0 0])
;;            (dtype/->vector test-sparse))))
;;   (let [test-sparse (sparse-buf/make-sparse-buffer :float32 [1 0 1 0 1 0 1 0])
;;         new-buffer (dtype/copy! (float-array (range 5 8))
;;                                 (-> (sparse-proto/set-stride test-sparse 2)
;;                                     (compute-drv/sub-buffer 1 3)))]
;;     (is (= (mapv float [5 6 7])
;;            (dtype/->vector new-buffer)))
;;     (is (= (mapv float [1 0 5 0 6 0 7 0])
;;            (dtype/->vector test-sparse)))))
