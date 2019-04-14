(ns tech.apl.sparse-game-of-life
  (:require [tech.apl.game-of-life :as apl-gol]
            [tech.tensor.impl :as tens-impl]
            [tech.tensor :as tens]
            [tech.datatype.functional :as dtype-fn]
            [tech.datatype :as dtype]
            [clojure.test :refer :all]))


(with-bindings {#'tens-impl/*container-type* :sparse}
  (def range-tens (-> (tens/reshape (vec (range 9)) [3 3])
                      ;;Have to set actual numeric datatype or the sparse value ends
                      ;;up as 'nil'
                      (tens/clone :datatype :int8)))

  (def bool-tens (-> range-tens
                   (apl-gol/membership [1 2 3 4 7])
                   ;;convert to zeros/ones for display.
                   (tens/clone :datatype :int8)))


  (def take-tens (apl-gol/apl-take bool-tens [5 7]))


  (def right-rotate (apl-gol/rotate-vertical take-tens -2))

  (def down-rotate (apl-gol/rotate-horizontal right-rotate -1))

  (def R-matrix down-rotate)


  (def rotate-arg [1 0 -1])

  (def group-rotated (->> rotate-arg
                          (map (partial apl-gol/rotate-vertical down-rotate))))

  (def table-rotated (->> rotate-arg
                          (mapcat (fn [rot-amount]
                                    (->> group-rotated
                                         (mapv #(apl-gol/rotate-horizontal
                                                 % rot-amount)))))))


  (def summed (apply dtype-fn/+ table-rotated))


  (def next-gen (apl-gol/game-of-life-operator R-matrix summed))


  (def half-RR (apl-gol/apl-take R-matrix [-10 -20]))


  (def RR (-> (apl-gol/apl-take R-matrix [-10 -20])
              (apl-gol/apl-take [15 35])))

  (def bigRR (-> (apl-gol/apl-take R-matrix [-1000 -2000])
                 (apl-gol/apl-take [1500 3500]))))



(deftest game-of-life-test
  (let [end-matrix (->> (apl-gol/life-seq RR)
                        (take 1000)
                        last)]
    (is (= :sparse (dtype/buffer-type (tens-impl/tensor->buffer end-matrix))))
    (is (= apl-gol/end-state (tens/->jvm end-matrix)))))
