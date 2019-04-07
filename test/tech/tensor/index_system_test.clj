(ns tech.tensor.index-system-test
  (:require [tech.datatype :as dtype]
            [tech.tensor.index-system :as index-system]
            [tech.tensor.dimensions.shape :as shape]
            [tech.datatype.reader :as reader]
            [tech.datatype.typecast :as typecast]
            [tech.datatype.functional :as dtype-fn]
            [clojure.pprint :as pp]
            [clojure.test :refer :all]))



(defn base-index-system-test
  [shape strides max-shape]
  (let [max-stride-idx (int (dtype-fn/argmax {:datatype :int32} strides))
        n-src-buffer-elems
        (* (reader/typed-read :int32 strides max-stride-idx)
           (reader/typed-read :int32 (shape/shape->count-vec shape)
                              max-stride-idx))
        n-elems (shape/ecount max-shape)
        forward (index-system/get-elem-dims-global->local
                 {:shape shape :strides strides} max-shape)
        backward (index-system/get-elem-dims-local->global
                  {:shape shape :strides strides} max-shape)
        forward-elems (->> (range n-elems)
                           (mapv #(vector % (.globalToLocal forward (int %)))))

        backward-elems (->> (range n-src-buffer-elems)
                            (mapcat (fn [local-addr]
                                      (->> (.localToGlobal backward (int local-addr))
                                           (map (fn [global-addr]
                                                  [global-addr local-addr])))))
                            (sort-by first))]
    (is (= (vec forward-elems)
           (vec backward-elems))
        (with-out-str
          (pp/pprint {:shape shape
                      :strides strides
                      :max-shape max-shape})))))


(deftest test-the-systems
  (base-index-system-test [2 2] [2 1] [2 2])
  (base-index-system-test [2 2] [2 1] [2 4])
  (base-index-system-test [2 2] [2 1] [2 6])
  (base-index-system-test [2 2] [2 1] [4 4])
  (base-index-system-test [2 2] [1 2] [2 2])
  (base-index-system-test [2 2] [1 2] [2 4])
  (base-index-system-test [2 2] [1 2] [4 2])

  (base-index-system-test [2 [1 0]] [2 1] [4 2])
  (base-index-system-test [2 [1 0]] [1 2] [4 6])

  (base-index-system-test [2 2] [2 4] [2 2])
  (base-index-system-test [2 2] [2 4] [2 4])
  (base-index-system-test [2 2] [2 4] [4 2])

  (base-index-system-test [2 2] [4 2] [2 2])
  (base-index-system-test [2 2] [4 2] [2 4])
  (base-index-system-test [2 2] [4 2] [4 6])

  (base-index-system-test [2 [1 0]] [4 2] [2 2])


  (base-index-system-test [2 2] [3 1] [2 2])

  (base-index-system-test [2 2] [3 1] [2 4])
  (base-index-system-test [2 2] [1 3] [4 4])


  (base-index-system-test [4 4 3] [12 3 1] [4 4 3])

  (base-index-system-test [3 4 4] [1 12 3] [3 4 4]))
