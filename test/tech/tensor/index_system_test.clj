(ns tech.tensor.index-system-test
  (:require [tech.datatype :as dtype]
            [tech.tensor.dimensions :as dims]
            [tech.tensor.dimensions.shape :as shape]
            [tech.datatype.reader :as reader]
            [tech.datatype.typecast :as typecast]
            [tech.datatype.functional :as dtype-fn]
            [clojure.pprint :as pp]
            [clojure.test :refer :all]))



(defn base-index-system-test
  ([shape strides offsets max-shape]
   (let [max-stride-idx (int (dtype-fn/argmax {:datatype :int32} strides))
         n-src-buffer-elems
         (* (reader/typed-read :int32 strides max-stride-idx)
            (reader/typed-read :int32 (shape/shape->count-vec shape)
                               max-stride-idx))
         n-elems (shape/ecount max-shape)
         dimensions (dims/dimensions shape :strides strides :offsets offsets :max-shape max-shape)
         forward (dims/->global->local dimensions)
         backward (dims/->local->global dimensions)
         forward-elems (->> (range n-elems)
                            (mapv #(vector % (.read forward (int %)))))

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
                       :offsets offsets
                       :max-shape max-shape})))))
  ([shape strides max-shape]
   (base-index-system-test shape strides
                           (reader/make-const-reader
                            0 :int32 (dtype/ecount shape))
                           max-shape)))


(deftest test-the-systems
  (base-index-system-test [2 2] [2 1] [2 2])
  ;;rotate down
  (base-index-system-test [2 2] [2 1] [0 1] [2 2])
  (base-index-system-test [3 3] [3 1] [0 1] [3 3])
  (base-index-system-test [3 3] [3 1] [2 0] [3 3])

  (base-index-system-test [3 3] [3 1] [0 1] [3 3])
  (base-index-system-test [2 2] [2 1] [2 4])
  (base-index-system-test [2 2] [2 1] [2 6])
  (base-index-system-test [2 2] [2 1] [4 4])
  (base-index-system-test [2 2] [1 2] [2 2])
  (base-index-system-test [2 2] [1 2] [2 4])
  (base-index-system-test [2 2] [1 2] [4 2])

  (base-index-system-test [2 [1 0]] [2 1] [4 2])
  (base-index-system-test [2 [1 0]] [1 2] [4 6])

  ;;Check offseting *and* indexing
  (base-index-system-test [2 [1 0]] [2 1] [0 1] [4 2])
  (base-index-system-test [2 [1 0]] [1 2] [0 1] [4 6])

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

  ;;Normal image
  (base-index-system-test [4 4 3] [12 3 1] [4 4 3])

  ;;Image in channels-first
  (base-index-system-test [3 4 4] [1 12 3] [3 4 4]))
