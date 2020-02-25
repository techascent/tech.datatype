(ns tech.v2.tensor.dimensions.global-to-local-test
  (:require [tech.v2.tensor.dimensions :as dims]
            [tech.v2.tensor.dimensions.global-to-local :as gtol]
            [tech.v2.tensor.dimensions.analytics :as dims-analytics]
            [tech.v2.datatype.functional :as dtype-fn]
            [clojure.test :refer [deftest is]]
            [criterium.core :as crit])
  (:import [tech.v2.datatype LongReader]))


(set! *unchecked-math* true)


(defn compare-reader-impls
  [base-dims expected-reduced-shape]
  (let [base-dims-reader (dims/get-elem-dims-global->local base-dims)
        reduced-dims (dims-analytics/reduce-dimensionality base-dims)
        {:keys [constructor-args]
         :as ast-data} (gtol/dims->global->local-transformation
                        base-dims)
        default-reader (gtol/elem-idx->addr-fn reduced-dims)
        ast-reader ((gtol/get-or-create-reader-fn ast-data) constructor-args)]
    (is (= expected-reduced-shape
           (->> reduced-dims
                (map (fn [[k v]] [k (vec v)]))
                (into {}))))
    (is (dtype-fn/equals base-dims-reader default-reader))
    (is (dtype-fn/equals default-reader ast-reader))
    {:base-dims-reader base-dims-reader
     :default-reader default-reader
     :ast-reader ast-reader}))


(deftest strided-image-test
  (compare-reader-impls (dims/dimensions [2 4 4]
                                         :strides [32 4 1])
                        {:shape [2 16]
                         :strides [32 1]
                         :offsets []
                         :max-shape [2 16]
                         :max-shape-strides [16 1]}))


(deftest strided-image-reverse-rgb-test
    (compare-reader-impls (dims/dimensions [2 4 [3 2 1 0]]
                                           :strides [32 4 1])
                          {:shape [2 4 [3 2 1 0]]
                           :strides [32 4 1]
                           :offsets []
                           :max-shape [2 4 4]
                           :max-shape-strides [16 4 1]}))


(deftest strided-image-reverse-rgb--most-sig-dim-test
  (compare-reader-impls (dims/dimensions [[1 0] 4 4]
                                         :strides [32 4 1])
                        {:shape [[1 0] 16]
                         :strides [32 1]
                         :offsets []
                         :max-shape [2 16]
                         :max-shape-strides [16 1]}))

;;TODO - check broadcasting on leading dimension
(deftest leading-bcast
  (compare-reader-impls (dims/dimensions [2 4 4]
                                         :strides [32 4 1]
                                         :max-shape [4 4 4])
                        {:shape [2 16]
                         :strides [32 1]
                         :offsets []
                         :max-shape [4 16]
                         :max-shape-strides [16 1]})

  (compare-reader-impls (dims/dimensions [2 4 4]
                                         :strides [16 4 1]
                                         :max-shape [4 4 4])
                        {:shape [32]
                         :strides [1]
                         :offsets []
                         :max-shape [64]
                         :max-shape-strides [1]}))


(deftest offsets
  (compare-reader-impls (dims/dimensions [2 4 4]
                                         :strides [32 4 1]
                                         :offsets [0 0 1]
                                         :max-shape [4 4 4])
                        {:shape [2 4 4],
                         :strides [32 4 1],
                         :offsets [0 0 1],
                         :max-shape [4 4 4],
                         :max-shape-strides [16 4 1]}))


(comment
  (do
    (println "Dimension indexing system reader timings")
    (let [base-dims (dims/dimensions [256 256 4]
                                     :strides [8192 4 1])
          ^LongReader base-dims-reader (dims/get-elem-dims-global->local base-dims)

          {:keys [constructor-args]
           :as ast-data} (gtol/dims->global->local-transformation
                          base-dims)
          ^LongReader default-reader (apply gtol/elem-idx->addr-fn constructor-args)
          ^LongReader ast-reader ((gtol/get-or-create-reader-fn ast-data)
                                  constructor-args)
          n-elems (.lsize base-dims-reader)
          read-all-fn (fn [^LongReader rdr]
                        (dotimes [idx n-elems]
                          (.read rdr idx)))]
      (println "Base Reader:")
      (crit/quick-bench (read-all-fn base-dims-reader)
                        :verbose)
      (println "Default Reader:")
      (crit/quick-bench (read-all-fn default-reader)
                        :verbose)
      (println "AST Reader:")
      (crit/quick-bench (read-all-fn ast-reader)
                        :verbose)))

  (do
    (println "Dimension indirect indexing system reader timings")
    (let [base-dims (dims/dimensions [256 256 [3 2 1 0]]
                                     :strides [8192 4 1])
          ^LongReader base-dims-reader (dims/get-elem-dims-global->local base-dims)

          {:keys [constructor-args]
           :as ast-data} (gtol/dims->global->local-transformation
                          base-dims)
          ^LongReader default-reader (apply gtol/elem-idx->addr-fn constructor-args)
          ^LongReader ast-reader ((gtol/get-or-create-reader-fn ast-data)
                                  constructor-args)
          n-elems (.lsize base-dims-reader)
          read-all-fn (fn [^LongReader rdr]
                        (dotimes [idx n-elems]
                          (.read rdr idx)))]
      (println "Base Reader:")
      (crit/quick-bench (read-all-fn base-dims-reader)
                        :verbose)
      (println "Default Reader:")
      (crit/quick-bench (read-all-fn default-reader)
                        :verbose)
      (println "AST Reader:")
      (crit/quick-bench (read-all-fn ast-reader)
                        :verbose)))


  )
