(ns tech.tensor
  (:require [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.sparse.protocols :as sparse-proto]
            [tech.tensor.dimensions :as dims]
            [tech.tensor.index-system :as index-system]
            [clojure.core.matrix.protocols :as mp]))
