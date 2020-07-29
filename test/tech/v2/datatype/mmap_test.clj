(ns tech.v2.datatype.mmap-test
  (:require [tech.v2.datatype.mmap :as mmap]
            [tech.v2.datatype :as dtype]
            [tech.resource :as resource]
            [clojure.test :refer [deftest is]]))



(deftest base-mmap-data
  (resource/stack-resource-context
   (let [proj-mmap (mmap/mmap-file "project.clj")
         initial-str (String. (dtype/->array-copy proj-mmap))]
     (is (.startsWith initial-str"(defproject")))))
