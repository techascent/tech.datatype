(ns think.datatype.validation.core
  (:require [think.gate.core :as gate])
  (:gen-class))

(defn on-foo
  [params] ;; consider destructuring your arguments
  (+ (:a params) 41))

(def routing-map
  {"foo" #'on-foo})

(defn gate
  []
  (gate/open #'routing-map))

(defn other-thing
  "Example of rendering a different component with the gate/component multimethod."
  []
  (gate/open #'routing-map :variable-map {:render-page "otherthing"}))

(defn -main
  [& args]
  (println (gate)))
