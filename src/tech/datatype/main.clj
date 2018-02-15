(ns tech.datatype.main
  (:require [tech.datatype.time-test :as time-test])
  (:gen-class))


(defn -main
  [& args]
  (time-test/run-time-tests)
  (time-test/run-indexed-time-tests))
