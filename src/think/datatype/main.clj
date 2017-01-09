(ns think.datatype.main
  (:require [think.datatype.time-test :as time-test])
  (:gen-class))


(defn -main
  [& args]
  (time-test/run-time-tests)
  (time-test/run-indexed-time-tests))
