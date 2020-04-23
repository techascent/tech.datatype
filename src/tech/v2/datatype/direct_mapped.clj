(ns tech.v2.datatype.direct-mapped
  (:require [tech.jna :as jna])
  (:import [tech.v2.datatype DirectMappedOps]
           [com.sun.jna Native]))



(defn- setup-direct-mapping!
  []
  (let [library (jna/load-library (jna/c-library-name))]
    (com.sun.jna.Native/register DirectMappedOps library)
    :ok))

(def direct-mapping (delay (setup-direct-mapping!)))
