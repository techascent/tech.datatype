(ns tech.v2.datatype.datetime.packed-test
  (:require [tech.v2.datatype :as dtype]
            [tech.v2.datatype.datetime :as dt]
            [clojure.test :refer [deftest is]])
  (:import [tech.v2.datatype
            PackedInstant PackedLocalDate
            PackedLocalTime PackedLocalDateTime]))
