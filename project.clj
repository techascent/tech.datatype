(defproject techasent/tech.datatype "0.3.18-SNAPSHOT"
  :description "Library for efficient manipulation of contiguous mutable containers of primitive datatypes."
  :url "http://github.com/thinktopic/think.datatype"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [net.mikera/vectorz-clj "0.47.0"]
                 [net.mikera/core.matrix "0.60.3"]]


  :java-source-paths ["java"]

  ;;If you want to run perf tests
;  :aot [think.datatype.main]
;  :main think.datatype.main

  :think/meta {:type :library :tags [:low-level]})
