(defproject techascent/tech.datatype "4.43"
  :description "Library for efficient manipulation of contiguous mutable containers of primitive datatypes."
  :url "http://github.com/tech-ascent/tech.datatype"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-tools-deps "0.4.1"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files [:install :user :project]}
  :profiles {:uberjar {:aot :all}}
  :java-source-paths ["java"])
