(defproject thinktopic/think.datatype.validation "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0-alpha17"]
                 [thinktopic/think.datatype "0.3.12-SNAPSHOT"]
                 [thinktopic/think.gate "0.1.4-SNAPSHOT"]]

  :source-paths ["src"]

  :figwheel {:css-dirs ["resources/public/css"]}
  :cljsbuild {:builds
              [{:id "dev"
                :figwheel true
                :source-paths ["src" "checkouts/think.datatype/src"]
                :compiler {:main "think.datatype.validation.gate-frontend"
                           :asset-path "out"
                           :output-to "resources/public/js/app.js"
                           :output-dir "resources/public/out"}}]}

  :main ^:skip-aot think.datatype.validation.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
