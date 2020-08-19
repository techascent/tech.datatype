(defproject techascent/tech.datatype "5.18-SNAPSHOT"
  :description "Library for efficient manipulation of contiguous mutable containers of primitive datatypes."
  :url "http://github.com/techascent/tech.datatype"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure      "1.10.1"]
                 [techascent/tech.jna      "3.24"]
                 [techascent/tech.parallel "2.11"]
                 [insn                     "0.4.0"
                  :exclusions [org.ow2.asm/asm]]
                 [org.ow2.asm/asm          "7.1"]
                 [it.unimi.dsi/fastutil    "8.2.1"]
                 [kixi/stats               "0.5.4"
                  :exclusions [org.clojure/test.check]]
                 [org.clojure/math.combinatorics   "0.1.6"]
                 [org.clojure/math.numeric-tower   "0.0.4"]
                 [org.clojure/test.check           "1.0.0"]
                 [org.apache.commons/commons-math3 "3.6.1"]
                 [primitive-math                   "0.1.6"]
                 [camel-snake-kebab                "0.4.0"]
                 [org.roaringbitmap/RoaringBitmap  "0.8.13"]
                 [org.xerial.larray/larray-mmap    "0.4.1"]]
  :profiles {:dev {:dependencies [[org.bytedeco.javacpp-presets/opencv-platform
                                   "4.0.1-1.4.4"]
                                  [criterium "0.4.5"]
                                  [ch.qos.logback/logback-classic "1.1.3"]
                                  [com.clojure-goes-fast/clj-memory-meter "0.1.0"]
                                  [uncomplicate/neanderthal "0.35.0"]]
                   ;;Separating out these paths for travis
                   :test-paths ["neanderthal" "test"]}
             :travis {:dependencies [[org.bytedeco.javacpp-presets/opencv-platform
                                      "4.0.1-1.4.4"]
                                     [criterium "0.4.5"]
                                     [ch.qos.logback/logback-classic "1.1.3"]
                                     [com.clojure-goes-fast/clj-memory-meter "0.1.0"]]}
             :uberjar {:aot :all}}
  :java-source-paths ["java"])
