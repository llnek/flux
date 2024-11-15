;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defproject io.czlab/flux "2.2.0"

  :license {:url "https://www.apache.org/licenses/LICENSE-2.0.txt"
            :name "Apache License"}

  :description "A simple workflow engine"
  :url "https://github.com/llnek/flux"

  :dependencies [[io.czlab/basal "2.2.0"]]

  :plugins [[cider/cider-nrepl "0.50.2" :exclusions [nrepl]]
            [lein-codox "0.10.8"]
            [lein-cljsbuild "1.1.8"]]

  :profiles {:provided {:dependencies [[org.clojure/clojure "1.12.0"]]}
             :uberjar {:aot :all}}

  :global-vars {*warn-on-reflection* true}
  :target-path "out/%s"
  :aot :all

  :coordinate! "czlab"
  :omit-source true

  :java-source-paths ["src/main/java" "src/test/java"]
  :source-paths ["src/main/clojure"]
  :test-paths ["src/test/clojure"]

  :test-selectors {:core :test-core}

  :jvm-opts ["-Dlog4j.configurationFile=file:attic/log4j2.xml"]
  :javac-options ["-source" "16"
                  "-target" "22"
                  "-Xlint:unchecked" "-Xlint:-options" "-Xlint:deprecation"])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


