(defproject clj-kstreams "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [ [org.clojure/clojure "1.8.0"]
                  [org.apache.kafka/kafka-streams "0.11.0.0"]
                  [org.apache.kafka/kafka-clients "0.11.0.0"]
                  [org.clojars.tavisrudd/redis-clojure "1.3.2"]]
  :main ^:skip-aot clj-kstreams.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
