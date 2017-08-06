(ns clj-kstreams.core
  (:import (org.apache.kafka.streams KafkaStreams StreamsConfig)
           (org.apache.kafka.streams.kstream KStreamBuilder ValueMapper)
           (org.apache.kafka.common.serialization Serdes))
  (:gen-class))

(def config
  (StreamsConfig. { StreamsConfig/APPLICATION_ID_CONFIG, "map-function-scala-example",
                    StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                    StreamsConfig/ZOOKEEPER_CONNECT_CONFIG, "localhost:2181" }))

(def builder
  (KStreamBuilder.))

(def input-topic
  (into-array String ["test-01"]))

(def execute
  (->
    (.stream builder input-topic)
    (.to "clj-test")))

(def stream
  (KafkaStreams. builder config))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  execute
  (.start stream)
  (println "Hello, World!"))
