(ns clj-kstreams.core
  (:import  [org.apache.kafka.streams KafkaStreams StreamsConfig KeyValue]
            [org.apache.kafka.streams.kstream KStreamBuilder ValueMapper KeyValueMapper Predicate]
            [org.apache.kafka.connect.json JsonSerializer JsonDeserializer]
            [org.apache.kafka.common.serialization Deserializer Serdes]
            [com.fasterxml.jackson.databind ObjectMapper])
  (:require clj-kstreams.geocoder)
  (:gen-class))

(def config
  (StreamsConfig. { StreamsConfig/APPLICATION_ID_CONFIG, "map-function-scala-example",
                    StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                    StreamsConfig/ZOOKEEPER_CONNECT_CONFIG, "localhost:2181" }))

(defmacro pred
  [args & body]
  `(reify
     Predicate
     (test [this ~@args]
       (boolean (do ~@body)))))

(def builder
  (KStreamBuilder.))

(def input-topic
  (into-array String ["test-01"]))

(def mapper
  (ObjectMapper.))

(def json-serde
  (Serdes/serdeFrom (JsonSerializer.) (JsonDeserializer.)))

(def input-stream
  (.stream builder (Serdes/ByteArray) json-serde input-topic))

(def atmospheric-topic
  (into-array String ["atmospheric-data"]))
(def atmospheric-stream
  (.stream builder (Serdes/ByteArray) json-serde atmospheric-topic))

(defmulti extract-air-temp (fn [v] (let [data (.get v "data")] (if (.has data "data") :data :variables))))
(defmethod extract-air-temp :data
  [v]
  (let [air-temp (-> v (.get "data") (.get "data") (.get "air_temp"))]
    { "unit"  (.asText (.get air-temp "unit"))
      "value" (.doubleValue (.get air-temp "value"))}))
(defmethod extract-air-temp :variables
  [v]
  (let [air-temp (-> v (.get "data") (.get "variables") (.get "air_temp"))]
    { "unit"  (.asText (.get air-temp "units"))
      "value" (.doubleValue (.get air-temp "value"))}))

(defn extract-coords
  [v]
  (let [coords (-> v (.get "data") (.get "coords"))]
  { "lat"  (.doubleValue (.get coords "lat"))
    "lon"  (.doubleValue (.get coords "lon"))}))

(defn run-input
  [stream]
  (-> stream
    (.map (reify KeyValueMapper
      (apply [_ k v]
        (KeyValue. (.getBytes (.asText (.get v "type"))) v))))
    (.filter (pred [k _] (= (String. k) "atmospheric_data")))
    (.mapValues (reify ValueMapper
      (apply [_ v]
        (let [air-temp (try (extract-air-temp v) (catch Exception e nil))]
          (if (nil? air-temp) nil (merge air-temp {"coords" (extract-coords v)}))))))
    (.filterNot (pred [_ v] (nil? v)))
    (.mapValues (reify ValueMapper
      (apply [_ v]
        (.valueToTree mapper v))))
    (.to (Serdes/ByteArray) json-serde "atmospheric-data")))

(defn get-lat-lon
  [json]
  (let [coords {"lat" (-> json (.get "coords") (.get "lat") (.doubleValue))
                "lon" (-> json (.get "coords") (.get "lon") (.doubleValue))}]
    (reduce-kv (fn [m k v] (assoc m k (/ (Math/floor (* 10 v)) 10)))
      {}
      coords)))

(defn run-atmospheric
  [stream]
  (println stream)
  (-> stream
    (.map (reify KeyValueMapper
      (apply [_ k v]
        (let [lat-lon       (get-lat-lon v)
              geodata       (clj-kstreams.geocoder/from-cache (get lat-lon "lat") (get lat-lon "lon"))
              country-name  (get geodata "countryName")]
          ;(.put v "geo" (.valueToTree mapper geodata))
          (.putAll v (.valueToTree mapper geodata))
          (KeyValue. (when country-name (.getBytes country-name)) v)))))
    (.filterNot (pred [k _] (nil? k)))
    (.to (Serdes/ByteArray) json-serde "output-test")))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (run-input input-stream)
  (run-atmospheric atmospheric-stream)
  (.start (KafkaStreams. builder config))
  (println "Hello, World!"))
