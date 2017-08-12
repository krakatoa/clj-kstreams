(ns clj-kstreams.geocoder
  (:require [redis.core :as redis]))

(defn transform-map
  [m]
  (reduce-kv (fn [m k v] (assoc m k (clojure.string/replace v #"\"" "")))
    {}
    m))

(defn from-cache
  [lat lon]
  (let [res (redis/with-server
              { :host "127.0.0.1"
                :port 6379 }
              (redis/hgetall (str "coords:" lat "," lon)))]
    (if (not (empty? res)) (transform-map res) {})))
