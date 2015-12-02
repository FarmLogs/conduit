(ns com.farmlogs.conduit.connection
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [langohr.core :as rmq]))

(defrecord RMQConnection
    [conn uri]
  component/Lifecycle
  (start [this]
    (log/info "Starting RMQ")
    (assoc this
           :conn (rmq/connect {:uri uri :automatically-recover true})))

  (stop [this]
    (log/info "Stopping RMQ")
    (rmq/close conn)
    (log/info "Stopped RMQ")))

(defn connection
  [uri]
  (->RMQConnection nil uri))
