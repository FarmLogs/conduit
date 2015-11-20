(ns com.farmlogs.messaging.subscription
  (:require [com.stuartsierra.component :as component]
            [com.farmlogs.messaging.subscription.ack-process :refer [ack-process]]))

(defrecord Subscription
    [])
