# Conduit

![build status](https://travis-ci.org/FarmLogs/conduit.svg?branch=master)

A messaging library designed to:

- enable the creation of worker components that are isolated from the
underlying messaging library.
- enable reliable message publishing

## Requirements

- Java 1.8

## pre-commit

- Install: https://pre-commit.com/
- running locally: This will also happen automatically before committing to a branch, but you can also run the tasks with `pre-commit run --all-files`

## Usage

### Project.clj

`[com.farmlogs.conduit "0.1.0"]`

### Reliable Publishing

Create a reliable channel:

```clojure
(require '[com.farmlogs.conduit.connection :as conn])
(require '[com.stuartsierra.component :as component])

(def system
  (component/start-system
   (component/system-map
    :rmq (conn/connection "amqp://guest:guest@localhost"))))
(def reliable-chan (-> system :rmq :conn (->reliable-chan 1000)))
```

Publish using the channel:

```clojure
(a/<!! (p/publish! reliable-chan "hi!" {:exchange ""
                                        :routing-key "test"}))
```

Close the channel by using `.close`

```clojure
(.close reliable-chan)
```

### Reliable Consumers

#### Production

```clojure
(require '[com.farmlogs.conduit.connection :as conn])
(require '[com.farmlogs.conduit.subscription :refer [subscription]])
(require '[com.stuartsierra.component :as component])
(require '[clojure.core.async :as a])

(defrecord LoggingWorker
      [subscription]
    component/Lifecycle
    (start [this]
      (let [ctrl-chan (a/chan)]
        (assoc this
               :ctrl-chan ctrl-chan
               :process
               (a/go
                 (loop []
                   (let [[[result-chan msg :as event]] (a/alts! [ctrl-chan subscription])]
                     (when-not (nil? event)
                       (println "msg:" msg)
                       (a/put! result-chan :ack)
                       (recur))))))))
    (stop [{:keys [ctrl-chan process] :as this}]
      (a/close! ctrl-chan)
      (a/<!! process)
      (dissoc this :ctrl-chan :process)))

(def system
    (-> (component/system-map
         :rmq (conn/connection "amqp://guest:guest@localhost")
         :subscription (component/using (subscription {:exchange-name "foo"
                                                       :queue-name "foo"
                                                       :exchange-type "topic"
                                                       :routing-key "*"}
                                                      1024)
                                        {:rmq-connection :rmq})
         :worker (component/using (->LoggingWorker nil)
                                  [:subscription]))
        (component/start-system)))
```

#### Testing Your Workers Without RMQ

```clojure
(extend-protocol component/Lifecycle
  clojure.core.async.impl.channels.ManyToManyChannel
  (start [this] this)
  (stop [this] this))

(def system
  (-> (component/system-map
        :subscription (a/chan)
        :worker (component/using (->LoggingWorker nil)
                                 [:subscription]))
      (component/start-system)))

(let [result-chan (a/chan 1)]
  (a/put! (:subscription system) [result-chan "Heya!"])
  (= (a/<!! result-chan) :ack))

(component/stop-system system)
```

## License

Copyright © 2015 AgriSight, Inc
