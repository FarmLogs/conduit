(ns com.farmlogs.conduit.subscription
  (:require [com.stuartsierra.component :as component]
            [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [clojure.core.async.impl.protocols :as impl]
            [com.farmlogs.conduit.subscription.ack-process :refer [ack-process]]
            [com.farmlogs.conduit.payload :refer [read-payload]]
            [langohr
             [basic :as rmq.basic]
             [channel :as rmq.chan]
             [consumers :as rmq.consumer]
             [exchange :as rmq.exch]
             [queue :as rmq.queue]])
  (:import [java.util Base64]))

(defn ->handle-message-fn
  [new-message-chan pending-messages-chan]
  (fn [ch metadata ^bytes payload]
    (let [result-chan (a/chan 1)]
      (when-not (a/>!! new-message-chan [result-chan metadata])
        (log/error "Failed to put message onto new-message-chan"
                   (pr-str {:payload (-> (Base64/getEncoder)
                                         (.encodeToString payload))
                            :headers metadata})))
      (try (when-not (a/>!! pending-messages-chan
                            [result-chan (read-payload (:content-type metadata) payload)])
             (a/>!! result-chan :retry))
           (catch Throwable t
             (log/error t (format (str "Exception while enqueuing"
                                       " new message: '%s' content-type: %s")
                                  (-> (Base64/getEncoder)
                                      (.encodeToString payload))
                                  (:content-type metadata)))
             (a/>!! result-chan :nack))))))

(defn consume-ok
  [{:keys [queue-name] :as config} consumer-tag]
  (log/info (format "Consuming %s with tag %s" queue-name consumer-tag)))

(defn cancel
  [cancelled-promise {:keys [queue-name] :as queue-config} consumer-tag]
  (deliver cancelled-promise true)
  (log/warn (format "Cancelled consumer tag: %s on queue: %s" consumer-tag queue-name)))

(defn cancel-ok
  [cancelled-promise {:keys [queue-name] :as queue-config} consumer-tag]
  (deliver cancelled-promise true)
  (log/info (format "Consumer tag: %s on queue: %s shutdown" consumer-tag queue-name)))

(defn- make-channel
  [conn {:keys [exchange-name exchange-type] :as config}]
  (let [chan (rmq.chan/open conn)]
    (rmq.exch/declare chan exchange-name exchange-type config)
    (rmq.queue/declare chan (:queue-name config) config)
    (rmq.queue/bind chan (:queue-name config) exchange-name config)
    chan))

(defrecord Subscription
    [rmq-connection rmq-chan queue-config buffer-size]
  component/Lifecycle
  (start [this]
    (log/info (format "Starting subscription on queue '%s'" (:queue-name queue-config)))
    (let [rmq-chan (or rmq-chan (make-channel (:conn rmq-connection) queue-config))
          new-messages (a/chan)
          pending-messages (a/chan buffer-size)
          ack-process (ack-process new-messages rmq-chan)
          cancelled? (promise)
          rmq-consumer (rmq.consumer/create-default
                        rmq-chan
                        {:handle-consume-ok-fn (partial consume-ok queue-config)
                         :handle-cancel-ok-fn (partial cancel-ok cancelled? queue-config)
                         :handle-delivery-fn (->handle-message-fn new-messages pending-messages)})
          consumer-tag (rmq.basic/consume rmq-chan (:queue-name queue-config) rmq-consumer)]



      (log/info (format "Started subscription on queue '%s' with tag '%s'"
                        (:queue-name queue-config)
                        consumer-tag))
      (assoc this
             :rmq-chan rmq-chan
             :rmq-consumer rmq-consumer
             :ack-process ack-process
             :new-messages new-messages
             :pending-messages pending-messages
             :cancelled? cancelled?
             :consumer-tag consumer-tag)))

  (stop [{:keys [cancelled? consumer-tag ack-process new-messages pending-messages] :as this}]
    ;; By the time this is called the com.stuartsierra.component
    ;; library has turned off the workers depending on this subscription.

    ;; Shut down the RMQ consumer, so we don't get any more messages delivered
    (log/info (format "Shutting down subscription '%s' on queue '%s'"
                      consumer-tag
                      (:queue-name queue-config)))
    (rmq.basic/cancel rmq-chan consumer-tag)

    ;; Wait until the server acknowledges the cancellation of our subscription
    @cancelled?

    ;; Close pending-messages so we can drain it
    (a/close! pending-messages)

    ;; Drain pending-messages, telling the ack-process to :retry any
    ;; unhandled messages.
    (loop [[result-chan :as unhandled-msg] (a/<!! pending-messages)]
      (when-not (nil? unhandled-msg)
        (a/>!! result-chan :retry)
        (recur (a/<!! pending-messages))))

    ;; Let the ack-process know it can terminate
    (a/close! new-messages)

    ;; wait for termination
    (a/<!! ack-process)

    ;; All messages are drained, we can close the channel
    (rmq.chan/close rmq-chan)
    (log/info (format "Shut down subscription '%s' on queue '%s'"
                      consumer-tag
                      (:queue-name queue-config)))

    (dissoc this
            :rmq-consumer :ack-process :new-messages :pending-messages
            :cancelled? :consumer-tag))

  impl/ReadPort
  (take! [{:keys [pending-messages]} fn1-handler]
    (impl/take! pending-messages fn1-handler)))

(comment
  (do (require '[com.farmlogs.conduit.connection :as conn])
      (defrecord EchoWorker
          [subscription]
        component/Lifecycle
        (start [this]
          (let [ctrl-chan (a/chan 1)]
            (assoc this
                   :ctrl-chan ctrl-chan
                   :process (a/go
                              (loop []
                                (let [[[result-chan msg :as event]] (a/alts! [ctrl-chan subscription])]
                                  (when-not (nil? event)
                                    ;(println "msg:" msg)
                                    (a/put! result-chan :ack)
                                    (recur))))
                              (log/info "worker stopping")))))
        (stop [{:keys [ctrl-chan process] :as this}]
          (a/close! ctrl-chan)
          (a/<!! process)
          (dissoc this :ctrl-chan :process)))

      (def system
        (-> (component/system-map
             :rmq (conn/connection "amqp://guest:guest@localhost")
             :subscription (component/using (map->Subscription {:queue-config
                                                                {:exchange-name "foo"
                                                                 :queue-name "foo"
                                                                 :exchange-type "topic"
                                                                 :routing-key "*"}
                                                                :buffer-size 1})
                                            {:rmq-connection :rmq})
             :worker (component/using (->EchoWorker nil)
                                      [:subscription]))
            (component/start-system))))

  (component/stop-system system)


  (def channel (rmq.chan/open (-> system :rmq :conn)))
  (dotimes [i 10000]
    (rmq.basic/publish channel "foo" "*" (str "hi " i) {:content-type "text/plain"}))
  )
