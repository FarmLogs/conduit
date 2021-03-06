(ns com.farmlogs.conduit.subscription
  (:require [com.stuartsierra.component :as component]
            [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [clojure.core.async.impl.protocols :as impl]
            [com.farmlogs.conduit.subscription.ack-process :refer [->ack-process]]
            [com.farmlogs.conduit.payload :refer [read-payload]]
            [langohr
             [basic :as rmq.basic]
             [channel :as rmq.chan]
             [consumers :as rmq.consumer]])
  (:import [java.util Base64]))

(defn ->handle-message-fn
  [new-message-chan pending-messages-chan]
  (fn [ch metadata ^bytes payload]
    (let [result-chan (a/chan 1)]
      (if-not (a/>!! new-message-chan [result-chan metadata])
        (log/error "Failed to put message onto new-message-chan"
                   (pr-str {:payload (-> (Base64/getEncoder)
                                         (.encodeToString payload))
                            :headers metadata}))
        (try (when-not (a/>!! pending-messages-chan
                              [result-chan (read-payload (:content-type metadata) payload)])
               (a/>!! result-chan :retry))
             (catch Throwable t
               (log/errorf t (str "Exception while enqueuing"
                                  " new message: '%s' content-type: %s")
                           (-> (Base64/getEncoder)
                               (.encodeToString payload))
                           (:content-type metadata))
               (a/>!! result-chan :drop)))))))

(defn consume-ok
  [queue-name consumer-tag]
  (log/infof "Consuming %s with tag %s" queue-name consumer-tag))

(defn cancel
  [cancelled-promise queue-name consumer-tag]
  (deliver cancelled-promise true)
  (log/warnf "Cancelled consumer tag: %s on queue: %s" consumer-tag queue-name))

(defn cancel-ok
  [cancelled-promise queue-name consumer-tag]
  (deliver cancelled-promise true)
  (log/infof "Consumer tag: %s on queue: %s shutdown" consumer-tag queue-name))

(defn- make-channel
  [conn prefetch-count]
  (doto (rmq.chan/open conn)
    (rmq.basic/qos prefetch-count)))

(defrecord Subscription
    [rmq-connection rmq-chan queue-name buffer-size]
  component/Lifecycle
  (start [this]
    (log/infof "Starting subscription on queue '%s'" queue-name)
    (let [rmq-chan (or rmq-chan (make-channel (:conn rmq-connection) buffer-size))
          new-messages (a/chan buffer-size)
          pending-messages (a/chan buffer-size)
          ack-process (->ack-process new-messages buffer-size rmq-chan)
          cancelled? (promise)
          rmq-consumer (rmq.consumer/create-default
                        rmq-chan
                        {:handle-consume-ok-fn (partial consume-ok queue-name)
                         :handle-cancel-ok-fn (partial cancel-ok cancelled? queue-name)
                         :handle-delivery-fn (->handle-message-fn new-messages pending-messages)})
          consumer-tag (rmq.basic/consume rmq-chan queue-name rmq-consumer)]



      (log/infof "Started subscription on queue '%s' with tag '%s'"
                 queue-name
                 consumer-tag)
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
    (log/infof "Shutting down subscription '%s' on queue '%s'"
               consumer-tag
               queue-name)
    (rmq.basic/cancel rmq-chan consumer-tag)

    ;; Close pending-messages so we can drain it
    (a/close! pending-messages)

    ;; Drain pending-messages, telling the ack-process to :retry any
    ;; unhandled messages.
    (loop [[result-chan :as unhandled-msg] (a/<!! pending-messages)]
      (when-not (nil? unhandled-msg)
        (a/>!! result-chan :retry)
        (recur (a/<!! pending-messages))))

    ;; Wait until the server acknowledges the cancellation of our subscription
    @cancelled?

    ;; Let the ack-process know it can terminate
    (a/close! new-messages)

    ;; wait for termination
    (a/<!! ack-process)

    ;; All messages are drained. We can close the RMQ channel.
    (rmq.chan/close rmq-chan)
    (log/info (format "Shut down subscription '%s' on queue '%s'"
                      consumer-tag
                      queue-name))

    (dissoc this
            :rmq-consumer :ack-process :new-messages :pending-messages
            :cancelled? :consumer-tag))

  impl/ReadPort
  (take! [{:keys [pending-messages]} fn1-handler]
    (impl/take! pending-messages fn1-handler)))

(def ^:static +queue-config-required-keys+
  #{:exchange-name :queue-name :exchange-type})

(defn subscription
  ([queue-name buffer-size]
   (subscription nil queue-name buffer-size))
  ([rmq-chan queue-name buffer-size]
   (assert (number? buffer-size))

   (->Subscription nil rmq-chan queue-name buffer-size)))
