(ns com.farmlogs.conduit.subscription.ack-process
  (:require [clojure.core.async :as a]
            [langohr.basic      :as rmq]
            [com.farmlogs.conduit.protocols :as p]
            [clojure.tools.logging :as log]))

(extend-protocol p/WorkerResult
  nil
  (-respond! [_ transport msg]
    (p/-respond! :nack transport msg))

  Object
  (-respond! [_ transport msg]
    (p/-respond! :nack transport msg))

  clojure.lang.Keyword
  (-respond! [this transport {:keys [delivery-tag] :as msg}]
    (case this
      :ack (rmq/ack transport delivery-tag)
      :nack (rmq/reject transport delivery-tag false)
      :retry (rmq/reject transport delivery-tag
                         (not (:redelivery? msg))))))

(defn ->responder
  "Given a transport and a result chan, start a thread that is
  responsible for doing the IO necessary to respond to a particular
  message."
  [transport result-chan]
  (a/thread
    (loop [[result msg-headers :as event] (a/<!! result-chan)]
      (when (some? event)
        (try (p/-respond! result transport msg-headers)
             (catch Throwable t
               (log/error t "Error while responding to message"
                          {:response event
                           :message-headers msg-headers})))
        (recur (a/<!! result-chan))))))

(defn ack-process
  "Given a core.async channel and an RMQ channel, start the message
  acknowledgement process. Return a channel that closes when the ack
  process terminates. The acknowledgement process can be shut down by
  closing new-messages-chan. Once all pending messages have finished
  the process will stop.

  Since RMQ uses blocking IO, the actual IO happens in a dedicated
  thread. This enables us to avoid blocking core.async
  threads. Backpressure in the system is maintained by not accepting
  new messages when there is no room in the IO buffer.

   - new-messages-chan :: a core.async channel that conveys new
                          messages into the ack process. Data on
                          this channel is of the form
                          [result-chan msg-headers]."
  [new-messages-chan buffer-size rmq-channel]
  (let [io-chan (a/chan buffer-size)
        responder-thread (->responder rmq-channel io-chan)]
    (a/go-loop [pending {new-messages-chan ::nothing}]
      (if-not (empty? pending)
        (let [[event chan] (a/alts! (vec (keys pending)))]
          (cond
            ;; Received new RMQ message
            (and (= new-messages-chan chan) event)
            (recur (assoc pending (first event) (second event)))

            ;; Ack process should stop accepting new messages
            (and (= new-messages-chan chan) (nil? event))
            (recur (dissoc pending new-messages-chan))

            ;; A result arrived
            :else
            (do (a/>! io-chan [event (get pending chan)])
                (recur (dissoc pending chan)))))
        (do
          ;; Shutdown responder thread.
          (a/close! io-chan)
          (a/<! responder-thread))))))
