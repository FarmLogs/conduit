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

(defn ack-process
  "Given a channel, start the message acknowledgement process. Return
  a channel that closes when the ack process terminates. The
  acknowledgement process can be shut down by closing
  new-messages-chan. Once all pending messages have finished the
  process will stop.

   - new-messages-chan :: a core.async channel that conveys new
                          messages into the ack process. Data on the
                          ack channel is of the form
                          [result-chan msg-headers]."
  [new-messages-chan rmq-channel]
  (a/go-loop [pending {new-messages-chan ::nothing}]
    (when-not (empty? pending)
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
          (do (try (p/-respond! event rmq-channel (get pending chan))
                   (catch Throwable t
                     (log/error t "Error while responding to message"
                                {:response event
                                 :message-headers (get pending chan)})))
              (recur (dissoc pending chan))))))))
