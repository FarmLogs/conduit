(ns com.farmlogs.conduit.reliable-channel
  "The reliable-chan that is returned by the ->reliable-chan function,
  enables code that sends a message to be notified about whether the
  message was successfully received by the message broker."
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [com.farmlogs.conduit.protocols :as p]
            [langohr
             [basic :as rmq.basic]
             [channel :as rmq.chan]])
  (:import [com.rabbitmq.client
            ConfirmListener
            ReturnListener]
           [java.util Base64]))

(defrecord Confirm [tag multiple? result])
(defrecord Await [tag async-chan])

(defn handle-confirm
  "Alert each Await that its message has been confirmed and return a
  map containing Awaits that haven't been alerted yet.

   - awaits :: a sorted map of the form {<delivery-tag> <Await>}
   - confirmed-tag :: the tag being confirmed by the broker
   - multiple? :: a boolean. if true, all tags <= confirmed tag are
                  being confirmed else just the confirmed-tag
   - result :: one of #{:success :failure :timeout} indicates whether
               the message was handled by the broker."
  [awaits confirmed-tag multiple? result]
  (reduce-kv (fn [awaits tag {:keys [async-chan] :as await}]
               (if (<= tag confirmed-tag)
                 (do (a/put! async-chan result)
                     (a/close! async-chan)
                     (dissoc awaits tag))
                 (reduced awaits)))
             awaits
             (if multiple?
               awaits
               (select-keys awaits [confirmed-tag]))))

(defn fan-out
  "Broadcast each event on source to all destinations."
  [source destinations]
  (let [m (a/mult source)]
    (doseq [d destinations]
      (a/tap m d))))

(defn handle-await-fn
  "Return a function that given an Await record sets a timeout period
  after which we consider the publication a failure.

   To achieve this, we create a go-block that listens for either a
   timeout or the result returned by the broker."
  [timeouts timeout-window]
  (fn
    [{:keys [tag async-chan] :as await}]
    (let [handled (a/chan 1)
          cancel-timeout (a/chan 1)]
      (fan-out handled [async-chan cancel-timeout])
      (a/go
        (a/alt!
          ;; The broker has responded. No need to continue waiting
          cancel-timeout ([_] :no-op)

          ;; The broker has not responded within the
          ;; timeout-window. Alert the await-process that it can stop
          ;; awaiting on this publication.
          (a/timeout timeout-window) ([_] (a/>! timeouts tag))

          ;; If both the cancel-timeout and the a/timeout have events,
          ;; be sure to respond to the cancel-timeout event. Otherwise,
          ;; we may falsely return a :timeout when the publication was
          ;; successful.
          :priority true))
      (assoc await :async-chan handled))))

(defn- await-process
  "Handle the arrival of new Awaits and Confirms. When the awaits
  channel is closed, return the map containing the Awaits that are
  still pending."
  [confirms awaits timeouts timeout-window]
  (let [handle-await (handle-await-fn timeouts timeout-window)]
    (a/go-loop [awaiting (sorted-map)]
      (a/alt!
        confirms ([{:keys [tag multiple? result] :as confirm}]
                  (recur (try (handle-confirm awaiting tag multiple? result)
                              (catch Throwable t
                                (log/error t
                                           "Error while handling confirm:"
                                           (pr-str confirm))
                                awaiting))))
        awaits ([{:keys [tag] :as await}]
                (if (nil? await)
                  awaiting
                  (recur (try (assoc awaiting tag (handle-await await))
                              (catch Throwable t
                                (log/error t "Error while handling await:"
                                           (pr-str await))
                                (a/>! (:async-chan await) :error)
                                awaiting)))))
        timeouts ([tag]
                  (recur (try (handle-confirm awaiting tag false :timeout)
                              (catch Throwable t
                                (log/error t "Error while handling timeout:" tag)
                                awaiting))))))))

(defn- shutdown-process
  "Wait until all publications have been confirmed or timeout."
  [awaiting confirms timeouts]
  (a/go-loop [awaiting awaiting]
    (when-not (empty? awaiting)
      (a/alt!
        confirms ([{:keys [tag multiple? result] :as confirm}]
                  (recur (try (handle-confirm awaiting tag multiple? result)
                              (catch Throwable t
                                (log/error t
                                           "Error while handling confirm:"
                                           (pr-str confirm))
                                awaiting))))
        timeouts ([tag]
                  (recur (try (handle-confirm awaiting tag false :timeout)
                              (catch Throwable t
                                (log/error t "Error while handling timeout:" tag)
                                awaiting))))))))

(defn ->await-process
  [confirms awaits timeout-window]
  (let [timeouts (a/chan)]
    (a/go
      ;; Run the await-process until the awaits channel is closed. At
      ;; that point begin the shutdown process.
      (-> (a/<! (await-process confirms awaits timeouts timeout-window))
          (shutdown-process confirms timeouts)
          (a/<!)))))

(defn- ->confirm-listener
  "Listen for Confirm messages.

   A message is Ack'd once the broker routes the message or determines
  the message is unroutable.

   A message is Nack'd when the broker encounters an
  exception (eg. out of memory or a disk failure) while trying to
  route the message."
  [confirmation-chan]
  (reify ConfirmListener
    (handleAck [_ tag multiple?]
      (a/>!! confirmation-chan
             (->Confirm tag multiple? :success)))
    (handleNack [_ tag multiple?]
      (a/>!! confirmation-chan
             (->Confirm tag multiple? :failure)))))

(defn- ->return-listener
  "Listen for Return messages, and indicate the message publication was a :failure.

   A message is returned when the broker is unable to route the
  message to a queue. If this happens there's probably a configuration
  problem."
  [confirmation-chan]
  (reify ReturnListener
    (handleReturn [_ reply-code reply-text exchange routing-key properties body]
      (let [headers (.getHeaders properties)]
        (a/>!! confirmation-chan (->Confirm (get headers (str ::delivery-tag))
                                            false
                                            :failure))
        (log/error "Returned publication:"
                   (pr-str
                    {:reply-code reply-code
                     :reply-text reply-text
                     :exchange exchange
                     :routing-key routing-key
                     :body  (.encodeToString (Base64/getEncoder) body)}))))))

(defrecord ReliableChan
    [await-chan await-process rmq-chan]
  java.lang.AutoCloseable
  (close [_]
    ;; Acquire the lock, because we shouldn't close the
    ;; rmq-channel in the middle of trying to publish on it.
    (locking rmq-chan
      (a/close! await-chan)
      (a/<!! await-process)
      (when (.isOpen rmq-chan)
        (.close rmq-chan))))

  component/Lifecycle
  (start [{:keys [rmq-connection timeout-window] :as this}]
    (let [rmq-chan (rmq.chan/open rmq-connection)
          confirmation-chan (a/chan)
          await-chan (a/chan)
          await-process (->await-process confirmation-chan await-chan timeout-window)]
      (doto rmq-chan
        (.addConfirmListener (->confirm-listener confirmation-chan))
        (.addReturnListener (->return-listener confirmation-chan))
        (.confirmSelect))
      (assoc this
             :await-chan await-chan
             :await-process await-process
             :rmq-chan rmq-chan)))
  (stop [this]
    (when (and rmq-chan (rmq.chan/open? rmq-chan))
      (.close this))
    (assoc this
           :await-chan nil
           :await-process nil
           :rmq-chan nil))

  p/ReliablePublish
  (publish! [_ message headers]
    (let [async-chan (a/chan 1)]
      (locking rmq-chan
        (if (.isOpen rmq-chan)
          (let [next-tag (.getNextPublishSeqNo rmq-chan)]
            (a/>!! await-chan (->Await next-tag async-chan))
            (rmq.basic/publish rmq-chan
                               (:exchange headers)
                               (:routing-key headers)
                               message
                               (merge {:mandatory true
                                       :persistent true
                                       :headers {(str ::delivery-tag) next-tag}}
                                      headers)))
          (do (a/put! async-chan :closed)
              (a/close! async-chan))))
      async-chan))
  (publish!! [this message headers]
    (a/<!! (p/publish! this message headers))))


(defn ->reliable-chan
  "Return an unstarted ReliableChannel."
  ([timeout-window]
   (map->ReliableChan {:timeout-window timeout-window}))
  ([rmq-connection timeout-window]
   (map->ReliableChan {:rmq-connection rmq-connection :timeout-window timeout-window})))

(comment
  (do (require '[com.farmlogs.conduit.connection :as conn])
      (require '[com.stuartsierra.component :as component])

      (def system
        (component/start-system
         (component/system-map
          :rmq (conn/connection "amqp://guest:guest@localhost"))))
      (def reliable-chan (-> system :rmq :conn (->reliable-chan 1000))))



  (a/<!! (p/publish! reliable-chan "hi!" {:exchange ""
                                          :routing-key "test"}))

  (.close reliable-chan)

  (component/stop-system system)
  )
