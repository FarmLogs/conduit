(ns com.farmlogs.conduit.reliable-channel
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [com.farmlogs.conduit.protocols :as p]
            [langohr
             [basic :as rmq.basic]
             [channel :as rmq.chan]])
  (:import [com.rabbitmq.client ConfirmListener
            Channel]))

(defrecord Confirm [tag multiple? result])
(defrecord Await [tag async-chan])

(defn handle-confirm
  "Alert each Await that it's message has been confirmed.

   - awaits :: a sorted map of the form {<delivery-tag> <Await>}
   - confirmed-tag :: the tag being confirmed by the broker
   - multiple? :: a boolean. if true, all tags <= confirmed tag are
                  being confirmed
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

(defn await-process
  [confirms awaits timeout-window]
  (let [timeouts (a/chan)
        handle-await (handle-await-fn timeouts timeout-window)]
    (a/go
      (try
        (loop [awaiting (sorted-map)]
          (recur
           (a/alt!
             confirms ([{:keys [tag multiple? result] :as confirm}]
                       (handle-confirm awaiting tag multiple? result))
             awaits ([{:keys [tag] :as await}]
                     (assoc awaiting tag (handle-await await)))
             timeouts ([tag]
                       (handle-confirm awaiting tag false :timeout))
             :priority true)))
        (catch Throwable t
          (log/error t "await-process terminating:"))))))

(defn- ->confirm-listener
  [confirmation-chan]
  (reify ConfirmListener
    (handleAck [_ tag multiple?]
      (a/>!! confirmation-chan
             (->Confirm tag multiple? :success)))
    (handleNack [_ tag multiple?]
      (a/>!! confirmation-chan
             (->Confirm tag multiple? :failure)))))

(defn ->reliable-chan
  [rmq-connection timeout-window]
  (let [rmq-chan (rmq.chan/open rmq-connection)
        confirmation-chan (a/chan)
        await-chan (a/chan)]
    (await-process confirmation-chan await-chan timeout-window)
    (doto rmq-chan
      (.addConfirmListener (->confirm-listener confirmation-chan))
      (.confirmSelect))
    (reify
      p/ReliablePublish
      (publish! [_ message headers]
        (let [async-chan (a/chan 1)]
          (locking rmq-chan
            (if (.isOpen rmq-chan)
              (let [msg-number (.getNextPublishSeqNo rmq-chan)]
                (a/>!! await-chan (->Await msg-number async-chan))
                (rmq.basic/publish rmq-chan
                                   (:exchange headers)
                                   (:routing-key headers)
                                   message
                                   headers))
              (a/put! async-chan :closed)))
          async-chan))

      java.lang.AutoCloseable
      (close [_]
        ;; Acquire the lock, because we shouldn't close the
        ;; rmq-channel in the middle of trying to publish on it.
        (locking rmq-chan (.close rmq-chan))))))
