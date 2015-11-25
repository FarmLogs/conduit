(ns com.farmlogs.conduit.subscription-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [com.farmlogs.conduit.subscription :refer :all]
            [com.farmlogs.conduit.payload :refer [read-payload]]
            [clojure.core.async :as a]))

(deftest test-handle-message
  (testing "Happy path"
    (let [new-message-chan (a/chan 1)
          pending-messages-chan (a/chan 1)
          metadata {:content-type "text/plain"}
          payload (.getBytes "foo")
          _ (-> (->handle-message-fn new-message-chan pending-messages-chan)
                (.invoke nil metadata payload))
          ack-process-input (a/<!! new-message-chan)
          worker-input      (a/<!! pending-messages-chan)]

      (is (identical? (first ack-process-input) (first worker-input))
          "The result-chan handed to Workers be handed to the ack-process.")
      (is (identical? (second ack-process-input) metadata)
          "The ack-process should receive the message's metadata.")
      (is (= (second worker-input) (String. payload))
          "The workers should receive the message's payload.")))

  (testing (str "If the ack-process doesn't get the message-headers,"
                " the workers shouldn't.")
    (let [new-message-chan (doto (a/chan 1) (a/close!))
          pending-messages-chan (a/chan 1)
          metadata {:content-type "text/plain"}
          payload (.getBytes "foo")
          log-output (atom [])]

      (with-redefs [log/log* (fn [_ level _ message]
                               (swap! log-output conj [level message]))]

        (-> (->handle-message-fn new-message-chan pending-messages-chan)
            (.invoke nil metadata payload)))
      (a/close! pending-messages-chan)

      (is (nil? (a/<!! new-message-chan))
          "No message arrived at the ack-process.")
      (is (nil? (a/<!! pending-messages-chan))
          "No message arrived at the workers.")))

  (testing (str "If we fail to enqueue the message payload, we tell the ack process"
                " to :retry the message.")
    (let [new-message-chan (a/chan 1)
          pending-messages-chan (doto (a/chan 1) (a/close!))
          metadata {:content-type "text/plain"}
          payload (.getBytes "foo")
          result (promise)]

      (a/go (let [[result-chan _] (a/<! new-message-chan)]
              (deliver result (a/<! result-chan))))

      (with-redefs [log/log* (constantly nil)]
        (-> (->handle-message-fn new-message-chan pending-messages-chan)
            (.invoke nil metadata payload)))
      (a/close! new-message-chan)

      (is (nil? (a/<!! pending-messages-chan))
          "The message didn't get to the workers.")
      (is (= :retry @result)
          "The ack-process was told to :retry the message.")))

  (testing (str "If we have an exception while enqueuing the message payload,"
                " we tell the ack process to :drop the message.")
    (let [new-message-chan (a/chan 1)
          pending-messages-chan (a/chan 1)
          metadata {:content-type "text/plain"}
          payload (.getBytes "foo")
          result (promise)]

      (a/go (let [[result-chan _] (a/<! new-message-chan)]
              (deliver result (a/<! result-chan))))

      (with-redefs [log/log* (constantly nil)
                    read-payload (fn [& _] (throw (ex-info "broken" {})))]
        (-> (->handle-message-fn new-message-chan pending-messages-chan)
            (.invoke nil metadata payload)))
      (a/close! new-message-chan)
      (a/close! pending-messages-chan)

      (is (nil? (a/<!! pending-messages-chan))
          "The message didn't get to the workers.")
      (is (= :drop @result)
          "The ack-process was told to :drop the message."))))
