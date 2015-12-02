(ns com.farmlogs.conduit.subscription.ack-process-test
  (:require [clojure.test :refer :all]
            [com.farmlogs.conduit.subscription.ack-process :refer :all]
            [com.farmlogs.conduit.protocols :as p]
            [clojure.core.async :as a]))

(defn result
  [output result]
  (reify p/WorkerResult
    (-respond! [_ _ msg]
      (a/put! output [result msg]))))

(defn broken-result
  []
  (reify p/WorkerResult
    (-respond! [_ _ msg]
      (throw (ex-info "broken!"
                      {:msg msg})))))

(defn take-with-timeout
  [chan timeout-ms]
  (a/alt!!
    chan ([v] v)
    (a/timeout timeout-ms) ([v] ::timeout)))

(deftest test-ack-process
  (testing "Happy Path"
    (let [input (a/chan 1)
          ack-process (->ack-process input 1 nil)
          output (a/chan 1) ;; Capture results sent to the msg broker
          send-result (partial result output)
          result-chan (a/chan 1)]

      ;; Simulate arrival of a message with headers :foo
      (a/>!! input [result-chan :foo])

      ;; Simulate a worker finishing work on the message
      (a/>!! result-chan (send-result :ack))

      ;; Begin shutdown sequence for the ack-process
      (a/close! input)
      (is (= [:ack :foo] (take-with-timeout output 10))
          "We should ACK the :foo message.")
      (is (nil? (a/<!! ack-process))
          "The ack-process should be terminated")))

  (testing "Messages get the correct acknowledgement."
    (let [input (a/chan 1)
          ack-process (->ack-process input 1 nil)
          output (a/chan 1) ;; Capture results sent to the msg broker
          send-result (partial result output)
          result-chan1 (a/chan 1)
          result-chan2 (a/chan 1)]

      ;; Simulate the arrival of w messages
      (a/>!! input [result-chan1 :foo])
      (a/>!! input [result-chan2 :bar])

      ;; Simulate workers finishing processing the messages
      (a/>!! result-chan1 (send-result :ack))
      (a/>!! result-chan2 (send-result :drop))

      ;; Begin shutdown sequence for the ack-process
      (a/close! input)

      (is (nil? (a/<!! ack-process))
          "The ack-process should be terminated.")
      (a/close! output)
      (is (= #{[:ack :foo] [:drop :bar]}
             (a/<!! (a/into #{} output)))
          "We should ACK :foo and NACK :bar.")))

  (testing "All messages get ack'd even if ack input closes before worker result."
    (let [input (a/chan 1)
          ack-process (->ack-process input 1 nil)
          output (a/chan 1) ;; Capture results sent to the msg broker
          send-result (partial result output)
          result-chan (a/chan 1)]

      ;; Simulate the arrival of a message
      (a/>!! input [result-chan :foo])

      ;; Begin shutdown sequence for the ack-process
      (a/close! input)
      (is (= ::timeout (take-with-timeout ack-process 10))
          "ack-process should still be running, b/c not all messages
          have been processed.")

      ;; Simulate the result of processing the message
      (a/>!! result-chan (send-result :ack))
      (is (= [:ack :foo] (take-with-timeout output 10))
          "We should ACK the :foo message.")
      (is (nil? (a/<!! ack-process))
          "Now that all messages have been processed, the ack-process
          should be terminated.")))

  (testing "Ack process keeps working if there's an exception in WorkerResult."
    (let [input (a/chan 1)
          ack-process (->ack-process input 1 nil)
          output (a/chan 2) ;; Capture results sent to the msg broker
          send-result (partial result output)
          result-chan1 (a/chan 1)
          result-chan2 (a/chan 1)]

      ;; Simulate the arrival of 2 messages
      (a/>!! input [result-chan1 :foo])
      (a/>!! input [result-chan2 :bar])

      ;; Begin shutdown sequence for the ack-process
      (a/close! input)

      (is (= ::timeout (take-with-timeout ack-process 10))
          "The ack-process should still be running, b/c there are
          unhandled messages.")

      ;; Simulate a worker returning some sort of unexpected result.
      (a/>!! result-chan2 (broken-result))

      ;; Simulate a worker :ack'ing :foo
      (a/>!! result-chan1 (send-result :ack))
      (is (nil? (a/<!! ack-process))
          "ack-process should be termainated.")
      (a/close! output)
      (is (= #{[:ack :foo]} (a/<!! (a/into #{} output)))
          "The :foo message should be ACK'd."))))
