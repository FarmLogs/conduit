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
          ack-process (ack-process input nil)
          output (a/chan 1)
          send-result (partial result output)
          result-chan (a/chan 1)]

      (a/>!! input [result-chan :foo])
      (a/>!! result-chan (send-result :ack))
      (a/close! input)
      (is (= [:ack :foo] (take-with-timeout output 10)))
      (is (closed? ack-process))))

  (testing "All messages get ack'd even if ack input closes before worker result."
    (let [input (a/chan 1)
          ack-process (ack-process input nil)
          output (a/chan 1)
          send-result (partial result output)
          result-chan (a/chan 1)]

      (a/>!! input [result-chan :foo])
      (a/close! input)
      (is (not (closed? ack-process)))
      (a/>!! result-chan (send-result :ack))
      (is (= [:ack :foo] (take-with-timeout output 10)))
      (is (closed? ack-process))))

  (testing "Ack process keeps working if there's an exception in WorkerResult."
    (let [input (a/chan 1)
          ack-process (ack-process input nil)
          output (a/chan 1)
          send-result (partial result output)
          result-chan1 (a/chan 1)
          result-chan2 (a/chan 1)]

      (a/>!! input [result-chan1 :foo])
      (a/>!! input [result-chan2 :bar])
      (a/close! input)
      (is (not (closed? ack-process)))
      (a/>!! result-chan2 (broken-result))
      (a/>!! result-chan1 (send-result :ack))
      (is (= [:ack :foo] (take-with-timeout output 10)))
      (is (closed? ack-process)))))
