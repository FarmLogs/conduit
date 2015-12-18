(ns com.farmlogs.conduit.reliable-channel-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [com.farmlogs.conduit.reliable-channel :refer :all]
            [clojure.core.async :as a]))


(deftest test-await-process-happy-path
  (testing "Happy path"
    (let [confirm-chan (a/chan)
          await-chan (a/chan)
          timeout-window 5
          await-proc (await-process confirm-chan await-chan timeout-window)

          await1 (->Await 1 (a/chan 1))
          await2 (->Await 2 (a/chan 1))

          conf1 (->Confirm (:tag await1) false :success)
          conf2 (->Confirm (:tag await2) false :success)]

      (doto await-chan
        (a/>!! await1)
        (a/>!! await2))

      (doto confirm-chan
        (a/>!! conf1)
        (a/>!! conf2))

      (are [result await] (= result (a/<!! (:async-chan await)))

        :success await1
        :success await2))))

(deftest test-await-process-timeout
  (testing "Timeouts fire"
    (let [confirm-chan (a/chan)
          await-chan (a/chan)
          timeout-window 5
          await-proc (await-process confirm-chan await-chan timeout-window)

          await1 (->Await 1 (a/chan 1))
          await2 (->Await 2 (a/chan 1))

          conf1 (->Confirm (:tag await1) false :success)]

      (doto await-chan
        (a/>!! await1)
        (a/>!! await2))

      (doto confirm-chan
        (a/>!! conf1))

      (are [result await] (= result (a/<!! (:async-chan await)))

        :success await1
        :timeout await2))))

(deftest test-await-process-multiple
  (testing "We properly handle confirms with multiple? set to true."
    (let [confirm-chan (a/chan)
          await-chan (a/chan)
          timeout-window 5
          await-proc (await-process confirm-chan await-chan timeout-window)

          await1 (->Await 1 (a/chan 1))
          await2 (->Await 2 (a/chan 1))
          await3 (->Await 3 (a/chan 1))
          await4 (->Await 4 (a/chan 1))

          conf3 (->Confirm (:tag await3) true :success)]

      (doto await-chan
        (a/>!! await1)
        (a/>!! await2)
        (a/>!! await3)
        (a/>!! await4))

      (doto confirm-chan
        (a/>!! conf3))

      (are [result await] (= result (a/<!! (:async-chan await)))

        :success await1
        :success await2
        :success await3
        :timeout await4)))

  (let [confirm-chan (a/chan)
        await-chan (a/chan)
        timeout-window 5
        await-proc (await-process confirm-chan await-chan timeout-window)

        await1 (->Await 1 (a/chan 1))
        await2 (->Await 2 (a/chan 1))
        await3 (->Await 3 (a/chan 1))
        await4 (->Await 4 (a/chan 1))

        conf2 (->Confirm (:tag await2) false :failure)
        conf3 (->Confirm (:tag await3) true :success)]

    (doto await-chan
      (a/>!! await1)
      (a/>!! await2)
      (a/>!! await3)
      (a/>!! await4))

    (doto confirm-chan
      (a/>!! conf2)
      (a/>!! conf3))

    (are [result await] (= result (a/<!! (:async-chan await)))

      :success await1
      :failure await2
      :success await3
      :timeout await4)))
