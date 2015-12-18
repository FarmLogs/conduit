(ns com.farmlogs.conduit.protocols)

(defprotocol WorkerResult
  (-respond! [this transport message-metadata]
    "Convey to the message broker the result of processing the message
    described by the message-metadata."))

(defprotocol ReliablePublish
  (publish!
   [transport message headers]))
