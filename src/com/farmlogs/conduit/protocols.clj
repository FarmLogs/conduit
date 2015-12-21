(ns com.farmlogs.conduit.protocols)

(defprotocol WorkerResult
  (-respond! [this transport message-metadata]
    "Convey to the message broker the result of processing the message
    described by the message-metadata."))

(defprotocol ReliablePublish
  "Publish a message. Return a core.async chan that indicates if the
  publication was successful.

  The chan will yield one of #{:success :failure :timeout :closed}"
  (publish!
   [transport message headers]))
