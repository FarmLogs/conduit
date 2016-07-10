(ns com.farmlogs.conduit.protocols)

(defprotocol WorkerResult
  (-respond! [this transport message-metadata]
    "Convey to the message broker the result of processing the message
    described by the message-metadata."))

(defprotocol ReliablePublish
  (publish!
    [transport message headers]
    "Publish a message. Return a core.async chan that indicates if the
     publication was successful.

     Headers must contain the following keys:

      - exchange :: The name of the exchange to publish to.
      - routing-key :: The topic or queue name that this message should
                       be routed to.

     The chan will yield one of #{:success :failure :timeout :closed :error}")
  (publish!! [transport message headers]
    "Publish a message. Return one of:
          #{:success :failure :timeout :closed :error}
     indicating if the publication was successful.

     Headers must contain the following keys:

      - exchange :: The name of the exchange to publish to.
      - routing-key :: The topic or queue name that this message should
                       be routed to."))

(defprotocol RMQConnection
  "Return an com.rabbitmq.client.Connection."
  (connection [this]))
