(ns com.farmlogs.messaging.payload)

(defmulti read-payload
  (fn [type data] type))

(defmethod read-payload "text/plain"
  [_ data]
  (String. data))
