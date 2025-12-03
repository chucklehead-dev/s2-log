(ns chucklehead.xtdb
  (:require [xtdb.log :as log]
            [xtdb.time :as time])
  (:import [chucklehead.xtdb.s2 S2Cluster$ClusterFactory S2Cluster$LogFactory]))

(defmethod log/->log-cluster-factory ::s2-cluster
  [_ {:keys [token basin max-append-in-flight-bytes append-timeout read-buffer-bytes retry-delay]}]
  (cond-> (S2Cluster$ClusterFactory. token basin)
    max-append-in-flight-bytes (.maxAppendInFlightBytes max-append-in-flight-bytes)
    append-timeout (.appendTimeout (time/->duration append-timeout))
    read-buffer-bytes (.readBufferBytes read-buffer-bytes)
    retry-delay (.retryDelay (time/->duration retry-delay))))

(defmethod log/->log-factory ::s2-log
  [_ {:keys [cluster stream epoch]}]
  (cond-> (S2Cluster$LogFactory. (str (symbol cluster)) stream)
    epoch (.epoch epoch)))
