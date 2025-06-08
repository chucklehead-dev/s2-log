(ns chucklehead.xtdb
  (:require [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import [xtdb.api Xtdb$Config]
           [chucklehead.xtdb.s2 S2Log]))

(defmethod xtn/apply-config! ::s2-log
  [^Xtdb$Config config _ {:keys [token basin stream
                                 max-append-in-flight-bytes
                                 append-timeout
                                 read-buffer-bytes
                                 retry-delay
                                 epoch]}]
  (doto config
    (.setLog (cond-> (S2Log/s2 token basin stream)
               max-append-in-flight-bytes (.maxAppendInFlightBytes max-append-in-flight-bytes)
               append-timeout (.appendTimeout (time/->duration append-timeout))
               read-buffer-bytes (.readBufferBytes read-buffer-bytes)
               retry-delay (.retryDelay (time/->duration retry-delay)
               epoch (.epoch epoch))))))