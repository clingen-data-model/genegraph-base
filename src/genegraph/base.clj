(ns genegraph.base
  (:require [genegraph.framework.event :as event]
            [genegraph.framework.storage.gcs :as gcs]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.app :as app]
            [genegraph.framework.processor :as processor]
            [genegraph.framework.env :as env]
            [hato.client :as hc]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.log :as log]
            [io.pedestal.http :as http]
            [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import [java.io File InputStream OutputStream]
           [java.nio.channels Channels])
  (:gen-class))

(def admin-env
  (if (or (System/getenv "DX_JAAS_CONFIG_DEV")
          (System/getenv "DX_JAAS_CONFIG")) ; prevent this in cloud deployments
    {:platform "prod"
     :dataexchange-genegraph (System/getenv "DX_JAAS_CONFIG")
     :local-data-path "data/"}
    {}))

(def local-env
  (case (or (:platform admin-env) (System/getenv "GENEGRAPH_PLATFORM"))
    "local" (assoc (env/build-environment "974091131481" ["dataexchange-genegraph"
                                                          "affils-service-key"])
                   :fs-handle {:type :file :base "data/base/"}
                   :local-data-path "data/")
    "dev" (assoc (env/build-environment "522856288592" ["dataexchange-genegraph"])
                 :version 1
                 :name "dev"
                 :function (System/getenv "GENEGRAPH_FUNCTION")
                 :kafka-user "User:2189780"
                 :fs-handle {:type :gcs
                             :bucket "genegraph-base-dev"}
                 :local-data-path "/data")
    "stage" (assoc (env/build-environment "583560269534" ["dataexchange-genegraph"])
                   :version 1
                   :name "stage"
                   :function (System/getenv "GENEGRAPH_FUNCTION")
                   :kafka-user "User:2592237"
                   :fs-handle {:type :gcs
                               :bucket "genegraph-base-stage"}
                   :local-data-path "/data")
    "prod" (assoc (env/build-environment "974091131481" ["dataexchange-genegraph"
                                                         "affils-service-key"])
                  :version 1
                  :name "prod"
                  :kafka-user "User:2592237"
                  :fs-handle {:type :gcs
                              :bucket "genegraph-base"}
                  :local-data-path "/data")
    {}))

(def env
  (merge local-env admin-env))

(defn qualified-kafka-name [prefix]
  (str prefix "-" (:name env) "-" (:version env)))

(def consumer-group
  (qualified-kafka-name "gg"))

(def data-exchange
  {:type :kafka-cluster
   :kafka-user (:kafka-user env)
   :common-config {"ssl.endpoint.identification.algorithm" "https"
                   "sasl.mechanism" "PLAIN"
                   "request.timeout.ms" "20000"
                   "bootstrap.servers" "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
                   "retry.backoff.ms" "500"
                   "security.protocol" "SASL_SSL"
                   "sasl.jaas.config" (:dataexchange-genegraph env)}
   :consumer-config {"key.deserializer"
                     "org.apache.kafka.common.serialization.StringDeserializer"
                     "value.deserializer"
                     "org.apache.kafka.common.serialization.StringDeserializer"}
   :producer-config {"key.serializer"
                     "org.apache.kafka.common.serialization.StringSerializer"
                     "value.serializer"
                     "org.apache.kafka.common.serialization.StringSerializer"}})

(defn output-handle [event]
  (-> event
      ::handle
      (assoc :path (get-in event [::event/data :target]))))

(defn success? [{status ::http-status}]
  (and (<= 200 status) (< status 400)))

(defn event->headers [event]
  (-> event
      (get-in [::event/data :headers])
      (update-vals #(if (keyword? %) (get env %) %))))

(defn fetch-file-fn [event]
  (log/info :fn ::fetch-file-fn
            :source (get-in event [::event/data :source])
            :name  (get-in event [::event/data :name])
            :status :started)
  (let [headers (event->headers event)
        base-opts {:http-client (hc/build-http-client {:redirect-policy :always})
                   :as :stream}
        opts (if headers (assoc base-opts :headers headers) base-opts)
        response (hc/get (get-in event [::event/data :source]) opts)]
    (when (instance? InputStream (:body response))
      (with-open [os (io/output-stream (storage/as-handle (output-handle event)))]
        (.transferTo (:body response) os)))
    (assoc event
           ::http-status (:status response))))

(def fetch-file
  (interceptor/interceptor
   {:name ::fetch-file
    :enter (fn [e] (fetch-file-fn e))}))

(defn publish-base-file-fn [event]
  (event/publish event {::event/topic :base-data
                        ::event/key (get-in event [::event/data :name])
                        ::event/data (-> event
                                         ::event/data
                                         (assoc :source (output-handle event)))}))

(def publish-base-file
  (interceptor/interceptor
   {:name ::publish-base-file
    :enter (fn [e] (publish-base-file-fn e))}))

(def fetch-base-events-topic
  {:name :fetch-base-events
   :serialization :edn
   :kafka-cluster :data-exchange
   :kafka-topic "gg-fb"
   :kafka-topic-config {"cleanup.policy" "compact"
                        "delete.retention.ms" "100"}})

(def base-data-topic
  {:name :base-data
   :serialization :edn
   :kafka-cluster :data-exchange
   :kafka-topic "gg-base"
   :kafka-topic-config {"cleanup.policy" "compact"
                        "delete.retention.ms" "100"}})

(def fetch-base-processor
  {:name :fetch-base-file
   :type :processor
   :subscribe :fetch-base-events
   :interceptors [fetch-file
                  publish-base-file]
   ::event/metadata {::handle
                     (assoc (:fs-handle env) :path "base/")}})

(def ready-server
  {:gene-validity-server
   {:type :http-server
    :name :ready-server
    ::http/host "0.0.0.0"
    ::http/allowed-origins {:allowed-origins (constantly true)
                            :creds true}
    ::http/routes
    [["/ready"
      :get (fn [_] {:status 200 :body "server is ready"})
      :route-name ::readiness]
     ["/live"
      :get (fn [_] {:status 200 :body "server is live"})
      :route-name ::liveness]]
    ::http/type :jetty
    ::http/port 8888
    ::http/join? false
    ::http/secure-headers nil}})

(def base-app-def
  {:type :genegraph-app
   :kafka-clusters {:data-exchange data-exchange}
   :topics {:fetch-base-events
            (assoc fetch-base-events-topic
                   :type :kafka-consumer-group-topic
                   :kafka-consumer-group consumer-group)
            :base-data
            (assoc base-data-topic
                   :type :kafka-producer-topic)}
   :processors {:fetch-base (assoc fetch-base-processor
                                   :kafka-cluster :data-exchange
                                   :kafka-transactional-id (qualified-kafka-name "fetch-base"))}
   :http-servers ready-server})


(defn -main [& args]
  (log/info :fn ::-main
            :msg "starting genegraph"
            :function (:function env))
  (let [app (p/init base-app-def)
        run-atom (atom true)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn []
                                 (log/info :fn ::-main
                                           :msg "stopping genegraph")
                                 (reset! run-atom false)
                                 (p/stop app))))
    (p/start app)))
