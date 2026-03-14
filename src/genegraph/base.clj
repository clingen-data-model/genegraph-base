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
            [io.pedestal.interceptor.chain :as chain]
            [io.pedestal.log :as log]
            [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import [java.io File InputStream OutputStream PushbackReader]
           [java.nio.channels Channels]
           [java.time Instant ZonedDateTime]
           [java.time.temporal TemporalUnit Temporal]
           [java.util.concurrent ScheduledThreadPoolExecutor
            ExecutorService ScheduledExecutorService TimeUnit])
  (:gen-class))

(def base-files
  (with-open [r (-> "base.edn" io/resource io/reader PushbackReader.)]
    (edn/read r)))

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
                                                          "affils-service-key"
                                                          "omim-thn"])
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
                                                         "affils-service-key"
                                                         "omim-thn"])
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

(defn hash-file-handle [event]
  (str (output-handle event) ".hash"))

(defn success? [{status ::http-status}]
  (and (<= 200 status) (< status 400)))

(defn event->headers [event]
  (-> event
      (get-in [::event/data :headers])
      (update-vals #(if (keyword? %) (get env %) %))))

(defn embed-secrets-in-source [source-url]
  (if-let [[_ first-section secret second-section] (re-find #"(.*)\{\{(.+)\}\}(.*)" source-url)]
    (str first-section (get env (keyword secret)) second-section)
    source-url))

(comment
  (->> base-files
       (filterv :hash-check-source)
       (mapv :hash-check-source)
       (mapv embed-secrets-in-source))
  )

(defn stored-object-hash [handle type]
  (case type
    :md5 (.getMd5 handle)
    :md5hex (.getMd5ToHexString handle)
    :crc32c (.getCrc32c handle)
    :crc32chex (.getCrc32cToHexString handle)))

(comment

  (let [h (storage/as-handle
           {:type :gcs
            :bucket "genegraph-base"
            :path "clinvar.xml.gz"})]
    (mapv #(stored-object-hash h %) [:md5 :md5hex :crc32c :crc32chex]))

  )

(defn hash-type [event]
  (get-in event [::event/data :hash-type] :crc32c))

(defn target-hash [event]
  (-> event
      output-handle
      storage/as-handle
      (stored-object-hash (hash-type event))))

(defn add-prior-hash-fn [event]
  (try 
    (assoc event
           ::prior-hash
           (target-hash event))
    (catch Exception e event)))

(def add-prior-hash
  (interceptor/interceptor
   {:name ::prior-hash
    :enter (fn [e] (add-prior-hash-fn e))}))

(defonce http-client (hc/build-http-client {:redirect-policy :always}))

(defn write-iri-to-storage-handle!
  ([iri storage-handle]
   (write-iri-to-storage-handle! iri storage-handle {}))
  ([iri storage-handle http-opts]
   (let [base-opts {:http-client http-client
                    :as :stream}
         response (hc/get (embed-secrets-in-source iri)
                          (merge http-opts
                                 base-opts))]
     (when (instance? InputStream (:body response))
       (with-open [os (io/output-stream (storage/as-handle storage-handle))]
         (.transferTo (:body response) os)))
     (:status response))))

(defn get-iri
  ([iri]
   (get-iri iri {}))
  ([iri http-opts]
   (let [base-opts {:http-client http-client}
         response (hc/get (embed-secrets-in-source iri)
                          (merge http-opts
                                 base-opts))]
     (:body response))))

(defn iri-body-matches-storage-object? [iri storage-handle]
  (= (get-iri iri)
     (-> storage-handle storage/as-handle slurp)))

;; technically I'm not doing any hashing here, only comparing
;; equality on a pre-created hash file
(defn check-hash-file-fn [event]
  (if-let [hash-source (get-in event [::event/data :hash-check-source])]
    (if (iri-body-matches-storage-object? hash-source
                                          (hash-file-handle event))
      (do (log/info :fn ::check-hash-file-fn
                    :outcome :file-is-current)
          (chain/terminate event))
      (do (log/info :fn ::check-hash-file-fn
                    :outcome :file-is-not-current)
          (assoc event ::stale? true)))
    event)) ; no hash file to check, need to download to check freshness

(def check-hash-file
  (interceptor/interceptor
   {:name ::check-hash-file
    :enter (fn [e] (check-hash-file-fn e))}))

(comment
  (def clinvar-hash-iri
    "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/ClinVarVCVRelease_00-latest.xml.gz.md5")

  (def clinvar-hash-handle
    {:type :gcs
     :bucket "genegraph-base"
     :path "clinvar.xml.gz.hash"})
  (write-iri-to-storage-handle!
   clinvar-hash-iri
   clinvar-hash-handle)
  (get-iri clinvar-hash-iri)

  (iri-body-matches-storage-object? clinvar-hash-iri clinvar-hash-handle))

(defn fetch-file-fn [event]
  (log/info :fn ::fetch-file-fn
            :source (get-in event [::event/data :source])
            :name  (get-in event [::event/data :name])
            :status :started)
  (assoc event
         ::http-status
         (write-iri-to-storage-handle! (get-in event [::event/data :source])
                                       (output-handle event))))

(def fetch-file
  (interceptor/interceptor
   {:name ::fetch-file
    :enter (fn [e] (fetch-file-fn e))}))

(defn publish-base-file-fn [event]
  (if (not= (target-hash event) (::prior-hash event))
    (event/publish event {::event/topic :base-data
                          ::event/key (get-in event [::event/data :name])
                          ::event/data (-> event
                                           ::event/data
                                           (assoc :source (output-handle event)))})
    event))

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
   :interceptors [check-hash-file
                  add-prior-hash
                  fetch-file
                  publish-base-file]
   ::event/metadata {::handle
                     (assoc (:fs-handle env) :path "base/")}})

(defn poll-base-records-fn [event]
  (log/info :fn ::poll-base-records-fn
            :action :initiating-poll)
  (reduce
   (fn [e base-record]
     (event/publish e {::event/data base-record
                       ::event/key (:name base-record)
                       ::event/topic :fetch-base-events}))
   
   event
   (filterv :poll base-files)))

(def poll-base-records-int
  (interceptor/interceptor
   {:name ::poll-base-records
    :enter (fn [e] (poll-base-records-fn e))}))

(def poll-base-processor
  {:type :processor
   :name :poll-base-processor
   :subscribe :poll-base-records
   :interceptors [poll-base-records-int]})

(def ready-server
  {:gene-validity-server
   {:type :http-server
    :name :ready-server
    :routes
    [["/ready"
      :get (fn [_] {:status 200 :body "server is ready"})
      :route-name ::readiness]
     ["/live"
      :get (fn [_] {:status 200 :body "server is live"})
      :route-name ::liveness]]}})

(def base-app-def
  {:type :genegraph-app
   :kafka-clusters {:data-exchange data-exchange}
   :topics {:fetch-base-events
            (assoc fetch-base-events-topic
                   :type :kafka-consumer-group-topic
                   :kafka-consumer-group consumer-group
                   :create-producer true)
            :poll-base-records
            {:name :poll-base-records
             :type :timer-topic
             :interval (* 1000 60 60 12)} ; 12 hrs
            :base-data
            (assoc base-data-topic
                   :type :kafka-producer-topic)}
   :processors {:fetch-base (assoc fetch-base-processor
                                   :kafka-cluster :data-exchange
                                   :kafka-transactional-id (qualified-kafka-name "fetch-base"))
                :poll-base-processor poll-base-processor}
   :http-servers ready-server})

(defonce update-publisher-executor
  (ScheduledThreadPoolExecutor. 1))

(defn check-for-base-updates [app]
  (.scheduleAtFixedRate update-publisher-executor
                        (fn [])
                        0
                        1
                        TimeUnit/MINUTES))



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
                                 (p/stop app)
                                 (Thread/sleep 5000))))
    (p/start app)))
