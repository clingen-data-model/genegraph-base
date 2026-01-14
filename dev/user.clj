(ns user
  (:require [genegraph.base :as base]
            [genegraph.framework.event :as event]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.kafka.admin :as kafka-admin]
            [portal.api :as portal]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.log :as log]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.java.process :as process]
            [clojure.java.shell :as sh]))


(comment
  (do
    (def p (portal/open))
    (add-tap #'portal/submit))

  )


(def process-base-interceptor
  (interceptor/interceptor
   {:enter #(clojure.pprint/pprint (::event/data %))}))

(def base-test-app-def
  {:type :genegraph-app
   :topics {:fetch-base-events
            {:name :fetch-base-events
             :type :simple-queue-topic}
            :base-data
            {:name :base-data
             :type :simple-queue-topic}}
   :processors {:fetch-base base/fetch-base-processor
                :process-base {:type :processor
                               :subscribe :base-data
                               :interceptors [process-base-interceptor]}}
   :http-servers base/ready-server})


(comment
  (run! #(kafka-admin/configure-kafka-for-app! (p/init %))
        [base/base-app-def])


  )

(def gv-seed-base-event-def
  {:type :genegraph-app
   :kafka-clusters {:data-exchange base/data-exchange}
   :topics {:fetch-base
            (assoc base/fetch-base-events-topic
                   :type :kafka-producer-topic
                   :create-producer true)}})

(comment
  (def gv-seed-base-event (p/init gv-seed-base-event-def))
  (p/start gv-seed-base-event)
  (->> (-> "base.edn" io/resource slurp edn/read-string)
       #_(filter #(= "http://www.w3.org/1999/02/22-rdf-syntax-ns#" (:name %)))
       #_(filter #(= "http://purl.obolibrary.org/obo/mondo.owl" (:name %)))
       #_(filter #(= "http://www.ebi.ac.uk/efo/efo-base.owl" (:name %)))
       #_(filter #(re-find #"sepio" (:name %)))
       #_(mapv :name)
       
       #_tap>
       
       (run! #(p/publish (get-in gv-seed-base-event
                                 [:topics :fetch-base])
                         {::event/data %
                          ::event/key (:name %)})))
  
  )


;; Setting up topics for application


;; Testing functionality
(comment
  (def base-test-app (p/init base-test-app-def))
  (p/start base-test-app)
  (p/stop base-test-app)
  
  (->> (-> "base.edn" io/resource slurp edn/read-string)
       (filter #(= "http://www.w3.org/1999/02/22-rdf-syntax-ns#" (:name %)))
       (run! #(p/publish (get-in base-test-app
                                 [:topics :fetch-base-events])
                         {::event/data %
                          ::event/key (:name %)})))
  
  

  (->> (-> "base.edn" io/resource slurp edn/read-string)
       (filter #(= "https://affils.clinicalgenome.org/" (:name %)))
       (run! #(p/publish (get-in base-test-app
                                 [:topics :fetch-base-events])
                         {::event/data %
                          ::event/key (:name %)})))
  
  )

(comment
;; docker buildx build . -t us-east1-docker.pkg.dev/clingen-stage/genegraph-stage/genegraph-api:v19arm --platform linux/arm64 --push

  (println
   (process/exec
    {:err :stdout}
    "docker"
    "buildx"
    "build"
    "."
    "--platform"
    "linux/arm64"
    "-t"
    (str
     "us-east1-docker.pkg.dev/"
     "clingen-stage/"
     "genegraph-stage/"
     "genegraph-base:latest")
    "--push"))

  (tap> (System/getProperties))

  (tap> (System/getenv))
  (println
   (process/exec
    {:env {"PATH" (str "/Users/tristan/google-cloud-sdk/bin:"(System/getenv "PATH"))}}
    "gcloud"))
  
  (println
   (sh/sh
    "docker"
    "push"
    (str
     "us-east1-docker.pkg.dev/"
     "clingen-stage/"
     "genegraph-stage/"
     "genegraph-base:latest")))
  (def res
    (process/exec
     "docker"
     "buildx"
     "build"
     "."
     "-t"
     (str
      "us-east1-docker.pkg.dev/"
      "clingen-stage/"
      "genegraph-stage/"
      "genegraph-base:latest")
     "--push"))



(let [result (process/exec "grep" "pattern" "nonexistent-file.txt")]
  (if (zero? (:exit result))
    (println "Command succeeded:" (:out result))
    (println "Command failed with exit code" (:exit result) 
             "\nError:" (:err result))))


  )


