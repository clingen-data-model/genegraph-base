(ns build
  "Build this thing."
  (:require [clojure.tools.build.api :as b]
            [clojure.java.process :as process]
            [clojure.data.json :as json]
            [clojure.java.io :as io]))

;; clj -T:build <function>

(def defaults
  "The defaults to configure a build."
  {:class-dir  "target/classes"
   :java-opts  ["-Dclojure.main.report=stderr"]
   :main       'genegraph.base
   :path       "target"
   :project    "deps.edn"
   :target-dir "target/classes"
   :uber-file  "target/app.jar"
   :exclude [#"META-INF/license.*" "src/user.clj"]})

(defn uber
  "Throw or make an uberjar from source."
  [_]
  (let [{:keys [paths] :as basis} (b/create-basis defaults)
        project                   (assoc defaults :basis basis)]
    (b/delete      project)
    (b/copy-dir    (assoc project :src-dirs paths))
    (b/compile-clj (assoc project
                          :src-dirs ["src"]
                          :ns-compile ['genegraph.base]))
    (b/uber        project)))

;; docker buildx build . -t us-east1-docker.pkg.dev/clingen-stage/genegraph-stage/genegraph-api:v19arm --platform linux/arm64 --push

(def app-name "genegraph-base")

(defn image-tag []
  (str
   "us-east1-docker.pkg.dev/"
   "clingen-dx/"
   "genegraph-prod/"
   "genegraph-base:v"
   (b/git-count-revs {})))

(defn kubernetes-deployment []
  {:apiVersion "apps/v1"
   :kind "Deployment"
   :metadata {:name app-name}
   :spec
   {:selector {:matchLabels {:app app-name}}
    :template
    {:metadata {:labels {:app app-name}}
     :spec
     {:containers
      [{:name app-name
        :image (image-tag)
        :env [{:name "GENEGRAPH_PLATFORM" :value "prod"}]
        :ports [{:name "genegraph-port" :containerPort 8888}]
        :readinessProbe {:httpGet {:path "/ready" :port "genegraph-port"}}
        :resources {:requests {:memory "700Mi" :cpu "50m"}
                    :limits {:memory "700Mi"}}}]
      :tolerations [{:key "kubernetes.io/arch"
                     :operator "Equal"
                     :value "arm64"
                     :effect "NoSchedule"}]
      :affinity {:nodeAffinity {:requiredDuringSchedulingIgnoredDuringExecution
                                {:nodeSelectorTerms
                                 [{:matchExpressions
                                   [{:key "kubernetes.io/arch"
                                     :operator "In"
                                     :values ["arm64"]}]}]}}}}}}})


(comment
  (b/git-count-revs {})

  (b/git-process {:git-args "status --porcelain=2"}))
;; NOTE: must be run from shell with gcloud stuff enabled
(defn docker-push
  [_]
  (process/exec
   {:err :stdout}
   "docker"
   "buildx"
   "build"
   "."
   "--platform"
   "linux/arm64"
   "-t"
   (image-tag)
   "--push"))

(defn kubernetes-apply
  [_]
  (let [p (process/start {:err :inherit} "kubectl" "apply" "-f" "-")
        captured (process/io-task #(slurp (process/stdout p)))
        exit (process/exit-ref p)]
    (with-open [w (io/writer (process/stdin p))]
      (json/write (kubernetes-deployment) w))
    (if (zero? @(process/exit-ref p))
      (println @captured)
      (println "non-zero exit code"))))

