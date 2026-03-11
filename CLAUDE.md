# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This App Does

`genegraph-base` downloads supporting data files (ontologies, reference datasets) needed by other Genegraph apps and advertises their availability on the ClinGen Data Exchange via Kafka.

## Common Commands

```bash
# Start a REPL for development
clj -A:dev

# Run tests
clj -M:test

# Build uberjar
clj -T:build uber

# Build and push Docker image (requires gcloud auth)
clj -T:build docker-push

# Full deploy (uber + docker-push + kubectl apply)
clj -T:build deploy
```

## Architecture

The app is built on `genegraph-framework` (from `io.github.clingen-data-model/genegraph-framework`), which provides a Kafka-based event processing model with interceptor chains (via Pedestal interceptors).

### Core flow

1. **Trigger**: Fetch events arrive on Kafka topic `gg-fb` (`:fetch-base-events`). Each event's `::event/data` is an entry from `resources/base.edn` — a map with `:source` (URL), `:target` (filename), `:name` (canonical URI), and optionally `:headers` and `:format`.
2. **Fetch**: The `fetch-file` interceptor downloads the file via HTTP (using `hato`) and writes it to storage (GCS in cloud environments, local filesystem in `local` mode).
3. **Publish**: The `publish-base-file` interceptor publishes the stored file reference to Kafka topic `gg-base` (`:base-data`) so downstream apps know the file is available.

### Environment / Platform

`GENEGRAPH_PLATFORM` env var controls deployment mode:
- `local` — uses GCP project `974091131481`, stores files under `data/base/` locally
- `dev` — GCS bucket `genegraph-base-dev`, GCP project `522856288592`
- `stage` — GCS bucket `genegraph-base-stage`, GCP project `583560269534`
- `prod` — GCS bucket `genegraph-base`, GCP project `974091131481`

Secrets (JAAS config, OMIM token, affiliations API key) are pulled from GCP Secret Manager via `genegraph.framework.env/build-environment`. Secret names in `:source` URLs can use `{{secret-name}}` template syntax, which `embed-secrets-in-source` resolves at runtime.

### Key files

- `src/genegraph/base.clj` — entire application: env config, Kafka topics/processor/app definition, HTTP interceptors, `-main`
- `resources/base.edn` — the catalog of files to download (ontologies, GFFs, ClinVar, HGNC, GenCC, GTR, OMIM, etc.)
- `resources/base-test.edn` — minimal single-entry version of base.edn for local testing
- `dev/user.clj` — REPL utilities: `base-test-app-def` (local queue-based app for testing fetch without Kafka), `gv-seed-base-event-def` (for publishing fetch events to real Kafka)
- `build.clj` — uberjar, Docker image, and Kubernetes deployment generation

### Development / Testing Without Kafka

In the REPL (`clj -A:dev`), use `base-test-app-def` in `dev/user.clj` which replaces Kafka topics with simple in-memory queues. Publish individual entries from `base.edn` using `p/publish` to test the fetch-and-store pipeline locally.

Portal (data inspector) is available in dev: `(def p (portal/open))` + `(add-tap #'portal/submit)`.

### HTTP Server

A minimal Pedestal HTTP server exposes `/ready` and `/live` endpoints for Kubernetes health checks.


### Genegraph Framework

This application is built on [genegraph-framework](https://github.com/clingen-data-model/genegraph-framework), a Clojure library for data-driven Kafka stream-processing apps. The entire application is described as a single map and started with `p/init` → `p/start`.

#### Key concepts

- **App-def map** — top-level keys: `:kafka-clusters`, `:topics`, `:storage`, `:processors`, `:http-servers`. Components reference each other by keyword; `p/init` wires them together.
- **Events** — plain maps with `::event/` namespaced keys (`::event/data`, `::event/key`, `::event/offset`, etc.). Interceptors receive and return event maps.
- **Effects are deferred** — `event/store`, `event/delete`, `event/publish` accumulate on the event map and execute *after* the interceptor chain completes.
- **Interceptors** — Pedestal interceptors (`:enter` fn receives event map, returns modified event map). Specified as values or fully-qualified symbols in the `:interceptors` vector.

#### Component types

| Kind | Types |
|---|---|
| Topics | `:simple-queue-topic`, `:kafka-consumer-group-topic`, `:kafka-producer-topic`, `:kafka-reader-topic` |
| Processors | `:processor` (single-threaded), `:parallel-processor` (3-stage pipeline; `:gate-fn` for per-key ordering) |
| Storage | `:rocksdb` (Nippy/LZ4), `:rdf` (Jena TDB2/SPARQL; use `rdf/tx` for reads), `:gcs-bucket`, `:atom` |
| HTTP | `:http-server` (Pedestal/http-kit; `:endpoints` link routes to processors) |

#### Startup / shutdown

Order: system-processor → storage → topics → processors → http-servers (shutdown is reverse). Every app has an internal `:system` `SimpleQueueTopic`; components publish lifecycle events (`:starting`, `:started`, `:up-to-date`, `:exception`) to it.

#### Namespace imports

```clojure
(require '[genegraph.framework.protocol :as p]
         '[genegraph.framework.event :as event]
         '[genegraph.framework.storage :as storage]
         '[genegraph.framework.storage.rdf :as rdf]
         '[genegraph.framework.kafka.admin :as kafka-admin]
         '[io.pedestal.interceptor :as interceptor])
```
