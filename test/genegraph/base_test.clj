(ns genegraph.base-test
  (:require [clojure.test :refer [deftest is testing are]]
            [genegraph.base :as sut]
            [genegraph.framework.event :as event]))

;;; ---------------------------------------------------------------------------
;;; embed-secrets-in-source

(deftest embed-secrets-in-source-no-template
  (is (= "https://example.com/file.txt"
         (sut/embed-secrets-in-source "https://example.com/file.txt"))))

(deftest embed-secrets-in-source-with-template
  (with-redefs [sut/env {:my-token "abc123"}]
    (is (= "https://example.com/downloads/abc123/file.txt"
           (sut/embed-secrets-in-source
            "https://example.com/downloads/{{my-token}}/file.txt")))))

(deftest embed-secrets-in-source-missing-key
  ;; missing key resolves to nil; (str nil) is "", so the placeholder becomes ""
  (with-redefs [sut/env {}]
    (is (= "https://example.com/downloads//file.txt"
           (sut/embed-secrets-in-source
            "https://example.com/downloads/{{missing-key}}/file.txt")))))

(deftest embed-secrets-in-source-template-at-end
  (with-redefs [sut/env {:token "tok"}]
    (is (= "https://example.com/tok"
           (sut/embed-secrets-in-source "https://example.com/{{token}}")))))

;;; ---------------------------------------------------------------------------
;;; success?

(deftest success?-2xx
  (are [status] (true? (sut/success? {::sut/http-status status}))
    200 201 204 301 302 399))

(deftest success?-error-codes
  (are [status] (false? (sut/success? {::sut/http-status status}))
    400 401 403 404 500 503))

;;; ---------------------------------------------------------------------------
;;; hash-type

(deftest hash-type-default
  (is (= :crc32c (sut/hash-type {::event/data {}}))))

(deftest hash-type-explicit
  (is (= :md5hex (sut/hash-type {::event/data {:hash-type :md5hex}}))))

(deftest hash-type-md5
  (is (= :md5 (sut/hash-type {::event/data {:hash-type :md5}}))))

;;; ---------------------------------------------------------------------------
;;; output-handle

(deftest output-handle-assembles-path
  (let [event {::sut/handle {:type :gcs :bucket "my-bucket" :path "base/"}
               ::event/data {:target "mondo.owl"}}]
    (is (= {:type :gcs :bucket "my-bucket" :path "mondo.owl"}
           (sut/output-handle event)))))

(deftest output-handle-file-type
  (let [event {::sut/handle {:type :file :base "data/base/" :path "base/"}
               ::event/data {:target "hp.owl"}}]
    (is (= "hp.owl" (:path (sut/output-handle event))))))

;;; ---------------------------------------------------------------------------
;;; event->headers

(deftest event->headers-empty-when-absent
  ;; (update-vals nil f) returns {} — the function returns an empty map, not nil
  (is (= {} (sut/event->headers {::event/data {}}))))

(deftest event->headers-string-values-pass-through
  (is (= {"Authorization" "Bearer token123"}
         (sut/event->headers
          {::event/data {:headers {"Authorization" "Bearer token123"}}}))))

(deftest event->headers-keyword-values-resolved-from-env
  (with-redefs [sut/env {:my-api-key "secret-value"}]
    (is (= {"X-Api-Key" "secret-value"}
           (sut/event->headers
            {::event/data {:headers {"X-Api-Key" :my-api-key}}})))))

(deftest event->headers-mixed-values
  (with-redefs [sut/env {:token "tok"}]
    (is (= {"Accept" "application/json" "X-Token" "tok"}
           (sut/event->headers
            {::event/data {:headers {"Accept" "application/json"
                                     "X-Token" :token}}})))))

;;; ---------------------------------------------------------------------------
;;; add-prior-hash-fn

(deftest add-prior-hash-fn-captures-hash
  (with-redefs [sut/target-hash (constantly "deadbeef")]
    (let [event {::event/data {:target "mondo.owl"}}
          result (sut/add-prior-hash-fn event)]
      (is (= "deadbeef" (::sut/prior-hash result))))))

(deftest add-prior-hash-fn-survives-exception
  ;; If the stored object doesn't exist yet, target-hash throws; the event
  ;; should be returned unchanged (no ::prior-hash key).
  (with-redefs [sut/target-hash (fn [_] (throw (Exception. "object not found")))]
    (let [event {::event/data {:target "new-file.owl"}}
          result (sut/add-prior-hash-fn event)]
      (is (= event result))
      (is (not (contains? result ::sut/prior-hash))))))

;;; ---------------------------------------------------------------------------
;;; publish-base-file-fn

(deftest publish-base-file-fn-unchanged-when-hashes-match
  (let [hash "abc123"
        event {::sut/prior-hash hash
               ::event/data {:name "http://example.org/file"
                             :target "file.owl"}}]
    (with-redefs [sut/target-hash (constantly hash)]
      (is (= event (sut/publish-base-file-fn event))))))

(deftest publish-base-file-fn-publishes-when-hashes-differ
  (let [event {::sut/prior-hash "old-hash"
               ::event/data {:name "http://example.org/file"
                             :target "file.owl"}}]
    (with-redefs [sut/target-hash (constantly "new-hash")
                  sut/output-handle (constantly {:type :gcs :bucket "b" :path "file.owl"})]
      ;; The framework's event/publish accumulates effects on the event map;
      ;; we verify the result differs from the input (a publish was queued).
      (is (not= event (sut/publish-base-file-fn event))))))

(deftest publish-base-file-fn-publish-carries-correct-key
  (let [entry {:name "http://example.org/mondo" :target "mondo.owl"}
        event {::sut/prior-hash "old"
               ::event/data entry}]
    (with-redefs [sut/target-hash (constantly "new")
                  sut/output-handle (constantly {:type :gcs :bucket "b" :path "mondo.owl"})]
      (let [result (sut/publish-base-file-fn event)
            ;; The framework stores pending publishes under ::event/effects.
            ;; Locate the :base-data topic publish regardless of exact key.
            published (->> result
                           vals
                           (tree-seq coll? seq)
                           (filter map?)
                           (filter #(= :base-data (::event/topic %)))
                           first)]
        (is (some? published) "Expected a :base-data publish effect")
        (is (= "http://example.org/mondo" (::event/key published)))))))

;;; ---------------------------------------------------------------------------
;;; base-files catalog integrity

(deftest base-files-is-non-empty-sequence
  (is (sequential? sut/base-files))
  (is (pos? (count sut/base-files))))

(deftest base-files-entries-have-required-keys
  (doseq [entry sut/base-files]
    (is (string? (:name entry))   (str "missing :name in " (pr-str entry)))
    (is (string? (:source entry)) (str "missing :source in " (pr-str entry)))
    (is (string? (:target entry)) (str "missing :target in " (pr-str entry)))))

(deftest base-files-names-are-unique
  (let [names (map :name sut/base-files)]
    (is (= (count names) (count (set names))))))

(deftest base-files-targets-are-unique
  (let [targets (map :target sut/base-files)]
    (is (= (count targets) (count (set targets))))))
