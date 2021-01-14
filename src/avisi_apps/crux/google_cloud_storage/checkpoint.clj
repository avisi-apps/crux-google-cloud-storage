(ns avisi-apps.crux.google-cloud-storage.checkpoint
  (:require [crux.checkpoint :as cp]
            [crux.system :as sys]
            [crux.io :as cio]
            [crux.tx :as tx]
            [clojure.spec.alpha :as s]
            [clojure.tools.reader.edn :as edn]
            [clojure.java.io :as io]
            [avisi-apps.crux.google-cloud-storage :as gcs])
  (:import [java.io File]
           [com.google.cloud.storage Storage StorageOptions StorageOptions$Builder BlobInfo Blob Storage$BlobWriteOption Storage$BlobTargetOption Storage$BlobListOption Blob$BlobSourceOption]
           [java.util Date]
           [java.nio.file Path]
           [java.nio.charset Charset]))

(set! *warn-on-reflection* true)

(s/def ::project string?)
(s/def ::bucket (s/and
                  string?
                  #(<= (count %) 63)))

(defrecord GoogleStorageCheckpointStore [^Storage storage bucket]
  cp/CheckpointStore
  (available-checkpoints [this {::cp/keys [cp-format]}]

    (->>
      (gcs/list-files this)
      (keep (fn [^Blob blob]
              (when-let [[_ tx-id checkpoint-at] (re-matches #"checkpoint-(\d+)-(.+).edn" (.getName blob))]
                {:blob blob
                 :sort-key [(Long/parseLong tx-id) checkpoint-at]})))
      (sort-by :sort-key #(compare %2 %1))
      (keep (fn [{:keys [^Blob blob]}]
             (let [resp (some->
                         (.getContent blob ^"[Lcom.google.cloud.storage.Storage.Blob$BlobSourceOption;" (make-array Blob$BlobSourceOption 0))
                         slurp
                         edn/read-string)]
               (when (= (::cp/cp-format resp) cp-format)
                 resp))))))
  (download-checkpoint [this {::keys [gcs-dir] :as checkpoint} dir]
    (->>
      (gcs/list-files this {:prefix gcs-dir})
      (mapv (fn [^Blob blob]
              (let [download-to (io/file ^File dir
                                  (.toFile (.relativize
                                             (.toPath (io/file gcs-dir))
                                             (.toPath (io/file (.getName blob))))))]
                (io/make-parents download-to)
                (.downloadTo blob ^Path (.toPath download-to) ^"[Lcom.google.cloud.storage.Blob$BlobSourceOption;" (make-array Blob$BlobSourceOption 0))))))
    checkpoint)
  (upload-checkpoint [this dir {:keys [tx ::cp/cp-format]}]
    (let [dir-path (.toPath ^File dir)
          cp-at (Date.)
          gcs-dir (format "checkpoint-%s-%s" (::tx/tx-id tx) (cio/format-rfc3339-date cp-at))]
      (run!
        (fn [^File file]
          (when (.isFile file)
            (.createFrom
              ^Storage storage
              ^BlobInfo (->
                          (BlobInfo/newBuilder ^String bucket ^String (str gcs-dir "/" (.relativize dir-path (.toPath file))))
                          (.build))
              ^Path (.toPath file)
              ^"[Lcom.google.cloud.storage.Storage$BlobWriteOption;" (make-array Storage$BlobWriteOption 0))))
        (file-seq dir))
      (let
        [cp {::cp/cp-format cp-format,
             :tx tx
             ::gcs-dir (str gcs-dir "/")
             ::cp/checkpoint-at cp-at}]
        (.create ^Storage storage
          ^BlobInfo (->
            (BlobInfo/newBuilder ^String bucket ^String (str gcs-dir ".edn"))
            (.build))
          ^bytes (-> (pr-str cp)
                   (.getBytes (Charset/forName "UTF-8")))
          ^"[Lcom.google.cloud.storage.Storage$BlobTargetOption;" (make-array Storage$BlobTargetOption 0))
        cp))))

(defn ->storage-options {::sys/args {:project {:required? true,
                                               :spec ::project
                                               :doc "The ID of your google cloud project"}}}
  [{:keys [project]}]
  (->
    (StorageOptions/newBuilder)
    ^StorageOptions$Builder(.setProjectId project)
    ^StorageOptions (.build)))

(defn ->cp-store {::sys/deps {:storage-options `->storage-options}
                  ::sys/args {:bucket {:required? true,
                                       :spec ::bucket
                                       :doc "The ID of your google cloud storage bucket"}}}
  [{:keys [^StorageOptions storage-options project bucket]}]
  (->GoogleStorageCheckpointStore
    (.getService storage-options)
    bucket))
