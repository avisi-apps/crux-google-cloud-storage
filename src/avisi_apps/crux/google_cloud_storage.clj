(ns avisi-apps.crux.google-cloud-storage
  (:import [com.google.cloud.storage Storage$BlobListOption Storage]))

(set! *warn-on-reflection* true)

(defn list-files
  ([deps] (list-files deps {}))
  ([{:keys [^Storage storage ^String bucket]} {:keys [prefix]}]
   (->
     (.list ^Storage storage bucket ^"[Lcom.google.cloud.storage.Storage$BlobListOption;"
       (into-array Storage$BlobListOption (cond-> []
                                            prefix (conj (Storage$BlobListOption/prefix prefix)))))
     (.iterateAll)
     seq)))
