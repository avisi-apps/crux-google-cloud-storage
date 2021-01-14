(ns avisi-apps.crux.google-cloud-storage.checkpoint-test
  (:require [clojure.test :as t]
            [avisi-apps.crux.google-cloud-storage.checkpoint :as gcs-check]
            [crux.io :as cio]
            [crux.tx :as tx]
            [crux.checkpoint :as cp]
            [crux.system :as sys]
            [clojure.java.io :as io])
  (:import [com.google.cloud.storage.contrib.nio.testing LocalStorageHelper]
           [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn with-tmp-dir* [prefix f]
  (let [dir (.toFile (Files/createTempDirectory prefix (make-array FileAttribute 0)))]
    (try (f dir) (finally (cio/delete-dir dir)))))

(defmacro with-tmp-dir
  [prefix [dir-binding] & body]
  `(with-tmp-dir*
     ~prefix
     (fn
       [~(->
           dir-binding
           (with-meta {:type File}))]
       ~@body)))

(t/deftest test-checkpoint-store
  (with-open [sys (-> (sys/prep-system {:store {:crux/module `gcs-check/->cp-store
                                                :storage-options (fn [_] (LocalStorageHelper/getOptions))
                                                :bucket "test"}})
                    (sys/start-system))]
    (with-tmp-dir "gcs-cp" [cp-dir]
      (let [{:keys [store]} sys
            src-dir (doto (io/file cp-dir "src")
                      (.mkdirs))
            cp-1 {::cp/cp-format ::foo-cp-format
                  :tx {::tx/tx-id 1}}
            cp-2 {::cp/cp-format ::foo-cp-format
                  :tx {::tx/tx-id 2}}]

        (t/testing "first checkpoint"
          (spit (io/file src-dir "hello.txt") "Hello world")

          (t/is (= cp-1
                  (-> (cp/upload-checkpoint store src-dir cp-1)
                    (select-keys #{::cp/cp-format :tx}))))

          (t/is (empty? (cp/available-checkpoints store {::cp/cp-format ::bar-cp-format})))

          (let [dest-dir (io/file cp-dir "dest")
                cps (cp/available-checkpoints store {::cp/cp-format ::foo-cp-format})]
            (t/is (= [cp-1]
                    (->> (cp/available-checkpoints store {::cp/cp-format ::foo-cp-format})
                      (map #(select-keys % #{::cp/cp-format :tx})))))
            (cp/download-checkpoint store (first cps) dest-dir)
            (t/is (= "Hello world"
                    (slurp (io/file dest-dir "hello.txt"))))))

        (t/testing "second checkpoint"
          (spit (io/file src-dir "ivan.txt") "Hey Ivan!")

          (t/is (= cp-2
                  (-> (cp/upload-checkpoint store src-dir cp-2)
                    (select-keys #{::cp/cp-format :tx}))))

          (t/is (empty? (cp/available-checkpoints store {::cp/cp-format ::bar-cp-format})))

          (let [dest-dir (io/file cp-dir "dest-2")
                cps (cp/available-checkpoints store {::cp/cp-format ::foo-cp-format})]
            (t/is (= [cp-2 cp-1]
                    (->> (cp/available-checkpoints store {::cp/cp-format ::foo-cp-format})
                      (map #(select-keys % #{::cp/cp-format :tx})))))
            (cp/download-checkpoint store (first cps) dest-dir)
            (t/is (= "Hello world"
                    (slurp (io/file dest-dir "hello.txt"))))

            (t/is (= "Hey Ivan!"
                    (slurp (io/file dest-dir "ivan.txt"))))))))))

