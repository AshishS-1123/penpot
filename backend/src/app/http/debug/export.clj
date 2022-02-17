;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) UXBOX Labs SL

(ns app.http.debug.export
  "A custom, size and performance optimized export format."
  (:require
   [app.common.data :as d]
   [app.common.exceptions :as ex]
   [app.common.spec :as us]
   [app.common.uuid :as uuid]
   [app.common.logging :as l]
   [app.media :as media]
   [app.config :as cf]
   [app.db :as db]
   [app.util.blob :as blob]
   [app.tasks.file-media-gc :as tfm]
   [app.rpc.mutations.files :as m.files]
   [app.rpc.queries.profile :as profile]
   [app.util.time :as dt]
   [app.storage :as sto]
   [clojure.java.io :as io]
   [cuerdas.core :as str]
   [datoteka.core :as fs]
   [ring.core.protocols :as rp]
   [app.common.transit :as t]
   [fipp.edn :as fpp]
   [yetti.adapter :as yt])
  (:import
   java.io.DataOutputStream
   java.io.DataInputStream
   java.io.InputStream
   java.io.OutputStream
   java.io.BufferedOutputStream
   java.io.BufferedInputStream
   org.apache.commons.io.IOUtils
   com.github.luben.zstd.ZstdOutputStream
   org.apache.commons.io.input.BoundedInputStream))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; HELPERS
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:const buffer-size (:http/output-buffer-size yt/base-defaults))

(def ^:const marks
  {:header  1
   :blob    2
   :stream  3
   :uuid    4
   :transit 5})

(defn buffered-output-stream
  "Returns a buffered output stream that ignores flush calls. This is
  needed because transit-java calls flush very aggresivelly on each
  object write."
  [^java.io.OutputStream os ^long chunk-size]
  (proxy [java.io.BufferedOutputStream] [os (int chunk-size)]
    ;; Explicitly do not forward flush
    (flush [])
    (close []
      (proxy-super flush)
      (proxy-super close))))

(defmacro assert-mark
  [v type]
  `(when (not= (long ~v) (~type marks))
     (ex/raise :type :assertion
               :code :unexpected-mark)))

(defmacro assert-label
  [n label]
  `(when (not= ~n ~label)
     (ex/raise :type :assertion
               :code :unexpected-label)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; LOW LEVEL STREAM IO
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; --- PRIMITIVE

(defn write-byte!
  [^DataOutputStream ostream data]
  (doto ostream
    (.writeByte (int data))))

(defn read-byte!
  [^DataInputStream istream]
  (.readByte istream))

(defn write-long!
  [^DataOutputStream ostream data]
  (doto ostream
    (.writeLong (long data))))

(defn read-long!
  [^DataInputStream istream]
  (.readLong istream))

(defn write-bytes!
  [^DataOutputStream ostream ^bytes data]
  (doto ostream
    (.write data 0 (alength data))))

(defn read-bytes!
  [^DataOutputStream istream ^bytes buff]
  (.read istream buff 0 (alength buff)))

;; --- COMPOSITE

(defn write-uuid!
  [^DataOutputStream ostream id]
  (doto ostream
    (write-byte! (:uuid marks))
    (write-long! (uuid/get-word-high id))
    (write-long! (uuid/get-word-low id))))

(defn read-uuid!
  [^DataInputStream istream]
  (let [m (read-byte! istream)]
    (assert-mark m :uuid)
    (let [a (read-long! istream)
          b (read-long! istream)]
      (uuid/custom b a))))

(defn write-label!
  [^DataOutputStream ostream label]
  (let [^String label (if (keyword? label) (name label) label)
        ^bytes data   (.getBytes label "UTF-8")]
    (doto ostream
      (write-long! (alength data))
      (write-bytes! data))))

(defn read-label!
  [^DataInputStream istream]
  (let [m (read-byte! istream)]
    (assert-mark m :label)
    (let [size (read-long! istream)
          buff (byte-array size)]
      (read-bytes! istream buff)
      (keyword (String. buff "UTF-8")))))

(defn write-header!
  [^DataOutputStream ostream ^long version]
  (doto ostream
    (write-byte! (:header marks))
    (write-label! :penpot-binary-export)
    (write-long! version)))

(defn read-header!
  [^DataInputStream istream]
  (let [m (read-byte! istream)]
    (assert-mark m :header)
    {:type (read-label! istream)
     :version (read-long! istream)}))

(defn write-blob!
  [^DataOutputStream ostream label data]
  (let [^bytes data (if (bytes? data) data (blob/encode data))]
    (doto ostream
      (write-byte! (:blob marks))
      (write-label! label)
      (write-long! (alength data))
      (write-bytes! data))))

(defn read-blob!
  ([istream label] (read-blob! istream label false))
  ([^DataInputStream istream label decode?]
   (let [m (read-byte! istream)]
     (assert-mark m :blob))
   (let [n (read-label! istream)]
     (assert-label n label))
   (let [size (read-long! istream)
         buff (byte-array size)]
     (read-bytes! istream buff)
     (if decode?
       (blob/decode buff)
       buff))))

(defn copy-stream!
  [^DataOutputStream ostream ^InputStream stream ^long size]
  (let [buff (byte-array buffer-size)]
    (IOUtils/copyLarge stream ostream 0 size buff)
    stream))

(defn write-stream!
  [^DataOutputStream ostream label stream size]
  (doto ostream
    (write-byte! (:stream marks))
    (write-label! label)
    (write-long! size)
    (copy-stream! stream size)))

(defn read-stream!
  [^DataInputStream istream label]
  (let [m (read-byte! istream)]
    (assert-mark m :blob))
  (let [n (read-label! istream)]
     (assert-label n label))
  (let [size (read-long! istream)]
    (doto (BoundedInputStream. istream size)
      (.setPropagateClose false))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; HIGH LEVEL IMPL
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(def storage-object-id-xf
  (comp
   (mapcat (juxt :media-id :thumbnail-id))
   (filter uuid?)))

(defn get-used-media
  [pool fdata]
  (let [fmedia-ids (#'tfm/collect-used-media fdata)]
    (with-open [conn (db/open pool)]
      (let [sql "select * from file_media_object where id = ANY(?)"]
        (db/exec! conn [sql (db/create-array conn "uuid" ids)])))))

(defn write-export!
  [{:keys [pool storage file-id]} ^DataOutputStream ostream]
  (let [{:keys [data] :as file} (db/get-by-id pool :file file-id)

        fdata   (blob/decode data)
        storage (media/configure-assets-storage storage)
        fmedia  (get-used-media pool fdata)
        sids    (into [] storage-object-id-xf fmedia)]

    (doto ostream
      (write-header! 1)
      (write-blob! :file (dissoc file :data))
      (write-blob! :fdata data)
      (write-blob! :fmedia fmedia)
      (write-blob! :sids sids))

    (doseq [id sids]
      (let [{:keys [size] :as obj} (sto/get-object storage id)]
        (doto ostream
          (write-uuid! id)
          (write-blob! :storage-object obj)
          (write-blob! :storage-metadata (meta obj)))

        (with-open [stream (sto/get-object-data storage obj)]
          (write-stream! ostream :storage-object-data stream size))))))

(defn read-import!
  [{:keys [pool storage profile-id]} ^DataInputStream istream]
  (let [file-id    (uuid/next)
        project-id (some-> (profile/retrieve-additional-data pool profile-id) :default-project-id)
        header     (read-header! istream)]

    (when-not project-id
      (ex/raise :type :validation
                :code :unable-to-lookup-project))

    (when (not= (:label header) :penpot-binary-export)
      (ex/raise :type :validation
                :code :invalid-import-file))

    (when (not= (:version header) 1)
      (ex/raise :type :validation
                :code :unsupported-version))

    (let [file   (read-blob! istream :file true)
          fdata  (read-blob! istream :fdata false)
          fmedia (read-blob! istream :fmedia true)
          sids   (read-blob! istream :sids true)]

      (db/with-atomic [conn pool]
        (let [storage (media/configure-assets-storage storage conn)]
          (m.files/create-file conn {:id file-id
                                     :name (:name file-id)
                                     :project-id project-id
                                     :profile-id profile-id
                                     :data fdata})

          (doseq [id sids]
            (prn "importing media" id)
            (let [stream (read-stream! istream :storage-object-data)
                  content (sto/content stream size)]


            (let [huid    (.readLong istream)
                  luid    (.readLong istream)
                  size    (.readLong istream)
                  id      (uuid/custom luid huid)
                  content (sto/content (doto (BoundedInputStream. istream size)
                                         (.setPropagateClose false))
                                       size)
                  sobj    (sto/put-object storage {:content content})]
              (db/insert! conn :file-media-object
                          {:id (uuid/next)
                           :file-id file-id
                           :is-local false
                           :name "test"
                           :media-id (:id sobj)
                           :thumbnail-id nil
                           :width 100
                           :height 100
                           :mtype "image/png"}))))))))

(defn export-handler
  [cfg request]
  (let [file-id (some-> (get-in request [:params :file-id]) uuid/uuid)]
    (when-not file-id
      (ex/raise :type :validation
                :code :missing-arguments))

    {:status 200
     :headers {"content-type" "application/octet-stream"
               "content-disposition" "attachment"}
     :body (reify rp/StreamableResponseBody
             (write-body-to-stream [_ _ output-stream]
               (time
                (try
                  (with-open [ostream (DataOutputStream. output-stream)]
                    (write-export! (assoc cfg :file-id file-id) ostream))
                  (catch org.eclipse.jetty.io.EofException _cause
                    ;; Do nothing, EOF means client closes connection abruptly
                    nil)
                  (catch Throwable cause
                    (l/warn :hint "unexpected exception on writing export"
                            :cause cause))))))}))


(defn import-handler
  [cfg {:keys [params profile-id] :as request}]
  (when-not (contains? params :file)
    (ex/raise :type :validation
              :code :missing-upload-file
              :hint "missing upload file"))
  (with-open [istream (io/input-stream (-> params :file :tempfile))]
    (with-open [istream (DataInputStream. istream)]
      (let [cfg (assoc cfg :profile-id profile-id)]
        (read-import! cfg istream)
        {:status 200
         :headers {"content-type" "text/plain"}
         :body "OK"}))))
