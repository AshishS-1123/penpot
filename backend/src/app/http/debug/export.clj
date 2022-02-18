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
   [app.common.pages.migrations :as pmg]
   [app.util.blob :as blob]
   [app.tasks.file-media-gc :as tfm]
   [clojure.walk :as walk]
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

(defn get-mark
  [id]
  (case id
    :header  1
    :blob    2
    :stream  3
    :uuid    4
    :label   5
    (ex/raise :type :assertion
              :code :invalid-mark-id
              :hint (format "invalid mark id %s" id))))


;; (defn buffered-output-stream
;;   "Returns a buffered output stream that ignores flush calls. This is
;;   needed because transit-java calls flush very aggresivelly on each
;;   object write."
;;   [^java.io.OutputStream os ^long chunk-size]
;;   (proxy [java.io.BufferedOutputStream] [os (int chunk-size)]
;;     ;; Explicitly do not forward flush
;;     (flush [])
;;     (close []
;;       (proxy-super flush)
;;       (proxy-super close))))

(defn assert-mark
  [v type]
  (let [expected (get-mark type)
        v        (long v)]
    (when (not= v expected)
      (ex/raise :type :assertion
                :code :unexpected-mark
                :hint (format "received mark %s, expected %s" v expected)))))

(defn assert-label
  [n label]
  (when (not= n label)
    (ex/raise :type :assertion
              :code :unexpected-label
              :hint (format "received label %s, expected %s" n label))))

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
  [^DataInputStream istream ^bytes buff]
  (.read istream buff 0 (alength buff)))

;; --- COMPOSITE

(defn write-uuid!
  [^DataOutputStream ostream id]
  (doto ostream
    (write-byte! (get-mark :uuid))
    (write-long! (uuid/get-word-high id))
    (write-long! (uuid/get-word-low id))))

(defn read-uuid!
  [^DataInputStream istream]
  (let [m (read-byte! istream)]
    (assert-mark m :uuid)
    (let [a (read-long! istream)
          b (read-long! istream)]
      (uuid/custom a b))))

(defn write-label!
  [^DataOutputStream ostream label]
  (let [^String label (if (keyword? label) (name label) label)
        ^bytes data   (.getBytes label "UTF-8")]
    (doto ostream
      (write-byte! (get-mark :label))
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
    (write-byte! (get-mark :header))
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
      (write-byte! (get-mark :blob))
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
    (write-byte! (get-mark :stream))
    (write-label! label)
    (write-long! size)
    (copy-stream! stream size)))

(defn read-stream!
  [^DataInputStream istream label]
  (let [m (read-byte! istream)]
    (assert-mark m :stream))
  (let [n (read-label! istream)]
     (assert-label n label))
  (let [size (read-long! istream)]
    [size (doto (BoundedInputStream. istream size)
            (.setPropagateClose false))]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; HIGH LEVEL IMPL
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def storage-object-id-xf
  (comp
   (mapcat (juxt :media-id :thumbnail-id))
   (filter uuid?)))

(defn get-used-media
  [pool fdata]
  (let [ids (#'tfm/collect-used-media fdata)]
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

        (with-open [^InputStream stream (sto/get-object-data storage obj)]
          (write-stream! ostream :storage-object-data stream size))))))

(defn read-import!
  [{:keys [pool storage profile-id]} ^DataInputStream istream]
  (letfn [(import-storage-object! [storage result id]
            (let [sid   (read-uuid! istream)
                  sobj  (read-blob! istream :storage-object true)
                  smeta (read-blob! istream :storage-metadata true)]
              (when (not= sid id)
                (ex/raise :type :assertion
                          :code :storage-object-id-does-not-match
                          :hint (format "received storage id %s, expected %s" sid id)))
              (let [[size stream] (read-stream! istream :storage-object-data)
                    content       (sto/content stream size)
                    params        (-> smeta
                                      (assoc :content content)
                                      (assoc :touched-at (dt/now))
                                      (assoc :reference "file-media-object"))]
                (assoc result id (sto/put-object storage params)))))

          (process-form [index form]
            (cond-> form
              ;; Relink Image Shapes
              (and (map? form)
                   (map? (:metadata form))
                   (= :image (:type form)))
              (update-in [:metadata :id] #(get index % %))))

          ;; A function responsible to analyze all file data and
          ;; replace the old :component-file reference with the new
          ;; ones, using the provided file-index
          (relink-shapes [data index]
            (walk/postwalk (partial process-form index) data))

          ;; A function responsible of process the :media attr of file
          ;; data and remap the old ids with the new ones.
          (relink-media [media index]
            (reduce-kv (fn [res k v]
                         (let [id (get index k)]
                           (if (uuid? id)
                             (-> res
                                 (assoc id (assoc v :id id))
                                 (dissoc k))
                             res)))
                       media
                       media))]

    (let [file-id    (uuid/next)
          project-id (some-> (profile/retrieve-additional-data pool profile-id) :default-project-id)
          header     (read-header! istream)]


      (when-not project-id
        (ex/raise :type :validation
                  :code :unable-to-lookup-project))

      (when (or (not= (:type header) :penpot-binary-export)
                (not= (:version header) 1))
        (ex/raise :type :validation
                  :code :invalid-import-file))

      (db/with-atomic [conn pool]
        (let [storage (media/configure-assets-storage storage conn)
              file    (read-blob! istream :file true)
              fdata   (read-blob! istream :fdata true)
              fmedia  (read-blob! istream :fmedia true)
              sids    (read-blob! istream :sids true)
              objects (reduce (partial import-storage-object! storage) {} sids)

              ;; Generate index of old->new fmedia id
              index   (reduce #(assoc %1 (:id %2) (uuid/next)) {} fmedia)

              fdata   (-> fdata
                          (assoc :id file-id)
                          (pmg/migrate-data)
                          (update :pages-index relink-shapes index)
                          (update :components relink-shapes index)
                          (update :media relink-media index))]


          ;; Create an empty filie
          (m.files/create-file conn {:id file-id
                                     :name (:name file)
                                     :project-id project-id
                                     :profile-id profile-id
                                     :data (blob/encode fdata)})

          (prn "----------------- INDEX")
          (fipp.edn/pprint index)
          (doseq [{:keys [media-id thumbnail-id] :as item} fmedia]
            (let [params {:id (uuid/next)
                          :file-id file-id
                          :is-local (:is-local item)
                          :name (:name item)
                          :media-id (get index media-id)
                          :thumbnail-id (get index thumbnail-id)
                          :width (:width item)
                          :height (:height item)
                          :mtype (:mtype item)}]
              (prn "=========================================")
              (fipp.edn/pprint item)
              (fipp.edn/pprint params)
              (db/insert! conn :file-media-object params))))))))

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
