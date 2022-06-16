;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) UXBOX Labs SL

(ns app.rpc.commands.binfile
  (:refer-clojure :exclude [assert])
  (:require
   [app.common.data :as d]
   [app.common.exceptions :as ex]
   [app.common.geom.point :as gpt]
   [app.common.logging :as l]
   [app.common.pages.migrations :as pmg]
   [app.common.pprint :as pp]
   [app.common.spec :as us]
   [app.common.transit :as t]
   [app.common.uuid :as uuid]
   [app.config :as cf]
   [app.db :as db]
   [app.media :as media]
   [app.rpc.mutations.files :refer [create-file]]
   [app.rpc.queries.comments :as comments]
   [app.rpc.queries.files :refer [decode-row]]
   [app.rpc.queries.profile :as profile]
   [app.rpc.retry :as retry]
   [app.storage :as sto]
   [app.tasks.file-gc]
   [app.util.fressian :as fres]
   [app.util.blob :as blob]
   [app.util.services :as sv]
   [app.util.time :as dt]
   [clojure.java.io :as io]
   [clojure.spec.alpha :as s]
   [clojure.walk :as walk]
   [cuerdas.core :as str]
   [datoteka.core :as fs]
   [yetti.adapter :as yt]
   [yetti.response :as yrs])
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
;; LOW LEVEL STREAM IO
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:const buffer-size (:xnio/buffer-size yt/defaults))
(def ^:const penpot-magic-number 800099563638710213)

(defn get-mark
  [id]
  (case id
    :header  1
    :blob    2
    :stream  3
    :uuid    4
    :label   5
    :obj     6
    (ex/raise :type :validation
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
;;       (proxy-super close)))

(defmacro assert
  [expr hint]
  `(when-not ~expr
     (ex/raise :type :validation
               :code :unexpected-condition
               :hint ~hint)))

(defmacro assert-mark
  [v type]
  `(let [expected# (get-mark type)
         val#      (long ~v)]
    (when (not= val# expected#)
      (ex/raise :type :validation
                :code :unexpected-mark
                :hint (format "received mark %s, expected %s" val# expected#)))))

(defmacro assert-label
  [~expr label]
  `(let [v# ~expr]
     (when (not= v# ~label)
       (ex/raise :type :assertion
                 :code :unexpected-label
                 :hint (format "received label %s, expected %s" v# ~label)))))


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

;; --- COMPOSITE

(defmacro assert-read-label!
  [istream expected-label]
  `(let [readed# (readed-label! ~istream)
         expected# ~expected-label]
     (when (not= readed# expected#)
       (ex/raise :type :validation
                 :code :unexpected-label
                 :hint (format "unxpected label found: %s, expected: %s" readed# expected#)))))

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

(defn write-obj!
  [^DataOutputStream ostream data]
  (let [^bytes data (fres/encode data)]
    (doto ostream
      (write-byte! (get-mark :obj))
      (write-long! (alength data))
      (write-bytes! data))))

(defn read-obj!
  [^DataInputStream istream]
  (let [m (read-byte! istream)]
    (assert-mark m :obj)
    (let [size (read-long! istream)]
      (assert (pos? size) "incorrect header size found on reading header")
      (let [buff (byte-array size)]
        (read-bytes! istream buff)
        (fres/decode buff)))))

(defn write-obj-with-label!
  [ostream label data]
  (write-label! ostream label)
  (write-obj! ostream data))

(defn read-obj-with-label!
  [ostream expected-label]
  (assert-label (read-label! ostream) expected-label)
  (read-obj! ostream))

(defn write-header!
  [^DataOutputStream ostream data]
  (doto ostream
    (write-byte! (get-mark :header))
    (write-long! penpot-magic-number)
    (write-obj! data)))

(defn read-header!
  [^DataInputStream istream]
  (let [mrk (read-byte! istream)
        mnb (read-long! istream)]
    (assert-mark mrk :header)
    (assert (= mnb penpot-magic-number) "invalid magic number on parsing header")
    (read-obj! istream)))

(defn write-blob!
  [^DataOutputStream ostream data]
  (let [^bytes data (if (bytes? data) data (blob/encode data))]
    (doto ostream
      (write-byte! (get-mark :blob))
      (write-long! (alength data))
      (write-bytes! data))))

(defn read-blob!
  ([istream] (read-blob! istream false))
  ([^DataInputStream istream decode?]
   (let [m (read-byte! istream)]
     (assert-mark m :blob)
     (let [size (read-long! istream)]
       (assert (pos? size) "incorrect header size found on reading header")
       (let [buff (byte-array size)]
         (read-bytes! istream buff)
         (if decode?
           (blob/decode buff)
           buff))))))

(defn copy-stream!
  [^DataOutputStream ostream ^InputStream stream ^long size]
  (let [buff (byte-array buffer-size)]
    (IOUtils/copyLarge stream ostream 0 size buff)
    stream))

(defn write-stream!
  [^DataOutputStream ostream stream size]
  (doto ostream
    (write-byte! (get-mark :stream))
    (write-long! size)
    (copy-stream! stream size)))

(defn read-stream!
  [^DataInputStream istream]
  (let [m (read-byte! istream)]
    (assert-mark m :stream)
    (let [size (read-long! istream)]
      [size (doto (BoundedInputStream. istream size)
              (.setPropagateClose false))])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; HIGH LEVEL IMPL
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- retrieve-file
  [pool file-id]
  (->> (db/query pool :file {:id file-id})
       (map decode-row)
       (first)))

(def ^:private sql:file-media-objects
  "SELECT * FROM file_media_object WHERE id = ANY(?)")

(defn- retrieve-file-media
  [pool {:keys [data] :as file}]
  (with-open [conn (db/open pool)]
    (let [ids (app.tasks.file-gc/collect-used-media data)
          ids (db/create-array conn "uuid" ids)]
      (db/exec! conn [sql:file-media-objects ids]))))

(def ^:private storage-object-id-xf
  (comp
   (mapcat (juxt :media-id :thumbnail-id))
   (filter uuid?)))

(def ^:private sql:file-libraries
  "WITH RECURSIVE libs AS (
     SELECT fl.id, fl.deleted_at
       FROM file AS fl
       JOIN file_library_rel AS flr ON (flr.library_file_id = fl.id)
      WHERE flr.file_id = ?::uuid
    UNION
     SELECT fl.id, fl.deleted_at
       FROM file AS fl
       JOIN file_library_rel AS flr ON (flr.library_file_id = fl.id)
       JOIN libs AS l ON (flr.file_id = l.id)
   )
   SELECT DISTINCT l.id
     FROM libs AS l
    WHERE l.deleted_at IS NULL OR l.deleted_at > now();")

(defn- retrieve-libraries
  [pool file-id]
  (map :id (db/exec! pool [sql:file-libraries file-id])))

(def ^:private sql:file-library-rels
  "SELECT * FROM file_library_rel
    WHERE file_id = ANY(?)")

(defn- retrieve-library-relations
  [pool ids]
  (with-open [conn (db/open pool)]
    (db/exec! conn [sql:file-library-rels (db/create-array conn "uuid" ids)])))

(defn write-export!
  [{:keys [pool storage ::ostream ::file-id ::include-libraries?]
    :or {include-libraries? false}}]
  (let [libs    (when include-libraries?
                  (retrieve-libraries pool file-id))
        rels    (when include-libraries?
                  (retrieve-library-relations pool libs))
        sids    (atom #{})]

    ;; Write header with metadata
    (doto ostream
      (write-header! {:version 1
                      :sw-version (:full cf/version)}))

    ;; Write all files & libs (without filedata)
    (doto ostream
      (write-label! :main)
      (write-uuid! file-id)
      (write-obj! libs)
      (write-obj! rels))

    ;; Write all filedata of all files (libraries including)
    (doto ostream
      (write-label! :fdata)
      (write-long! (inc (count libs))))

    (doseq [file-id (cons file-id libs)]
      (let [file  (retrieve-file pool file-id)
            media (retrieve-file-media pool file)]

        ;; Collect all storage ids for later write them all under
        ;; specific storage objects section.
        (swap! sids into (sequence storage-object-id-xf media))

        (doto ostream
          (write-uuid! file-id)
          (write-obj! file)
          (write-obj! media))))

    ;; Write all collected storage objects
    (doto ostream
      (write-label! :sobjects)
      (write-obj! @sids))

    (let [storage (media/configure-assets-storage storage)]
      (doseq [id @sids]
        (let [{:keys [size] :as obj} @(sto/get-object storage id)]
          (doto ostream
            (write-uuid! id)
            (write-obj! obj)
            (write-obj! (meta obj)))

          (with-open [^InputStream stream @(sto/get-object-data storage obj)]
            (write-stream! ostream stream size)))))))

;; TODO: add progress (?)

(defn export-files-response
  [cfg]
  (reify yrs/StreamableResponseBody
    (-write-body-to-stream [_ _ output-stream]
      (time
       (try
         (with-open [ostream (ZstdOutputStream. output-stream 6)]
           (with-open [ostream (DataOutputStream. ostream)]
             (write-export! (assoc cfg ::ostream ostream))))

         (catch java.io.IOException _cause
           ;; Do nothing, EOF means client closes connection abruptly
           nil)
         (catch Throwable cause
           (l/warn :hint "unexpected error on encoding response"
                   :cause cause))
         (finally
           (.close ^OutputStream output-stream)))))))



(defn read-import!
  [{:keys [pool storage ::profile-id ::project-id ::istream] :as cfg}]
  ;; Verify that we received a proper .penpot file
  (try
    (read-header! istream)
    (catch Throwable _cause
      (ex/raise :type :validation
                :code :invalid-penpot-file)))

  (assert-read-label! istream :main)

  (db/with-atomic [conn pool]
    (let [storage (media/configure-assets-storage storage conn)






;; --- Command: export-binfile

(s/def ::file-id ::us/uuid)
(s/def ::profile-id ::us/uuid)

(s/def ::export-binfile
  (s/keys :req-un [::profile-id ::file-id]))

(sv/defmethod ::export-binfile
  "Export a penpot file in a binary format."
  [{:keys [pool] :as cfg} {:keys [profile-id file-id] :as params}]
  {:hello "world"})
