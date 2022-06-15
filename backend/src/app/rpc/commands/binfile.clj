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
   [app.rpc.queries.files :as files]
   [app.rpc.queries.profile :as profile]
   [app.rpc.retry :as retry]
   [app.storage :as sto]
   [app.tasks.file-gc]
   [app.util.fressian :as fres]
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

(defn get-mark
  [id]
  (case id
    :header  1
    :blob    2
    :stream  3
    :uuid    4
    :label   5
    :obj     6
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
;;       (proxy-super close)))

(defmacro assert
  [expr hint]
  `(when-not ~expr
     (ex/raise :type :assertion
               :code :unexpected-condition
               :hint ~hint)))

(defmacro assert-mark
  [v type]
  `(let [expected# (get-mark type)
         val#      (long v)]
    (when (not= val# expected#)
      (ex/raise :type :assertion
                :code :unexpected-mark
                :hint (format "received mark %s, expected %s" val# expected#)))))

(defn assert-label
  [n label]
  (when (not= n label)
    (ex/raise :type :assertion
              :code :unexpected-label
              :hint (format "received label %s, expected %s" n label))))

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
      (assert (pos? dlen) "incorrect header size found on reading header")
      (let [buff (byte-array size)]
        (read-bytes! istream buff)
        (fres/decode buff)))))

(defn write-obj-with-label!
  [ostream label data]
  (write-label! ostream label)
  (write-obj! ostream data))

(defn read-obj-with-label!
  [ostream expected-label]
  (let [label (read-label! ostream)]
    (assert-label expected-label label)
    (read-obj! ostream)))

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
       (assert (pos? dlen) "incorrect header size found on reading header")
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

(def penpot-magic-number 800099563638710213)

(def storage-object-id-xf
  (comp (mapcat (juxt :media-id :thumbnail-id))
        (filter uuid?)))

(defn get-used-media
  [pool fdata]
  (let [ids (app.tasks.file-gc/collect-used-media fdata)]
    (with-open [conn (db/open pool)]
      (let [sql "select * from file_media_object where id = ANY(?)"]
        (db/exec! conn [sql (db/create-array conn "uuid" ids)])))))

(defn write-export!
  [{:keys [pool storage]} & {:keys [ostream file-id]}]
  (let [file    (db/get-by-id pool :file file-id)
        fdata   (blob/decode (:data file))
        storage (media/configure-assets-storage storage)
        fmedia  (get-used-media pool fdata)
        sids    (into [] storage-object-id-xf fmedia)]

    (doto ostream
      (write-header! {:version 1 :total-files 1 :sw-version (:full cf/version)})
      (write-label! :files))

    (doto ostream
      (write-label! :file)
      (write-obj! (dissoc file :data)))

    (doto ostream
      (write-label! :fdata)
      (write-obj! fdata))

    (doto ostream
      (write-label! :fmedia)
      (write-obj! fmedia))

    (doto ostream
      (write-label! :sids)
      (write-obj! sids))

    (write-label! :sobjects)
    (doseq [id sids]
      (let [{:keys [size] :as obj} (sto/get-object storage id)]
        (doto ostream
          (write-uuid! id)
          (write-obj! obj)
          (write-obj! (meta obj)))

        (with-open [^InputStream stream (sto/get-object-data storage obj)]
          (write-stream! ostream stream size))))))

;; --- Command: export-binfile

(s/def ::file-id ::us/uuid)
(s/def ::profile-id ::us/uuid)

(s/def ::export-binfile
  (s/keys :req-un [::profile-id ::file-id]))

(sv/defmethod ::export-binfile
  "Export a penpot file in a binary format."
  [{:keys [pool] :as cfg} {:keys [profile-id file-id] :as params}]
  {:hello "world"})
