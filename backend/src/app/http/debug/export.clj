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
   [app.rpc.queries.profile :as profile]
   [app.rpc.commands.binfile :as binf]
   [app.storage :as sto]
   [app.tasks.file-gc]
   [app.util.blob :as blob]
   [app.util.time :as dt]
   [clojure.java.io :as io]
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

;; (defn read-import!
;;   [{:keys [pool storage profile-id]} ^DataInputStream istream]
;;   (letfn [(import-storage-object! [storage result id]
;;             (let [sid   (read-uuid! istream)
;;                   sobj  (read-blob! istream :storage-object true)
;;                   smeta (read-blob! istream :storage-metadata true)]
;;               (when (not= sid id)
;;                 (ex/raise :type :assertion
;;                           :code :storage-object-id-does-not-match
;;                           :hint (format "received storage id %s, expected %s" sid id)))
;;               (let [[size stream] (read-stream! istream :storage-object-data)
;;                     content       (sto/content stream size)
;;                     params        (-> smeta
;;                                       (assoc :content content)
;;                                       (assoc :touched-at (dt/now))
;;                                       (assoc :reference "file-media-object"))]
;;                 (assoc result id (sto/put-object! storage params)))))

;;           (process-form [index form]
;;             (cond-> form
;;               ;; Relink Image Shapes
;;               (and (map? form)
;;                    (map? (:metadata form))
;;                    (= :image (:type form)))
;;               (update-in [:metadata :id] #(get index % %))))

;;           ;; A function responsible to analyze all file data and
;;           ;; replace the old :component-file reference with the new
;;           ;; ones, using the provided file-index
;;           (relink-shapes [data index]
;;             (walk/postwalk (partial process-form index) data))

;;           ;; A function responsible of process the :media attr of file
;;           ;; data and remap the old ids with the new ones.
;;           (relink-media [media index]
;;             (reduce-kv (fn [res k v]
;;                          (let [id (get index k)]
;;                            (if (uuid? id)
;;                              (-> res
;;                                  (assoc id (assoc v :id id))
;;                                  (dissoc k))
;;                              res)))
;;                        media
;;                        media))]

;;     (let [file-id    (uuid/next)
;;           project-id (some-> (profile/retrieve-additional-data pool profile-id) :default-project-id)
;;           header     (read-header! istream)]

;;       (when-not project-id
;;         (ex/raise :type :validation
;;                   :code :unable-to-lookup-project))

;;       (when (or (not= (:type header) :penpot-binary-export)
;;                 (not= (:version header) 1))
;;         (ex/raise :type :validation
;;                   :code :invalid-import-file))

;;       (db/with-atomic [conn pool]
;;         (let [storage (media/configure-assets-storage storage conn)
;;               file    (read-blob! istream :file true)
;;               fdata   (read-blob! istream :fdata true)
;;               fmedia  (read-blob! istream :fmedia true)
;;               sids    (read-blob! istream :sids true)
;;               objects (reduce (partial import-storage-object! storage) {} sids)

;;               ;; Generate index of old->new fmedia id
;;               index   (reduce #(assoc %1 (:id %2) (uuid/next)) {} fmedia)

;;               fdata   (-> fdata
;;                           (assoc :id file-id)
;;                           (pmg/migrate-data)
;;                           (update :pages-index relink-shapes index)
;;                           (update :components relink-shapes index)
;;                           (update :media relink-media index))]


;;           ;; Create an empty filie
;;           (create-file conn {:id file-id
;;                              :name (:name file)
;;                              :project-id project-id
;;                              :profile-id profile-id
;;                              :data (blob/encode fdata)})

;;           (prn "----------------- INDEX")
;;           (pp/pprint index)
;;           (doseq [{:keys [media-id thumbnail-id] :as item} fmedia]
;;             (let [params {:id (uuid/next)
;;                           :file-id file-id
;;                           :is-local (:is-local item)
;;                           :name (:name item)
;;                           :media-id (get index media-id)
;;                           :thumbnail-id (get index thumbnail-id)
;;                           :width (:width item)
;;                           :height (:height item)
;;                           :mtype (:mtype item)}]
;;               (prn "=========================================")
;;               (pp/pprint item)
;;               (pp/pprint params)
;;               (db/insert! conn :file-media-object params))))))))

(defn export-handler
  [cfg {:keys [params] :as request}]
  (let [file-id (some-> params :file-id uuid/uuid)
        libs?   (contains? params :includelibs)]

    (when-not file-id
      (ex/raise :type :validation
                :code :missing-arguments))

    (yrs/response
     :status  200
     :headers {"content-type" "application/octet-stream"
               "content-disposition" (str "attachmen; filename=" file-id ".penpot")}
     :body    (-> cfg
                  (assoc ::binf/file-id file-id)
                  (assoc ::binf/include-libraries? libs?)
                  (binf/export-files-response)))))

;; (defn import-handler
;;   [cfg {:keys [params profile-id] :as request}]
;;   (when-not (contains? params :file)
;;     (ex/raise :type :validation
;;               :code :missing-upload-file
;;               :hint "missing upload file"))
;;   (with-open [istream (io/input-stream (-> params :file :tempfile))]
;;     (with-open [istream (DataInputStream. istream)]
;;       (let [cfg (assoc cfg :profile-id profile-id)]
;;         (read-import! cfg istream)
;;         (yrs/response
;;          :status  200
;;          :headers {"content-type" "text/plain"}
;;          :body    "OK")))))
