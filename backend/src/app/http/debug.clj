;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) UXBOX Labs SL

(ns app.http.debug
  (:require
   [app.common.data :as d]
   [app.common.exceptions :as ex]
   [app.common.spec :as us]
   [app.common.uuid :as uuid]
   [app.config :as cf]
   [app.db :as db]
   [app.db.sql :as sql]
   [app.rpc.mutations.files :as m.files]
   [app.rpc.queries.profile :as profile]
   [app.http.debug.export :as dbg-export]
   [app.util.blob :as blob]
   [app.util.template :as tmpl]
   [app.util.time :as dt]
   [app.worker :as wrk]
   [clojure.java.io :as io]
   [clojure.spec.alpha :as s]
   [cuerdas.core :as str]
   [datoteka.core :as fs]
   [emoji.core :as emj]
   [fipp.edn :as fpp]
   [integrant.core :as ig]
   [markdown.core :as md]
   [markdown.transformers :as mdt]
   [promesa.core :as p]
   [promesa.exec :as px]
   [yetti.request :as yrq]
   [yetti.response :as yrs]))

;; (selmer.parser/cache-off!)

(defn authorized?
  [pool {:keys [profile-id]}]
  (or (= "devenv" (cf/get :host))
      (let [profile (ex/ignoring (profile/retrieve-profile-data pool profile-id))
            admins  (or (cf/get :admins) #{})]
        (contains? admins (:email profile)))))

(defn index
  [{:keys [pool]} request]
  (when-not (authorized? pool request)
    (ex/raise :type :authentication
              :code :only-admins-allowed))
  (yrs/response :status  200
                :headers {"content-type" "text/html"}
                :body    (-> (io/resource "templates/debug.tmpl")
                             (tmpl/render {}))))


(def sql:retrieve-range-of-changes
  "select revn, changes from file_change where file_id=? and revn >= ? and revn <= ? order by revn")

(def sql:retrieve-single-change
  "select revn, changes, data from file_change where file_id=? and revn = ?")

(defn prepare-response
  [body]
  (let [headers {"content-type" "application/transit+json"}]
    (yrs/response :status 200 :body body :headers headers)))

(defn prepare-download-response
  [body filename]
  (let [headers {"content-disposition" (str "attachment; filename=" filename)
                 "content-type" "application/octet-stream"}]
    (yrs/response :status 200 :body body :headers headers)))

(defn- retrieve-file-data
  [{:keys [pool]} {:keys [params] :as request}]
  (when-not (authorized? pool request)
    (ex/raise :type :authentication
              :code :only-admins-allowed))

  (let [file-id  (some-> (get-in request [:params :file-id]) uuid/uuid)
        revn     (some-> (get-in request [:params :revn]) d/parse-integer)
        filename (str file-id)]

    (when-not file-id
      (ex/raise :type :validation
                :code :missing-arguments))

    (let [data (if (integer? revn)
                 (some-> (db/exec-one! pool [sql:retrieve-single-change file-id revn]) :data)
                 (some-> (db/get-by-id pool :file file-id) :data))]

      (when-not body
        (ex/raise :type :not-found
                  :code :enpty-data
                  :hint "empty response"))

      (if (contains? params :download)
        (prepare-download-response data filename)
        (prepare-response (some-> data blob/decode))))))

(defn- upload-file-data
  [{:keys [pool]} {:keys [profile-id params] :as request}]
  (let [project-id (some-> (profile/retrieve-additional-data pool profile-id) :default-project-id)
        data       (some-> params :file :path fs/slurp-bytes blob/decode)]

    (if (and data project-id)
      (let [fname (str "imported-file-" (dt/now))
            file-id (try
                      (uuid/uuid (-> params :file :filename))
                      (catch Exception _ (uuid/next)))
            file (db/exec-one! pool (sql/select :file {:id file-id}))]
        (if file
          (db/update! pool :file
                      {:data (blob/encode data)}
                      {:id file-id})
          (m.files/create-file pool {:id file-id
                                     :name fname
                                     :project-id project-id
                                     :profile-id profile-id
                                     :data data}))
        (yrs/response 200 "OK"))
      (yrs/response 500 "ERROR"))))

(defn file-data
  [cfg request]
  (case (yrq/method request)
    :get (retrieve-file-data cfg request)
    :post (upload-file-data cfg request)
    (ex/raise :type :http
              :code :method-not-found)))

(defn retrieve-file-changes
  [{:keys [pool]} request]
  (when-not (authorized? pool request)
    (ex/raise :type :authentication
              :code :only-admins-allowed))

  (let [file-id  (some-> (get-in request [:params :id]) uuid/uuid)
        revn     (or (get-in request [:params :revn]) "latest")
        filename (str file-id)]

    (when (or (not file-id) (not revn))
      (ex/raise :type :validation
                :code :invalid-arguments
                :hint "missing arguments"))

    (cond
      (d/num-string? revn)
      (let [item (db/exec-one! pool [sql:retrieve-single-change file-id (d/parse-integer revn)])]
        (prepare-response request (some-> item :changes blob/decode vec) filename))

      (str/includes? revn ":")
      (let [[start end] (->> (str/split revn #":")
                             (map str/trim)
                             (map d/parse-integer))
            items       (db/exec! pool [sql:retrieve-range-of-changes file-id start end])]
        (prepare-response request
                          (some->> items
                                   (map :changes)
                                   (map blob/decode)
                                   (mapcat identity)
                                   (vec))
                          filename))
      :else
      (ex/raise :type :validation :code :invalid-arguments))))


(defn retrieve-error
  [{:keys [pool]} request]
  (letfn [(parse-id [request]
            (let [id (get-in request [:path-params :id])
                  id (us/uuid-conformer id)]
              (when (uuid? id)
                id)))

          (retrieve-report [id]
            (ex/ignoring
             (some-> (db/get-by-id pool :server-error-report id) :content db/decode-transit-pgobject)))

          (render-template [report]
            (let [context (dissoc report
                                  :trace :cause :params :data :spec-problems
                                  :spec-explain :spec-value :error :explain :hint)
                  params  {:context (with-out-str
                                      (fpp/pprint context {:width 200}))
                           :hint    (:hint report)
                           :spec-explain  (:spec-explain report)
                           :spec-problems (:spec-problems report)
                           :spec-value    (:spec-value report)
                           :data          (:data report)
                           :trace         (or (:trace report)
                                              (some-> report :error :trace))
                           :params        (:params report)}]
              (-> (io/resource "templates/error-report.tmpl")
                  (tmpl/render params))))]

    (when-not (authorized? pool request)
      (ex/raise :type :authentication
                :code :only-admins-allowed))

    (let [result (some-> (parse-id request)
                         (retrieve-report)
                         (render-template))]
      (if result
        (yrs/response :status 200
                      :body result
                      :headers {"content-type" "text/html; charset=utf-8"
                                "x-robots-tag" "noindex"})
        (yrs/response 404 "not found")))))

(def sql:error-reports
  "select id, created_at from server_error_report order by created_at desc limit 100")

(defn retrieve-error-list
  [{:keys [pool]} request]
  (when-not (authorized? pool request)
    (ex/raise :type :authentication
              :code :only-admins-allowed))
  (let [items (db/exec! pool [sql:error-reports])
        items (map #(update % :created-at dt/format-instant :rfc1123) items)]
    (yrs/response :status 200
                  :body (-> (io/resource "templates/error-list.tmpl")
                            (tmpl/render {:items items}))
                  :headers {"content-type" "text/html; charset=utf-8"
                            "x-robots-tag" "noindex"})))

(defn health-check
  "Mainly a task that performs a health check."
  [{:keys [pool]} _]
  (db/with-atomic [conn pool]
    (db/exec-one! conn ["select count(*) as count from server_prop;"])
    (yrs/response 200 "OK")))

(defn changelog
  [_ _]
  (letfn [(transform-emoji [text state]
            [(emj/emojify text) state])
          (md->html [text]
            (md/md-to-html-string text :replacement-transformers (into [transform-emoji] mdt/transformer-vector)))]
    (if-let [clog (io/resource "changelog.md")]
      (yrs/response :status 200
                    :headers {"content-type" "text/html; charset=utf-8"}
                    :body (-> clog slurp md->html))
      (yrs/response :status 404 :body "NOT FOUND"))))

(defn- wrap-async
  [{:keys [executor] :as cfg} f]
  (fn [request respond raise]
    (-> (px/submit! executor #(f cfg request))
        (p/then respond)
        (p/catch raise))))

(defmethod ig/pre-init-spec ::handlers [_]
  (s/keys :req-un [::db/pool ::wrk/executor]))

(defmethod ig/init-key ::handlers
  [_ cfg]
  {:index (wrap-async cfg index)
   :health-check (wrap-async cfg health-check)
   :retrieve-file-changes (wrap-async cfg retrieve-file-changes)
   :retrieve-error (wrap-async cfg retrieve-error)
   :retrieve-error-list (wrap-async cfg retrieve-error-list)
   :file-data (wrap-async cfg file-data)
   :changelog (wrap-async cfg changelog)
   :export (wrap-async cfg dbg-export/handler cfg)
   :import (wrap-async cfg dbg-export/import-handler)})
