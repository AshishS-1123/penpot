;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) UXBOX Labs SL

(ns app.rpc.commands.binfile
  (:require
   [app.common.exceptions :as ex]
   [app.common.geom.point :as gpt]
   [app.common.spec :as us]
   [app.db :as db]
   [app.rpc.queries.comments :as comments]
   [app.rpc.queries.files :as files]
   [app.rpc.retry :as retry]
   [app.util.blob :as blob]
   [app.util.services :as sv]
   [app.util.time :as dt]
   [clojure.spec.alpha :as s]))

;; --- Command: export-binfile

(s/def ::file-id ::us/uuid)
(s/def ::profile-id ::us/uuid)

(s/def ::export-binfile
  (s/keys :req-un [::profile-id ::file-id]))

(sv/defmethod ::export-binfile
  "Export a penpot file in a binary format."
  [{:keys [pool] :as cfg} {:keys [profile-id file-id] :as params}]
  {:hello "world"})
