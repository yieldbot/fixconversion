(ns fixconversion.elasticinsert
  (:require [clojure.tools.logging :as log]
            [clj-time.core :as time]
            [clj-time.format :as tf]
            [clj-time.coerce :as tcoerce]
            [clojure.string :refer [lower-case]]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.bulk :as esb]
            [es-mapbox.mappings.events :as esevents])
  (:gen-class))

(defmulti as-bigdec class)
(defmethod as-bigdec String [s]
  (if (= s "")
    (bigdec 0)
    (bigdec s)))
(defmethod as-bigdec Integer [i] (bigdec i))
(defmethod as-bigdec Long [l] (bigdec l))
(defmethod as-bigdec Double [d] d)
(defmethod as-bigdec nil [i] (bigdec 0))
(def total-items (atom 0))

(defn sanitize-event
  [event]
  (reduce merge
          (map (fn [[k v]]
                 (cond
                   (and (contains? #{"cts_ns" "cts_js" "cts_ini" "cts_res" "cts_ad" "cts_rend" "cts_imp"} k)
                        (= v "undefined"))
                   {}
                   (and (contains? #{"pvd" "pv_pvd"} k)
                        (or
                         (= Double/NaN v)  ;; for numeric Nan value
                         (= "nan" (lower-case v)))) ;; for string nan value
                   {k 0}
                   :else {k v}))
               event)))

(defn filter-event-fields
  [evt-type event]
  (condp = evt-type
    "tagdata" (select-keys event esevents/pageview_event_fields)
    "adimpression" (select-keys event esevents/adimpression_event_fields)
    "adserved" (select-keys event esevents/adserved_event_fields)
    "admiss" (select-keys event esevents/admiss_event_fields)
    "click" (select-keys event esevents/click_event_fields)
    "clickmiss" (select-keys event esevents/clickmiss_event_fields)
    "conversion" (select-keys event esevents/conversion_event_fields)
    "matcher" (select-keys event esevents/matcher_event_fields)
    "swipe" (select-keys event esevents/swipe_event_fields)
    "unknown-conversion" (select-keys event esevents/conversion_event_fields)
    "videoevent" (select-keys event esevents/videoevent_event_fields)
    "testclick" (select-keys event esevents/click_event_fields)
    "testadimpression" (select-keys event esevents/adimpression_event_fields)
    "testconversion" (select-keys event esevents/conversion_event_fields)))

(defn filter-nested-fields
  [event]
  (reduce merge
          (map (fn [[k v]]
                 (cond
                   (= k "region_perf")
                   {k (select-keys v esevents/region_perf_fieldkeys)}
                   (= k "misc")
                   {k (select-keys v esevents/misc_fieldkeys)}
                   (= k "pua")
                   {k (select-keys v esevents/pua_fieldkeys)}
                   :else {k v}))
               event)))

(defn get-domain
  [url]
  (when url
    (let [groups (re-find #"(https?://)([^:/]*)" url)]
      (when (and groups (>= (count groups) 3))
        (let [domain (nth groups 2)]
          (if (.startsWith domain "www.")
            (subs domain 4)
            domain))))))

(defn get-dref
  [evt-type event r-fname]
  (let [r-val (get event r-fname nil)]
    (if r-val
      (let [domain (get-domain r-val)]
        (if domain domain "(direct)"))
      "(direct)")))

(defn add-dref
  [evt-type event]
  (let [dref-fname (if (contains? esevents/pub-event-types evt-type) "dref" "pv_dref")
        r-fname (if (contains? esevents/pub-event-types evt-type) "r" "pv_r")]
    (if-not (get event dref-fname)
      (conj event {dref-fname (get-dref evt-type event r-fname)})
      event)))

(defn add-cpx-fields
  [evt-type event]
  (let [cpc_in (get event "cpc_in")
        cpc_out (get event "cpc_out")
        cpm_in (get event "cpm_in")
        cpm_out (get event "cpm_out")]
    (conj event
          (when cpc_in {"cpc100k_in" (long (* 100000 (as-bigdec cpc_in)))})
          (when cpc_out {"cpc100k_out" (long (* 100000 (as-bigdec cpc_out)))})
          (when cpm_in {"cpm100k_in" (long (* 100000 (as-bigdec cpm_in)))})
          (when cpm_out {"cpm100k_out" (long (* 100000 (as-bigdec cpm_out)))}))))

(defn fix-videoevent-psn
  [evt-type event]
  (let [psn (get event "psn")
        pv_psn (get event "pv_psn")]
    (if (and (nil? psn) (= evt-type "videoevent"))
      (conj event {"psn" pv_psn})
      event)))

(defn add-init-timings
  [evt-type event]
  (try
    (let [cts_res (Long. (get event "cts_res" 0))
          cts_rend (Long. (get event "cts_rend" 0))
          cts_ns_or_js (Long. (get event "pv_cts_ns" (get event "pv_cts_js" 0)))
          res-diff (- cts_res cts_ns_or_js)
          rend-diff (- cts_rend cts_ns_or_js)]
      (condp = evt-type
        "adserved" (conj event {"res_time" (if (< res-diff 0) -1 res-diff)})
        "adimpression" (conj event {"rend_time" (if (< rend-diff 0) -1 rend-diff)})
        event))
    (catch Exception e
      (log/info "add-init-timings exception e= " e)
      event)))

(defn modify-event
  [evt-type event]
  (->>
   event
   (add-dref evt-type)
   (add-cpx-fields evt-type)
   (add-init-timings evt-type)
   (fix-videoevent-psn evt-type)))

(defn create-es-event
  [evt-type event]
  (let [toplevel-filtered (filter-event-fields evt-type (modify-event evt-type event))
        nested-filtered (filter-nested-fields toplevel-filtered)]
    (if (empty? nested-filtered)
      {}
      (sanitize-event nested-filtered))))


(defn create-es-event-with-logs
  [evt-type event]
  (let [final-event (create-es-event evt-type event)
        ri (get event "ri" nil)
        final-ri (get final-event "ri" nil)]
    (log/info "calling create-es-event-1 with ri: " ri " final-ri: " final-ri)
    final-event))

(defn get-event-type
  [evt-type]
  (condp = evt-type
    "temp-conversion" "conversion"
    "tagdata" "pageview"
    evt-type))

(defn get-index-name
  [event evt-type sts]
  (try
    (let [prefix (case evt-type
                   "tagdata" "pubevents"
                   "matcher" "matcherevents"
                   "adevents")
          isostr (tf/unparse (tf/with-zone (tf/formatter "YYYY-MM-dd") (time/time-zone-for-id "UTC")) (tcoerce/from-long (Long. sts)))]
      (str prefix "-" isostr))
    (catch Exception e
      (do
        (log/error "Caught exception for event " event)
        (throw e)))))

(defn get-event-id
  [evt-type event]
  (case evt-type
    ("tagdata" "matcher") (event "pvi")
    ("temp-conversion" "click" "testclick" "conversion" "testconversion" "admiss" "clickmiss" "videoevent" "swipe") (str (event "ri") "_" (event "sts"))
    (event "ri")))

(defn drop-restricted-fields
  [source]
  (dissoc source "_type" "_id" "_uid" "_source" "_all" "_parent" "_field_names" "_routing" "_index" "_size" "_timestamp" "_ttl"))

(defn create-envelop-docs
  [evt-type json-docs]
  (let [agg-docs (map
                  (fn [item]
                    (assoc (drop-restricted-fields (item "source"))
                           :_id (item "id")
                           :_type (item "type")
                           :_index (item "index")))
                  @json-docs)]
    (remove nil? agg-docs)))

(defn create-fulldoc-event
  [evt-type item]
  (try
    (let [es-evt-type (get-event-type evt-type)]
      (when-not (and (= es-evt-type "pageview")
                     (.startsWith (item "ua" "nomatch") "yieldbot-internal-crawler"))
        (assoc (create-es-event evt-type item)
               :_id (get-event-id evt-type item)
               :_type es-evt-type
               :_index (get-index-name item evt-type (item "sts")))))
    (catch Exception e
      (do
        (log/error "create-fulldoc-event: caught exception for event " item)
        (throw e)))))

(defn create-es-docs
  [evt-type json-docs]
  (let [es-docs (map (fn [item]
                       (create-fulldoc-event evt-type item))
                     @json-docs)]
    (remove nil? es-docs)))

(defn log-errors
  [bulk-resp]
  (when (get bulk-resp :errors)
    (let [items (bulk-resp :items)]
      (doseq [entry items]
        (when (> ((entry :index) :status) 299)
          (log/error "indexing error item =" entry))))))

(defn insert-kafka-batch
  [evt-type es-url json-docs envelop]
  (let [conn (esr/connect es-url)
        bulk-ops (if envelop
                    (esb/bulk-index (create-envelop-docs evt-type json-docs))
                    (esb/bulk-index (create-es-docs evt-type json-docs)))
        bulk-resp (esb/bulk conn bulk-ops {:refresh true})]
        (log/info "insert-kafka-batch-1: " evt-type)
    (when (get bulk-resp :errors)
      (log-errors bulk-resp))))

(defn handle-kafka-batch
  ([evt-type es-url json-docs envelop]
   (insert-kafka-batch evt-type es-url json-docs envelop)))

(defn init
  [props])
