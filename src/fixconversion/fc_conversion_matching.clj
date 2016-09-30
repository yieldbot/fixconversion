(ns fixconversion.fc-conversion-matching
  (:import [kafka.javaapi.consumer ConsumerConnector])
  (:require [clojure.tools.logging :as log]
            [prismo.core :as p]
            [cheshire.core :as json]
            [clj-http.client :as httpclient]
            [clj-time.coerce :as tc]
            [clj-time.core :as time]
            [clj-kafka.producer :as kafka]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.bulk :as esb]
            [clojurewerkz.elastisch.rest.document :as esrd]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojurewerkz.elastisch.query :as q]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [postal.core :refer [send-message]]
            [fixconversion.elasticinsert :as elasticinsert])
  (:gen-class))

(def make-es-client (memoize esr/connect))

(def c-src-click "click")
(def c-src-impr "adimpression")
(def c-src-unknown "unknown")
(def bad-data-psn "_bad_data_")

(def bad-event-topic "bad-event")
(def matched-conversion-topic "conversion")
(def test-conversion-topic "testconversion")
(def conversion-lookback (* 1000 60 60 24 30)) ; 30 days lookback window
(def max-attempts 30) ; max window size for matching conversions.

(def kafka-producer (atom nil))

(def total-items (atom 0))

(defn get-content
[content]
(json/parse-string (json/generate-string content)))


(defn make-kafka-producer
  "Build a kafka-producer"
  [props]
  (let [broker-list (get props :kafka.producer.list)]
    (kafka/producer {"metadata.broker.list" broker-list
                     "serializer.class" "kafka.serializer.DefaultEncoder"
                     "partitioner.class" "kafka.producer.DefaultPartitioner"})))

(defn handle-s3-batch
  [props topic json-docs num-items]
  (elasticinsert/handle-kafka-batch topic (get props :es.url) json-docs false)
  (reset! json-docs [])
  (reset! num-items 0))


(defn send-kafka-msg
  "Sends kafka msgs to the given topic"
  [props topic msg]
  (let [json-docs (atom [])
        num-items (atom 0)
        json-msg (json/generate-string msg)]
    (log/info "topic **** kafka-msg**********" topic  json-msg)
    (swap! json-docs conj msg)
    (swap! num-items inc)
    (swap! total-items inc)
    (log/info "KAFKA processing batch with num-items=" @num-items ", total-items so far=" @total-items)
    (handle-s3-batch props topic json-docs num-items)))
    
(defn send-kafka-msg-backup
  "Sends kafka msgs to the given topic"
  [props topic msg]
  (let [json-msg (json/generate-string msg)]
    (when (and (= matched-conversion-topic topic)
               (not (= (get msg "cvsrc") "unknown")))
      (log/info "topic **** kafka-msg**********" topic  json-msg))))

(defn add-pending-conversion-es
  [props event]
  (log/info (format "******ES add pending-conversion ri=%s, sts=%s" (get event "ri") (get event "sts")))
  (let [attempts (get event "attempts" 0)
        conn (esr/connect (get props :es.url))
        index-name "pending-conversion-temp-1"
        c-id  (str (get event "ri") "-" (get event "sts"))
        new-event (assoc event
                         "attempts" (inc attempts))]
    (esrd/put conn index-name "conversion" c-id new-event)))

(defn add-pending-conversion-badevents
  [props event]
  (log/info (format "******ES add pending-conversion-badevents ri=%s, sts=%s" (get event "ri") (get event "sts")))
  (let [attempts (get event "attempts" 0)
        conn (esr/connect (get props :es.url))
        index-name "pending-conversion-temp-badevents"
        c-id  (str (get event "ri") "-" (get event "sts"))
        new-event (assoc event "reason" "14-cnt-2")]
    (esrd/put conn index-name "conversion" c-id new-event)))

(defn get-server-timestamp
  []
  (str (tc/to-long (time/now))))

(defn send-test-conversion-success-email
  [new-event]
  (log/info "send-test-conversion-success-email called for event " new-event)
  (send-message {:from (str "Yieldbot on " (java.net.InetAddress/getLocalHost))
                 :to [(get new-event "c_email")]
                 :subject "successful test conversion"
                 :body (json/generate-string new-event {:pretty true})}))

(defn merge-parent-event
  [event parent-info]
  (let [rest-event (into {}
                         (for [[k v] parent-info]
                           [(if (.startsWith k "pv_") k (str "c_" k)) v]))
        c-test-event (if (get rest-event "c_test") {"test" true} {})
        new-event (merge event rest-event c-test-event
                         {"psn" (get parent-info "psn")
                          "asn" (get parent-info "asn")
                          "cv_sts" (str (get event "sts"))
                          "sts" (str (get event "sts"))})]
    (try
      (when (get new-event "c_test")
        (send-test-conversion-success-email new-event))
      (catch Exception e
        (log/info "merge-parent-event had exception sending email event=" new-event " e=" (.getMessage e))))
    new-event))

(defn process-parent-info
  [props event parent-info]
  (let [e-sts (Long. (get event "sts" 0))
        p-sts (Long. (get parent-info "sts" 0))
        diff (if (> e-sts p-sts) (- e-sts p-sts) (- p-sts e-sts))]
    (if (> diff conversion-lookback)
      (do
        (log/info "conversion against 30 day old click/impr, parent-info=" parent-info)
        (log/info "conversion against 30 day old click/impr, parent-sts=" (get parent-info "sts" 0) " event=" event)
        (add-pending-conversion-badevents props 
                                        (assoc event
                                                     "reason" (str "conversion against 30 day old click/impr, parent-sts=" (get parent-info "sts" 0))
                                                     "psn" bad-data-psn))
        (send-kafka-msg-backup props bad-event-topic (assoc event
                                                     "reason" (str "conversion against 30 day old click/impr, parent-sts=" (get parent-info "sts" 0))
                                                     "psn" bad-data-psn)))
      (let [final-event (merge-parent-event event parent-info)]
        (log/info "Found parent-info for event:ri=" (get event "ri") "sts" (get event "sts"))
        (if (get final-event "c_test")
          (send-kafka-msg props test-conversion-topic final-event)
          (send-kafka-msg props matched-conversion-topic final-event))))))

(defn remove-pending-cv-key
  [props pending-index pending-cv-key]
  (let [conn (esr/connect (get props :es.url))]
    (log/info "remove-pending-cv-key pending-cv-key=" pending-cv-key)
    (esrd/delete conn pending-index "conversion" pending-cv-key)))

(defn drop-expired-conversion
  [props event pending-cv-key pending-index]
  (log/info (format "conversion attempts more than %s, expiring it, pending-cv-key=%s" max-attempts pending-cv-key))
  (send-kafka-msg-backup props bad-event-topic
                  (assoc event
                         "reason" "conversion attempts more than 30"
                         "psn" bad-data-psn))
  (add-pending-conversion-badevents props 
                    (assoc event
                         "reason" "conversion attempts more than 30"
                         "psn" bad-data-psn))
  (remove-pending-cv-key props pending-index pending-cv-key))

(defn match-conversion
  [props event cvsrc ri pending-cv-key pending-index]
  (let [conn (esr/connect (get props :es.url))
        index-name "adevents-*"
        mapping-types (format "%s,test%s" cvsrc cvsrc)
        results (esrd/search conn index-name mapping-types :query (q/term :ri ri))
        parent-info-es (get-content (into {}(->> results esrsp/hits-from (map :_source))))]
    (if (not-empty parent-info-es)
      (do
        (process-parent-info props event  parent-info-es)
        (when pending-cv-key
          (remove-pending-cv-key props pending-index pending-cv-key)))
      (do
        (log/info "could not find parent-info for conv=" event)
        (if (and (> (get event "attempts" 0) max-attempts) pending-cv-key)
          (drop-expired-conversion props event pending-cv-key pending-index)
          (add-pending-conversion-es props event)
          )))))


(defn handle-conversion
  [props json-msg pending-cv-key pending-index]
  (let [ri (get json-msg "ri")
        cvsrc (get json-msg "cvsrc" c-src-click)
        psn (get json-msg "psn" nil)
        event (assoc json-msg "cvsrc" cvsrc "psn" psn)]
    (if-not ri
      (send-kafka-msg-backup props bad-event-topic (assoc json-msg
                                                   "reason" "no ri present"
                                                   "psn" bad-data-psn))
      (if (nil? psn)
        (condp = cvsrc
          c-src-unknown (send-kafka-msg props matched-conversion-topic event)
          c-src-click (match-conversion props event cvsrc ri pending-cv-key pending-index)
          c-src-impr (match-conversion props event cvsrc ri pending-cv-key pending-index)
          (send-kafka-msg-backup props bad-event-topic (assoc event
                                                       "reason" "unnexpected cvsrc"
                                                       "psn" bad-data-psn)))))))

(defn run-pending-reads
  [props]
  (while true
    (try
      (let [conn (esr/connect (get props :es.url))
            pending-index "pending-conversion-temp-1"
            results (esrd/search conn pending-index "conversion" :size 5000 :query (q/match-all))
            total-pending ((results :hits) :total)
            rows (->> results esrsp/hits-from (map :_id))]
        (log/info (format "run-pending-reads found %s pending entries" total-pending))
        (doseq [key rows]
          (let[results1 (esrd/search conn pending-index "conversion" :filter {:bool {:must {:term {:_id key}}}})]
            (log/info "Pending ri-sts *********** heremap" key (get-content (into {}(->> results1 esrsp/hits-from (map :_source)))))
            (handle-conversion props (get-content (into {}(->> results1 esrsp/hits-from (map :_source)))) key pending-index))))
      (catch Exception e
        (log/error (str "run-pending-reads caught exception e=" e))))
    (Thread/sleep 6000)))

(defn handle-kafka-batch
  [props json-docs]
  (doseq [json-msg @json-docs] (handle-conversion props json-msg nil nil)))

(defn init
  [props]
  (reset! kafka-producer (make-kafka-producer props))
  ;;(launch-log-readiness)
  (future 
          (try
            (run-pending-reads props)
            (catch Exception e
              (log/info (str "launch-pending-reads loop caught exception " (.getMessage e)))))))
  
;;TBD need to create an alert , fail consul check
