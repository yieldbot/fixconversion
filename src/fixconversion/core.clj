(ns fixconversion.core
  (:import [kafka.javaapi.consumer ConsumerConnector])
  (:require [clojure.tools.cli :refer (parse-opts)]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [compojure.core :refer [defroutes GET]]
            [ring.util.response :refer [resource-response response]]
            [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.middleware.json :refer [wrap-json-response]]
            [ring.adapter.jetty :as jetty]
            [fixconversion.fc-conversion-matching :as gconvmatching]
            [cheshire.core :as json]
            [clj-time.core :as tc]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.bulk :as esb]
            [clojurewerkz.elastisch.rest.document :as esrd]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojurewerkz.elastisch.query :as q]
            [clojurewerkz.elastisch.aggregation :as agg]
            [clj-kafka.core :refer :all]
            [clj-kafka.consumer.zk :as kzk])
  (:gen-class))


(def json-docs (atom []))
(def num-items (atom 0))
(def time-to-log (atom true))
(def total-items (atom 0))
(def  cnt (atom 0))

(def make-es-client (memoize esr/connect))

(defmacro aggs [& body]
  `(-> {} ~@body))

(defn get-content
[content]
(json/parse-string (json/generate-string content)))

(defn log-readiness
  []
  (while true
    (let [min (tc/minute (tc/to-time-zone (tc/now) tc/utc))]
      (if (or (and (>= min 10)  (< min 11))
              (and (>= min 20)  (< min 21))
              (and (>= min 30)  (< min 31))
              (and (>= min 40)  (< min 41))
              (and (>= min 50)  (< min 51))
              (and (>= min 0)  (< min 1)))
        (do (reset! time-to-log true)
            (Thread/sleep 60000))
        (Thread/sleep 30000)))))

(defn launch-log-readiness
  []
  (future (log-readiness)))

(defn handle-kafka-batch
  [props]
  (let [batch-handler (get props :batch.handler)]
    (case batch-handler
      "fc.conversion.matching" (gconvmatching/handle-kafka-batch props json-docs))))

(def cli-options
  [["-c" "--config-file CONFIG-FILE" "Config file "]
   ["-h" "--help"]])

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn  load-resource
  [config-file]
  (let [thr (Thread/currentThread)
        ldr (.getContextClassLoader thr)]
    (read-string (slurp (.getResourceAsStream ldr config-file)))))



(defn collect-kafka-msg
  "collects kafka msgs in an atom"
  [props json-msg]
    (swap! json-docs conj json-msg)
    (swap! num-items inc)
    (swap! total-items inc)
    (when (= 0 (mod @num-items 5))
      (handle-kafka-batch props)
      (reset! json-docs [])
      (reset! num-items 0)))




(defn fixconv-events
  [props]
      (let [conn (esr/connect (get props :es.url))
            pending-index "adevents-2016-09-12"
            ;pending-index "adevents-2016-09-13"
            ;pending-index "adevents-2016-09-13,adevents-2016-09-14"
            ;pending-index "adevents-2016-09-14"
            ;pending-index "adevents-2016-09-14"
            results (esrd/search conn pending-index "conversion" :size 4000 :query (q/match-all) :filter {:bool {:must {:range {:sts {:gte "1473709020000" :lt "1473886800000"}}}}})
            ;results (esrd/search conn pending-index "conversion" :size 6500 :query (q/match-all) :filter {:bool {:must {:range {:sts {:gte "1473709020000" :lt "1473771600000"}}}}})
            ;results (esrd/search conn pending-index "conversion" :size 6500 :query (q/match-all) :filter {:bool {:must {:range {:sts {:gte "1473771600000" :lt "1473800000000"}}}}})
            ;results (esrd/search conn pending-index "conversion" :size 4500 :query (q/match-all) :filter {:bool {:must {:range {:sts {:gte "1473800000000" :lt "1473850000000"}}}}})
            ;results (esrd/search conn pending-index "conversion" :size 9000 :query (q/match-all) :filter {:bool {:must {:range {:sts {:gte "1473850000000" :lt "1473886800000"}}}}})

            total-pending ((results :hits) :total)
            rows (->> results esrsp/hits-from (map :_id))]
        (log/info (format "fixconversion found %s  entries" total-pending))
        (doseq [key rows]
          (let[results1 (esrd/search conn pending-index "conversion" :filter {:bool {:must {:term {:_id key}}}})
            json-msg (get-content (into {}(->> results1 esrsp/hits-from (map :_source))))]
            (swap! cnt inc)
            (log/info "12-cnt=" cnt "ri-sts *********** heremap" key (get-content (into {}(->> results1 esrsp/hits-from (map :_source)))))
            ;;(log/info "key="key "****rows=" rows )
            ;(log/info "ri-sts *********** heremap" key (get-content (into {}(->> results1 esrsp/hits-from (map :_source)))))
            (collect-kafka-msg props json-msg)))))




(defn -main
  "main func "
  [& args]
  (let [{:keys [options arguments summary errors]} (parse-opts args cli-options)]
    (cond
      (:help options) (exit 0 summary)
      (not (:config-file options)) (exit 1 (str "config-file not passed usage=" summary))
      errors (exit 2 error-msg errors))
    (let [{:keys [config-file]} options
          cprops (load-resource config-file)
          batch-handler (get cprops :batch.handler)]
      (log/info "running with config-file=" config-file " edn.data=" cprops)
      (launch-log-readiness)
      (case batch-handler
        "fc.conversion.matching" (gconvmatching/init cprops))
      (try
        ( fixconv-events cprops)
        (catch Exception e
          (do
            (.printStackTrace e)
            (System/exit 2))))
      )))
