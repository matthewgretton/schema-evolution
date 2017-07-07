(ns schema-evolution.event-log
  (:require [schema-evolution.avro :as avro]
            [schema-evolution.schema :as schema]))

(defn publish [serialised-events writer-schema event]
  {:pre [(not-any? nil? [serialised-events writer-schema event])]}
  (let [parsed-schema (avro/parse-schema writer-schema)
        serialised-event {:event/bytes               (avro/serialise parsed-schema event)
                          :event/writer-schema-bytes (avro/serialise schema/event-schema-schema writer-schema)}]
    (conj serialised-events serialised-event)))

(defn consume [serialised-events reader-schema offset]
  (when-let [{:keys [:event/bytes :event/writer-schema-bytes]} (get serialised-events offset)]
    (let [writer-schema (avro/parse-schema (avro/deserialise schema/event-schema-schema writer-schema-bytes))
          reader-schema (avro/parse-schema reader-schema)
          derserialised-event (avro/deserialise writer-schema reader-schema bytes)]
      derserialised-event)))


;; mutating versions
(defn publish! [event-log-atom schema event]
  (swap! event-log-atom publish schema event))

(defn consume! [event-log-atom offset-ref reader-schema]
  (dosync (let [off-set @offset-ref]
            (if-let [event (consume @event-log-atom reader-schema off-set)]
              [event (commute offset-ref inc)]
              [event off-set]))))

;;;;;;;;;;;;;
;; Example ;;
;;;;;;;;;;;;;

(def event-log-atom (atom []))

(def example-event-schema {
                           :namespace "example.avro"
                           :type      "record"
                           :name      "Example"
                           :fields    [
                                       {
                                        :name "a"
                                        :type "string"
                                        }
                                       ]
                           })

(def example-event {:a "Bob"})

(publish! event-log-atom example-event-schema example-event)


(def example-schema-v2 {
                        :namespace "example.avro"
                        :type      "record"
                        :name      "Example"
                        :fields    [
                                    {
                                     :name "a"
                                     :type "string"
                                     }

                                    {
                                     :name    "b"
                                     :type    ["null" "string"]
                                     :default nil
                                     }
                                    ]
                        })

(def offset-ref (ref 0))

(consume! event-log-atom offset-ref example-schema-v2)







