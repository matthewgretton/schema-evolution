(ns schema-evolution.event-log
  (:require [schema-evolution.serialisation :as s]))

(defn create-event-log-ref [schema]
  (ref {:schema schema
        :events []
        :offset 0
        }))

(defn evolve-schema [event-log schema]
  (assoc event-log :schema schema))

(defn append-event [{:keys [:schema] :as event-log} event]
  (let [serialised-event (s/serialise schema event)]
    (update event-log :events conj serialised-event)))

(defn inc-offset [{:keys [:events :offset] :as event-log}]
  (if (< offset (count events))
    (update event-log :offset inc)
    event-log))

(defn consume [{:keys [:events :schema :offset]}]
  (if-let [bytes (get events offset)]
    (s/deserialise schema bytes)))


(defn evolve-schema! [event-log-ref schema]
  (dosync (alter event-log-ref evolve-schema)))


(defn publish! [event-log-ref event]
  (dosync (alter event-log-ref append-event event))
  nil)

(defn consume! [event-log-ref]
  (dosync (if-let [event (consume @event-log-ref)]
            (do (alter event-log-ref inc-offset)
                event))))


(def example-schema
  {
   "namespace" "example.avro"
   "type"      "record"
   "name"      "Example"
   "fields"    [
                {
                 "name" "a"
                 "type" "string"
                 }
                ]
   })

(def event-log-ref (create-event-log-ref example-schema))


(publish! event-log-ref {"a" "Bob" "b" (int 0)})

(consume! event-log-ref)