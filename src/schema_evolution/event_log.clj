(ns schema-evolution.event-log
  (:require [schema-evolution.serialisation :as s]))

(defn append-event [{:keys [:schema] :as event-log} event]
  (let [serialised-event (s/serialise schema event)]
    (update event-log :events conj serialised-event)))

(defn inc-offset [{:keys [:events :offset] :as event-log}]
  (if (< offset (count events))
    (update event-log :offset inc)
    event-log))

(defn consume-last-event [{:keys [:events :schema :offset]}]
  (if-let [bytes (get events (dec offset))]
    (s/deserialise schema bytes)))

(defn publish! [event-log-atom event]
  (swap! event-log-atom append-event event)
  nil)

(defn consume! [event-log-atom]
  (let [new-event-log (swap! event-log-atom inc-offset)]
    (consume-last-event new-event-log)))



(def user-schema-map
  {
   "namespace" "example.avro"
   "type"      "record"
   "name"      "User"
   "fields"    [
                {
                 "name" "a"
                 "type" "string"
                 }
                {
                 "name"    "b"
                 "type"    ["null" "int"]
                 "default" nil
                 }
                ]
   })

(def user-event-log-atom (atom {:schema user-schema-map
                                :events []
                                :offset 0
                                }))





(publish! user-event-log-atom  {"a" "Bob" "b" (int 0)})

(consume! user-event-log-atom)