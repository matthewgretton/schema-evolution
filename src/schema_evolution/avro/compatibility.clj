(ns schema-evolution.compatibility
  (:import (org.apache.avro SchemaCompatibility))
  (:require [abracad.avro :as avro]))

(defn message [writer-schema reader-schema]
  (str "Data written by the" " " writer-schema " " "can be read by the " reader-schema))

(def type-descriptions
  (let [[new-schema old-schema] ["new-schema" "old-schema"]]
    {:backwards-compatible (message old-schema new-schema)
     :forwards-compatible  (message new-schema old-schema)}))


(defn check [reader-schema writer-schema]
  (-> (SchemaCompatibility/checkReaderWriterCompatibility (avro/parse-schema reader-schema)
                                                          (avro/parse-schema writer-schema))
      (.getType)
      (str)
      (.toLowerCase)
      (keyword)))

(defn types [old-schema new-schema]
  (let [backwards-compatibility (check new-schema old-schema)
        forwards-compatiblity (check old-schema new-schema)]
    (case [backwards-compatibility forwards-compatiblity]
      [:compatible :incompatible] #{:backwards-compatible}
      [:incompatible :compatible] #{:forwards-compatible}
      [:compatible :compatible] #{:backwards-compatible :forwards-compatible}
      [:incompatible :incompatible] #{})))


(def example-schema {
                     :namespace "example.avro"
                     :type     "record"
                     :name      "Example"
                     :fields    [
                                 {
                                  :name "a"
                                  :type "string"
                                  }
                                 ]
                     })


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
                                     :name "b"
                                     :type ["null" "string"]
                                     :default nil
                                     }


                                    ]
                        })

(def example-schema-v3 {
                        :namespace "example.avro"
                        :type      "record"
                        :name      "Example"
                        :fields    [
                                    {
                                     :name "a"
                                     :type "string"
                                     }

                                    {
                                     :name "b"
                                     :type "string"
                                     }


                                    ]
                        })

(->> (types example-schema example-schema-v2)
     (select-keys type-descriptions))

(->> (types example-schema example-schema-v3)
     (select-keys type-descriptions))








