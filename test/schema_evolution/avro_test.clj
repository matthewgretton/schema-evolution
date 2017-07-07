(ns schema-evolution.avro-test
  (:require [schema-evolution.avro :as avro]
            [abracad.avro :as abracad.avro]
            [clojure.test :refer :all]
            [abracad.avro.util :as avro-util]
            [schema-evolution.schema :as schema]
            [schema-evolution.schema :as ss])
  (:import (org.apache.avro.generic GenericEnumSymbol GenericData$EnumSymbol)
           (org.apache.avro Schema$StringSchema)))


(def schema-data
  {
   :namespace "example.avro"
   :type      "record"
   :name      "example"
   :fields    [

               {
                :name "stringField"
                :type :string
                }

               {
                :name "recordField"
                :type {
                       :namespace "example.avro"
                       :type      "record"
                       :name      "blah"
                       :fields    [
                                   {
                                    :name "b"
                                    :type :string
                                    }
                                   ]
                       }
                }

               {
                :name "recordArrayField"
                :type {:type  "array"
                       :items {
                               :namespace "example.avro"
                               :type      "record"
                               :name      "blah2"
                               :fields    [
                                           {
                                            :name "b"
                                            :type :string
                                            }
                                           ]
                               }}
                }

               {
                :name "primitveArrayField"
                :type {:type  "array"
                       :items "schema_evolution.avro.primitive_schema"}
                }
               ;
               {
                :name "stringArrayField"
                :type {:type  "array"
                       :items :string}
                }

               {
                :name "enumField"
                :type {:type      "enum",
                       :namespace "schema_evolution.avro"
                       :name      "blah",
                       ;; In order to be able to serialise the schema data, symbols must be strings, as AVRO has
                       ;; no concept of keywords. When serialising data however enums are treated as keywords see
                       ;; event data below.
                       :symbols   [
                                   "null"
                                   "string"
                                   ]}
                }
               ]
   })

(def schema (avro/parse-schema ss/event-schema-schema schema-data))

(def event-data
  {:stringField        "Bob"
   :recordField        {:b "Bob"}
   :recordArrayField   [{:b "Bob"}]
   :primitveArrayField [:string]
   :stringArrayField   ["Bob"]
   :enumField          :null})

(deftest schema-test
  "Test our whether we can use our defined record schema, schema to "

  (is (= schema-data (->> (avro/serialise schema/event-schema-schema schema-data) (avro/deserialise schema/event-schema-schema)))
      "Test serialising and deserialising a schema")

  (is (= event-data (->> (avro/serialise schema event-data) (avro/deserialise schema)))
      "Test serialising and deserialising  example data using the schema"))




