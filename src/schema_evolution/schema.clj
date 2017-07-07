(ns schema-evolution.schema
  (:require [schema-evolution.avro :as avro]))

(def event-schema-schema
  (avro/parse-schema
    {
     :namespace "schema_evolution.avro"
     :name      "enum_schema"
     :type      "record"
     :fields    [{
                  :name "namespace"
                  :type "string"
                  }

                 {
                  :name "name"
                  :type "string"
                  }

                 {
                  :name "type"
                  :type "string"
                  }

                 {
                  :name "symbols"
                  :type {:type  "array"
                         :items "string"}
                  }

                 ]}

    {:type      "enum",
     :namespace "schema_evolution.avro"
     :name      "primitive_schema",
     :symbols   [
                 "null"
                 "boolean"
                 "int"
                 "long"
                 "float"
                 "double"
                 "bytes"
                 "string"
                 ]}


    {:namespace "schema_evolution.avro"
     :type      :record
     :name      "record_schema"
     :fields    [
                 {
                  :name "namespace"
                  :type :string
                  }
                 {
                  :name "name"
                  :type :string
                  }

                 {
                  :name "type"
                  :type :string
                  }

                 {
                  :name :fields
                  :type {:type  :array
                         :items {:namespace "schema_evolution.avro"
                                 :type      "record"
                                 :name      "field_schema"
                                 :fields    [
                                             {
                                              :name "name"
                                              :type :string
                                              }
                                             {
                                              :name "type"
                                              :type ["schema_evolution.avro.primitive_schema"
                                                     "schema_evolution.avro.record_schema"
                                                     "schema_evolution.avro.enum_schema"
                                                     ;;did this to handle the array <-> record cyclic relationship...
                                                     {
                                                      :namespace "schema_evolution.avro"
                                                      :name      "array_schema"
                                                      :type      "record"
                                                      :fields    [{
                                                                   :name "type"
                                                                   :type "string"
                                                                   }

                                                                  {
                                                                   :name "items"
                                                                   :type ["schema_evolution.avro.primitive_schema"
                                                                          "schema_evolution.avro.record_schema"
                                                                          "schema_evolution.avro.enum_schema"
                                                                          "schema_evolution.avro.array_schema"
                                                                          :string
                                                                          ]
                                                                   }


                                                                  ]}]
                                              }
                                             ]}}}]}))

