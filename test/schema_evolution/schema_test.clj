(ns schema-evolution.schema-test
  (:require [schema-evolution.avro :as avro]
            [schema-evolution.compatibility :as compatibility]
            [clojure.test :refer :all]))

(defn test-schema-evolution [evolution-desc expected-compatibility-types old-schema new-schema]
  (is (= expected-compatibility-types (compatibility/types old-schema new-schema)) evolution-desc))


(defn test-description [type expected]
  (is (= expected (get compatibility/type-descriptions type))))


(deftest test-descriptions
  (test-description
    :backwards-compatible "Data written by the old-schema can be read by the new-schema")

  (test-description
    :forwards-compatible "Data written by the new-schema can be read by the old-schema"))







(deftest test-schema-evolutions
  (test-schema-evolution "Renaming a Schema"
                         #{:backwards-compatible}
                         {
                          "namespace" "example.avro"
                          "type"      "record"
                          "name"      "blah"
                          "fields"    [
                                       {
                                        "name" "a"
                                        "type" "string"
                                        }
                                       ]
                          }
                         {
                          "namespace" "example.avro"
                          "type"      "record"
                          "name"      "blah2"
                          "aliases"   ["blah"]
                          "fields"    [
                                       {
                                        "name" "a"
                                        "type" "string"
                                        }
                                       ]
                          })

  (test-schema-evolution "Renaming a field"
                         #{:backwards-compatible}
                         {
                          "namespace" "example.avro"
                          "type"      "record"
                          "name"      "blah"
                          "fields"    [
                                       {
                                        "name" "a"
                                        "type" "string"
                                        }
                                       ]
                          }
                         {
                          "namespace" "example.avro"
                          "type"      "record"
                          "name"      "blah"
                          "fields"    [
                                       {
                                        "name"    "b"
                                        "aliases" ["a"]
                                        "type"    "string"
                                        }
                                       ]
                          })

  (test-schema-evolution "Adding an optional Field with a default value"
                         #{:forwards-compatible :backwards-compatible}
                         {
                          "namespace" "example.avro"
                          "type"      "record"
                          "name"      "blah"
                          "fields"    [
                                       {
                                        "name" "a"
                                        "type" "string"
                                        }
                                       ]
                          }
                         {
                          "namespace" "example.avro"
                          "type"      "record"
                          "name"      "blah"
                          "fields"    [
                                       {
                                        "name" "a"
                                        "type" "string"
                                        }

                                       {
                                        "name"    "b"
                                        "type"    ["null" "string"]
                                        "default" nil
                                        }
                                       ]
                          })

  (test-schema-evolution "Adding an optional Field without a default"
                         #{:forwards-compatible}
                         {
                          "namespace" "example.avro"
                          "type"      "record"
                          "name"      "blah"
                          "fields"    [
                                       {
                                        "name" "a"
                                        "type" "string"
                                        }
                                       ]
                          }
                         {
                          "namespace" "example.avro"
                          "type"      "record"
                          "name"      "blah"
                          "fields"    [
                                       {
                                        "name" "a"
                                        "type" "string"
                                        }

                                       {
                                        "name" "b"
                                        "type" ["null" "string"]
                                        }
                                       ]
                          })

  (test-schema-evolution "Adding a required Field"
                         #{:forwards-compatible}
                         {
                          "namespace" "example.avro"
                          "type"      "record"
                          "name"      "blah"
                          "fields"    [
                                       {
                                        "name" "a"
                                        "type" "string"
                                        }
                                       ]
                          }
                         {
                          "namespace" "example.avro"
                          "type"      "record"
                          "name"      "blah"
                          "fields"    [
                                       {
                                        "name" "a"
                                        "type" "string"
                                        }

                                       {
                                        "name" "b"
                                        "type" "string"
                                        }
                                       ]
                          }))










;1) re-name topic
;2) re-name a field
;4) Make a backwards compatible change
;   a) Adding a new optional field
;5) Make a non backwards compatible change
;   a) Adding a required field/Making an optional field required (removing null type from field)
;      - where default value can be used
;      - where old data must be sourced

