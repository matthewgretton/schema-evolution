(ns schema-evolution.serialisation
  (:import (java.io File ByteArrayOutputStream IOException)
           (org.apache.avro Schema Schema$Type SchemaCompatibility)
           (org.apache.avro.generic GenericData GenericData$Record GenericDatumWriter GenericDatumReader)
           (org.apache.avro.file DataFileWriter)
           (org.apache.avro.io EncoderFactory BinaryEncoder DecoderFactory JsonDecoder))
  (:require [clojure.data.json :as json]))


(defn create-new-event-log []
  {:events [] :offset 0})

(defn parse-clojure-schema [clojure-schema]
  (-> (json/write-str clojure-schema)
      (Schema/parse)))

(defn map->record [schema map]
  "Convert a clojure map to a GenericData Record. "
  (let [record (new GenericData$Record schema)]
    (doseq [[k v] map]
      (if (map? v)
        (.put record k (map->record (-> (.getField schema k) (.schema)) (get map k)))
        (.put record k v)))
    record))



(defn record->map [record]
  (json/read-json (str record)))



(defn serialise [schema-map data-map]
  "Serialise the supplied data to bytes according to the supplied schema"
  (let [schema (parse-clojure-schema schema-map)
        record (map->record schema data-map)]
    (with-open [out (ByteArrayOutputStream.)]
      (let [encoder (-> (EncoderFactory/get) (.binaryEncoder out nil))
            writer (-> (GenericDatumWriter. schema))]

        (try
          (.write writer record encoder)
          (catch IOException e
            (throw (IOException. "Serialisation error:" e))))

        (.flush encoder)
        (.toByteArray out)))))


(defn deserialise
  ([schema-map bytes]
   (deserialise schema-map schema-map bytes))
  ([writer-schema-map reader-schema-map bytes]
   "Deserialise the bytes to a map of data according to the supplied schema"
   (let [writer-schema (parse-clojure-schema writer-schema-map)
         reader-schema (parse-clojure-schema reader-schema-map)
         reader (GenericDatumReader. writer-schema reader-schema)
         decoder (-> (DecoderFactory/get) (.binaryDecoder bytes nil))
         deserialised-record (try
                               (.read reader nil decoder)
                               (catch IOException e
                                 (throw (IOException. "Deserialisation error:" e))
                                 ))]
     (record->map deserialised-record))))








;;;;;;;;;;;;;;;
;; Use Cases ;;
;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Renaming a Schema:                  ;;
;;                                     ;;
;; Compatibiliy type: F,B              ;;
;;                                     ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def old-schema
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
   })

(def new-schema
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



(->> {"a" "bob"}
     (serialise old-schema)
     (deserialise old-schema))

(->> {"a" "bob"}
     (serialise new-schema)
     (deserialise new-schema))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Renaming a Field:         ;;
;;                           ;;
;; Compatibiliy types: F,B   ;;
;;                           ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def old-schema
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
   })

(def new-schema
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

;; backwards compatibility check
(->> {"a" "bob"}
     (serialise old-schema)
     (deserialise new-schema))


;; forwards compatiblity check
(->> {"b" "bob"}
     (serialise new-schema)
     (deserialise old-schema))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Addding an optional Field: ;;
;;                            ;;
;; Compatibiliy types: F,B    ;;
;;                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;



(def old-schema
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
   })

(def new-schema
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
                 ;; default has to be the same type as the first type
                 "type"    ["null" "string"]
                  "default" nil
                 }
                ]
   })

(defn check-compatiblity [reader-schema writer-schema]
  (->> (SchemaCompatibility/checkReaderWriterCompatibility (parse-clojure-schema reader-schema)
                                                           (parse-clojure-schema writer-schema))))


(str (check-compatiblity old-schema new-schema))





(json/write-str new-schema)

;; backwards compatability
(->> {"a" "bob"}
     (serialise old-schema)
     ;;fortunately schema registery will always add the old schema
     (deserialise old-schema new-schema))

;; forwards compatibility
(->> {"a" "bob"}
     (serialise new-schema)
     (deserialise old-schema))

(->> {"a" "bob"
      "b" "ted"}
     (serialise new-schema)
     (deserialise old-schema))




;1) re-name topic
;2) re-name a field
;4) Make a backwards compatible change
;   a) Adding a new optional field
;5) Make a non backwards compatible change
;   a) Adding a required field/Making an optional field required (removing null type from field)
;      - where default value can be used
;      - where old data must be sourced






