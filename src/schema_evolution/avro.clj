(ns schema-evolution.avro
  (:require [abracad.avro :as avro]
            [clojure.data.json :as json]))

(def parse-schema avro/parse-schema)

(def serialise avro/binary-encoded)

(defn deserialise
  ([schema source]
   (deserialise schema schema source))
  ([writer-schema reader-schema source]
   (let [reader (avro/datum-reader writer-schema reader-schema)
         decoder (avro/binary-decoder source)]
     (.read reader nil decoder))))