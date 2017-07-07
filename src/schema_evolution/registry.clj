(ns schema-evolution.registry)


;;Schema Registry stuff/ not sure whether it's useful...
(defn check-schema-compatibility [registered-schema schema]
  (assert (= registered-schema schema)))

(defn qualified-name [schema]
  (let [name (get schema "name")
        namespace (get schema "namespace")]
    (str namespace "/" name)))

(defn register-schema [schema-registry schema]
  (let [qualified-name (qualified-name schema)]
    (if-let [registered-schema (get schema-registry qualified-name)]
      (do (check-schema-compatibility registered-schema schema)
          schema-registry)
      (assoc schema-registry qualified-name schema))))