(ns mongo-lib.core
  (:import [com.mongodb MongoClient
                        MongoCredential
                        MongoClientOptions
                        ServerAddress]
           [com.mongodb.client.model Collation
                                     CollationCaseFirst]
           [org.bson Document]
           [org.bson.types ObjectId]))

(def conn (atom nil))

(def db (atom nil))

(defn mongodb-connect
  "Connect to mongo db"
  [db-name]
  (let [admin-db "admin"
        username "admin"
        password (char-array "passw0rd")
        cred (MongoCredential/createCredential username
                                               admin-db
                                               password)
        host "127.0.0.1"
        port 27017]
    (swap! conn
           (fn [conn-input]
            (MongoClient. (ServerAddress. host
                                        port)
                          cred
                          (.build (MongoClientOptions/builder))
             ))
     )
    (swap! db
           (fn [db-input]
            (.getDatabase @conn
                          db-name))
     ))
  )

(defn mongodb-disconnect
  ""
  []
  (.close @conn))

(defn get-collection
  ""
  [db
   collection-name]
  (.getCollection @db
                  collection-name))

(defn build-document
  ""
  [clj-map]
  (let [result  (Document.)]
   (reduce (fn [acc [c-key
                     c-value]]
            (.append result (name c-key)
                            (if (map? c-value)
                             (build-document c-value)
                             c-value))
            )
           result
           clj-map))
  )

(defn build-collation
  ""
  [collation-map]
  (let [collation-builder (Collation/builder)]
   (if (contains? collation-map :locale)
    (.locale collation-builder (:locale collation-map))
    (.locale collation-builder "en_US"))
   (if (contains? collation-map :case-first)
    (.collationCaseFirst collation-builder (case (clojure.string/lower-case
                                                  (:case-first collation-map))
                                            "upper" (CollationCaseFirst/UPPER)
                                            "lower" (CollationCaseFirst/LOWER)
                                            (CollationCaseFirst/OFF))
     )
    nil)
   (.build collation-builder))
  )

(defn mongodb-find
  ""
  [collection
   & [filter-map
      projection-map
      sort-map
      limit
      skip
      collation]]
  (let [collection  (if (string? collection)
                     (get-collection db
                                     collection)
                     collection)
        filter-doc  (build-document filter-map)
        projection-doc  (build-document projection-map)
        sort-doc  (build-document sort-map)
        limit  (or limit 0)
        skip  (or skip 0)
        collation-obj  (build-collation collation)
        itr-result  (.iterator (-> (.find collection filter-doc)
                                   (.projection projection-doc)
                                   (.sort sort-doc)
                                   (.limit limit)
                                   (.skip skip)
                                   (.collation collation-obj)
                                ))
        result-all  (atom [])
        result-single  (atom {})]
   (while (.hasNext itr-result)
    (let [data (.next itr-result)]
     (doseq [e-key (vec (.keySet data))]
      (swap! result-single assoc (keyword e-key) (if (= e-key "_id")
                                                  (str (.get data (str e-key))
                                                   )
                                                  (.get data (str e-key))
                                                  ))
      )
     (swap! result-all conj @result-single)
     (reset! result-single {}))
    )
   @result-all))

(defn mongodb-find-one
  ""
  [collection
   & [filter-map
      projection-map]]
  (first (mongodb-find collection
                       filter-map
                       projection-map
                       nil
                       1))
  )

(defn mongodb-find-by-id
  ""
  [collection
   _id]
  (first (mongodb-find collection
                       {:_id (ObjectId. _id)}
                       nil
                       nil
                       1))
  )

(defn mongodb-insert-one
  ""
  [collection
   insert-document]
  (let [collection  (if (string? collection)
                     (get-collection db
                                     collection)
                     collection)]
   (.insertOne collection (build-document insert-document)))
  )

(defn mongodb-insert-many
  ""
  [collection
   insert-documents-vector]
  (let [collection  (if (string? collection)
                     (get-collection db
                                     collection)
                     collection)
        list-obj  (java.util.ArrayList.)]
   (doseq [insert-document insert-documents-vector]
    (.add list-obj (build-document insert-document))
    )
   (.insertMany collection list-obj))
  )

(defn mongodb-update-by-id
  ""
  [collection
   _id
   update-document]
  (let [collection  (if (string? collection)
                     (get-collection db
                                     collection)
                     collection)]
   (.updateOne collection (build-document {:_id (ObjectId. _id)})
                          (build-document {:$set update-document}))
   ))

(defn mongodb-delete-by-id
  ""
  [collection
   _id]
  (let [collection  (if (string? collection)
                     (get-collection db
                                     collection)
                     collection)]
   (.deleteOne collection (build-document {:_id (ObjectId. _id)}))
   ))

(defn mongodb-count
  ""
  [collection
   & [entity-filter]]
  (let [collection  (if (string? collection)
                     (get-collection db
                                     collection)
                     collection)]
   (.count collection (build-document entity-filter))
   ))

(defn pretty-print
  ""
  []
  (let [result (mongodb-find "grocery"
                             {}
                             {:gname true
                              :fats true
                              :calories true}
                             {:gname 1}
                             0
                             0
                             {:case-first "lower"})]
   (doseq [single-result result]
    (println single-result))
   ))

