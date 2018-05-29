(ns mongo-lib.core
  (:import [java.lang Boolean
                      Long
                      String
                      Double]
           [java.util Date
                      ArrayList]
           [com.mongodb MongoClient
                        MongoCredential
                        MongoClientOptions
                        ServerAddress]
           [com.mongodb.client.model Collation
                                     CollationCaseFirst]
           [org.bson Document
                     BsonDocument
                     BsonArray
                     BsonObjectId
                     BsonString
                     BsonBoolean
                     BsonInt64
                     BsonDouble
                     BsonDateTime]
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
  collection-name
  & [document-class]]
 (if (nil? document-class)
  (.getCollection @db
                  collection-name)
  (.getCollection @db
                  collection-name
                  document-class))
 )

(defn build-document
 ""
 [clj-document
  & [data-type]]
 (let [data-if-tree-fn (fn [c-value-p]
                        (if (map? c-value-p)
                          (build-document c-value-p)
                          (if (vector? c-value-p)
                           (build-document c-value-p "array")
                           (let [new-value (atom c-value-p)]
                               (when (instance? Boolean
                                                c-value-p)
                                (reset! new-value (BsonBoolean. c-value-p))
                                )
                               (when (instance? Long
                                                c-value-p)
                                (reset! new-value (BsonInt64. c-value-p))
                                )
                               (when (instance? String
                                                c-value-p)
                                (reset! new-value (BsonString. c-value-p))
                                )
                               (when (instance? Double
                                                c-value-p)
                                (reset! new-value (BsonDouble. c-value-p))
                                )
                               (when (instance? Date
                                                c-value-p)
                                (reset! new-value (BsonDateTime. (.getTime c-value-p)))
                                )
                               @new-value))
                          ))]
 (if (= data-type
        "array")
  (let [result  (BsonArray.)]
   (reduce (fn [acc c-value]
            (.add result (data-if-tree-fn c-value))
            result)
           result
           clj-document))
  (let [result  (BsonDocument.)]
   (reduce (fn [acc [c-key
                     c-value]]
            (.append result (name c-key)
                            (data-if-tree-fn c-value))
            )
           result
           clj-document))
  ))
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

(defn build-clojure-document
 ""
 [data-value]
 (let [result-value (atom nil)]
  (when (instance? ArrayList
                   data-value)
   (reset! result-value [])
   (doseq [elem data-value]
    (swap!
      result-value
      conj
      (build-clojure-document elem))
    ))
  (when (instance? Document
                   data-value)
   (reset! result-value {})
   (doseq [e-key (vec (.keySet data-value))]
    (swap!
      result-value
      assoc
      (keyword e-key)
      (if (= e-key "_id")
       (str (.get data-value (str e-key))
        )
       (build-clojure-document (.get data-value (str e-key))
        ))
     ))
   )
  (when (not
         (or (instance? ArrayList
                        data-value)
             (instance? Document
                        data-value))
         )
   (reset! result-value data-value))
  @result-value))

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
        result-all  (atom [])]
   (while (.hasNext itr-result)
    (swap!
      result-all
      conj
      (build-clojure-document (.next itr-result))
     ))
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
                       {:_id (BsonObjectId. (ObjectId. _id))}
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
                                     collection
                                     BsonDocument)
                     collection)]
   (.insertOne collection (build-document insert-document))
   ))

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
   (.updateOne collection (build-document {:_id (BsonObjectId. (ObjectId. _id))})
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
   (.deleteOne collection (build-document {:_id (BsonObjectId. (ObjectId. _id))}))
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

