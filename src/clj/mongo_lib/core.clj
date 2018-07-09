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
                                     CollationCaseFirst
                                     IndexOptions]
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

(def conn
    (atom nil))

(def db
     (atom nil))

(defn mongodb-connect
  "Connect to mongo db"
  [db-name]
  (let [admin-db "admin"
        username "admin"
        password (char-array
                   "passw0rd")
        cred (MongoCredential/createCredential
               username
               admin-db
               password)
        host "127.0.0.1"
        port 27017]
    (swap!
      conn
      (fn [conn-input]
        (MongoClient.
          (ServerAddress.
            host
            port)
          cred
          (.build
            (MongoClientOptions/builder))
         ))
     )
    (swap!
      db
      (fn [db-input]
        (.getDatabase
          @conn
          db-name))
     ))
 )

(defn mongodb-disconnect
  "Disconnect from mongo"
  []
  (.close
    @conn))

(defn get-collection
  "Get reference to collection from particular database"
  [db
   collection-name
   & [document-class]]
  (if (nil? document-class)
    (.getCollection
      @db
      collection-name)
    (.getCollection
      @db
      collection-name
      document-class))
 )

(defn build-document
  "Convert clojure like object into java Bson like object"
  [clj-document
   & [data-type]]
  (let [data-if-tree-fn (fn [c-value-p]
                          (if (map? c-value-p)
                            (build-document
                              c-value-p)
                            (if (vector? c-value-p)
                              (build-document
                                c-value-p
                                "array")
                              (let [new-value (atom c-value-p)]
                                (when (instance? Boolean
                                                 c-value-p)
                                  (reset!
                                    new-value
                                    (BsonBoolean.
                                      c-value-p))
                                 )
                                (when (instance? Long
                                                 c-value-p)
                                  (reset!
                                    new-value
                                    (BsonInt64.
                                      c-value-p))
                                 )
                                (when (instance? String
                                                 c-value-p)
                                  (reset!
                                    new-value
                                    (BsonString.
                                      c-value-p))
                                 )
                                (when (instance? Double
                                                 c-value-p)
                                  (reset!
                                    new-value
                                    (BsonDouble.
                                      c-value-p))
                                 )
                                (when (instance? Date
                                                 c-value-p)
                                  (reset!
                                    new-value
                                    (BsonDateTime.
                                      (.getTime
                                        c-value-p))
                                   ))
                                @new-value))
                           ))]
    (if (= data-type
           "array")
      (let [result (BsonArray.)]
        (reduce
          (fn [acc c-value]
            (.add
              result
              (data-if-tree-fn
                c-value))
            result)
          result
          clj-document))
      (let [result (BsonDocument.)]
        (reduce
          (fn [acc [c-key
                    c-value]]
            (.append
              result
              (name
                c-key)
              (data-if-tree-fn
                c-value))
           )
          result
          clj-document))
     ))
 )

(defn build-collation
  "Build collation java object aou of clojure map"
  [collation-map]
  (let [collation-builder (Collation/builder)]
    (if (contains? collation-map
                   :locale)
      (.locale
        collation-builder
        (:locale collation-map))
      (.locale
        collation-builder
        "en_US"))
    (when (contains? collation-map
                     :case-first)
      (.collationCaseFirst
        collation-builder
        (case (clojure.string/lower-case
                (:case-first collation-map))
          "upper" (CollationCaseFirst/UPPER)
          "lower" (CollationCaseFirst/LOWER)
          (CollationCaseFirst/OFF))
       ))
    (.build collation-builder))
 )

(defn build-clojure-document
  "Convert java Bson like object into clojure map or vector"
  [data-value]
  (let [result-value (atom nil)]
    (when (instance? ArrayList
                     data-value)
      (reset!
        result-value
        [])
      (doseq [elem data-value]
        (swap!
          result-value
          conj
          (build-clojure-document elem))
       ))
    (when (instance? Document
                     data-value)
      (reset!
        result-value
        {})
      (doseq [e-key (vec (.keySet data-value))]
        (swap!
          result-value
          assoc
          (keyword
            e-key)
          (if (= e-key
                 "_id")
           (str
             (.get
               data-value
               (str
                 e-key))
            )
           (build-clojure-document
             (.get
               data-value
               (str
                 e-key))
            ))
         ))
     )
    (when (not
            (or (instance? ArrayList
                           data-value)
                (instance? Document
                           data-value))
           )
      (reset!
        result-value
        data-value))
    @result-value))

(defn mongodb-find
  "Find records that match filter-map
   return only fields mentined in projection-map
   sort by fields from sort-map ascending 1 or descending -1
   limit final count of results
   skip number of records from start and return rest
   collation language key codes"
  [collection
   & [filter-map
      projection-map
      sort-map
      limit
      skip
      collation]]
  (let [collection (if (string? collection)
                     (get-collection
                       db
                       collection)
                     collection)
        filter-doc (build-document
                     filter-map)
        projection-doc (build-document
                         projection-map)
        sort-doc (build-document
                   sort-map)
        limit (or limit
                  0)
        skip  (or skip
                  0)
        collation-obj (build-collation
                        collation)
        itr-result (.iterator
                     (-> (.find
                           collection
                           filter-doc)
                         (.projection
                           projection-doc)
                         (.sort
                           sort-doc)
                         (.limit
                           limit)
                         (.skip
                           skip)
                         (.collation
                           collation-obj))
                    )
        result-all (atom [])]
    (while (.hasNext
             itr-result)
      (swap!
        result-all
        conj
        (build-clojure-document
          (.next
            itr-result))
       ))
    @result-all))

(defn mongodb-find-one
  "Find one record that matches filter-map
   with only fields mentioned in projection-map"
  [collection
   & [filter-map
      projection-map]]
  (first
    (mongodb-find
      collection
      filter-map
      projection-map
      nil
      1))
 )

(defn mongodb-find-by-id
  "Find record by id"
  [collection
   _id]
  (first
    (mongodb-find
      collection
      {:_id (BsonObjectId.
              (ObjectId. _id))}
      nil
      nil
      1))
 )

(defn mongodb-insert-one
  "Insert one record in particular collection"
  [collection
   insert-document]
  (let [collection (if (string? collection)
                     (get-collection
                       db
                       collection
                       BsonDocument)
                     collection)]
    (.insertOne
      collection
      (build-document
        insert-document))
   ))

(defn mongodb-insert-many
  "Insert many records in particular collection"
  [collection
   insert-documents-vector]
  (let [collection (if (string? collection)
                     (get-collection
                       db
                       collection)
                     collection)
        list-obj (java.util.ArrayList.)]
   (doseq [insert-document insert-documents-vector]
     (.add
       list-obj
       (build-document
         insert-document))
    )
   (.insertMany
     collection
     list-obj))
 )

(defn mongodb-update-by-id
  "Update record by id in particular collection with update-document"
  [collection
   _id
   update-document]
  (let [collection (if (string? collection)
                     (get-collection
                       db
                       collection)
                     collection)]
    (.updateOne
      collection
      (build-document
        {:_id (BsonObjectId.
                (ObjectId. _id))}
       )
      (build-document
        {:$set update-document}))
   ))

(defn mongodb-delete-by-id
  "Delete record by _id"
  [collection
   _id]
  (let [collection (if (string? collection)
                     (get-collection
                       db
                       collection)
                     collection)]
    (.deleteOne
      collection
      (build-document
        {:_id (BsonObjectId.
                (ObjectId. _id))}
       ))
   ))

(defn mongodb-delete-by-filter
  ""
  [collection
   filter-map]
  (let [collection (if (string? collection)
                     (get-collection
                       db
                       collection)
                     collection)]
    (.deleteMany
      collection
      (build-document
        filter-map))
   ))

(defn mongodb-count
  "Count records from particular collection matching entity-filter"
  [collection
   & [entity-filter]]
  (let [collection (if (string? collection)
                     (get-collection
                       db
                       collection)
                     collection)]
    (.count
      collection
      (build-document
        entity-filter))
   ))

(defn mongodb-exists
  ""
  [collection
   & [entity-filter]]
  (let [count-result (mongodb-count
                       collection
                       entity-filter)]
    (> count-result
       0))
 )

(defn mongodb-create-index
  "Create mongo index on particular collection make it unique or not"
  [collection
   fields
   index-name
   unique
   & [expire-after-seconds]]
  (let [collection (if (string? collection)
                     (get-collection
                       db
                       collection)
                     collection)
        index-options (-> (IndexOptions.)
                          (.unique unique)
                          (.name index-name))]
    (when expire-after-seconds
      (.expireAfter
        index-options
        expire-after-seconds
        java.util.concurrent.TimeUnit/SECONDS))
    (.createIndex
      collection
      (build-document
        fields)
      index-options))
 )

(defn mongodb-list-indexes
  "List all mongo indexes of particular collection as clojure maps"
  [collection]
  (let [collection (if (string? collection)
                     (get-collection
                       db
                       collection)
                     collection)
        indexes-list (.listIndexes
                       collection)
        indexes-vector (atom [])]
    (doseq [index indexes-list]
      (swap!
        indexes-vector
        conj
        (build-clojure-document
          index))
      )
    @indexes-vector))

(defn- find-index
  "Helper function for index exists function"
  [indexes-list
   index-name
   i]
  (when (< i
           (count indexes-list))
    (let [index (get indexes-list i)
          index-name-db (:name index)]
      (if (= index-name-db
             index-name)
        index
        (recur
          indexes-list
          index-name
          (inc i))
       ))
   ))

(defn mongodb-index-exists?
  "Check if mongo index exists"
  [collection
   index-name]
  (let [indexes-list (mongodb-list-indexes
                       collection)]
    (find-index
      indexes-list
      index-name
      0))
 )

(defn mongodb-drop-index
  "Drop mongo index on particular collection"
  [collection
   index-name]
  (let [collection (if (string? collection)
                     (get-collection
                       db
                       collection)
                     collection)]
    (.dropIndex
      collection
      index-name))
 )

(defn pretty-print
  "Test function"
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

