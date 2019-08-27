(ns mongo-lib.core
  (:require [clojure.string :as cstring])
  (:import [java.lang Boolean
                      Long
                      String
                      Double]
           [java.util Date
                      ArrayList]
           [com.mongodb.client MongoClients
                               MongoClient]
           [com.mongodb.client.internal MongoDatabaseImpl
                                        MongoCollectionImpl]
           [com.mongodb.client.model Collation
                                     CollationCaseFirst
                                     IndexOptions]
           [org.bson Document
                     BsonValue
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

(defn object-id
  "Returns string id as ObjectId"
  [_id-as-string]
  (when (and _id-as-string
             (string?
               _id-as-string))
    (BsonObjectId.
      (ObjectId.
        _id-as-string))
   ))

(defn mongodb-make-connection
  "Creates connection to mongo client"
  [db-uri]
  (when (and db-uri
             (string?
               db-uri))
    (MongoClients/create
      db-uri))
 )

(defn mongodb-get-database
  "Get database from connection"
  [db-name
   & [connection]]
  (when (and db-name
             (string?
               db-name))
    (try
      (if connection
        (.getDatabase
          connection
          db-name)
        (.getDatabase
          @conn
          db-name))
      (catch Exception e
        (println
          e))
     ))
 )

(defn mongodb-drop-database
  "Drop mongo database"
  [db-name
   & [connection]]
  (if (and db-name
             (string?
               db-name))
    (try
      (let [mongo-db (mongodb-get-database
                       db-name
                       connection)]
        (if (and mongo-db
                 (= MongoDatabaseImpl
                    (type
                      mongo-db))
             )
          (do
            (.drop
              mongo-db)
            true)
          false))
      (catch Exception e
        (println
          e)
        false))
    false))

(defn mongodb-connect
  "Connect to mongo db"
  [db-uri
   db-name
   & [is-mobile]]
  (let [made-connection (if is-mobile
                          db-uri
                          (mongodb-make-connection
                            db-uri))
        fetched-database (mongodb-get-database
                           db-name
                           made-connection)]
    (if (and (not
               (nil?
                 made-connection))
             (not
               (nil?
                 fetched-database))
         )
      (do
        (reset!
          conn
          made-connection)
        (reset!
          db
          fetched-database)
        true)
      false))
 )

(defn mongodb-disconnect
  "Disconnect from mongo"
  [& [connection]]
  (try
    (if (and connection
             (instance?
               MongoClient
               connection))
      (do
        (.close
          connection)
        true)
      (if (and @conn
               (instance?
                 MongoClient
                 @conn))
        (do
          (.close
            @conn)
          true)
        false))
    (catch Exception e
      (println
        e))
   ))

(defn get-collection
  "Get reference to collection from particular database"
  [collection-name
   & [document-class
      database]]
  (try
    (let [mongo-db (if (and database
                            (= MongoDatabaseImpl
                               (type
                                 database))
                        )
                     database
                     @db)]
      (when (and mongo-db
                 (= MongoDatabaseImpl
                    (type
                      mongo-db))
                 collection-name
                 (string?
                   collection-name)
                 (not
                   (cstring/blank?
                     collection-name))
             )
        (if (nil?
              document-class)
          (.getCollection
            mongo-db
            collection-name)
          (.getCollection
            mongo-db
            collection-name
            document-class))
       ))
    (catch Exception e
      (println e))
   ))

(defn mongodb-drop-collection
  "Drops collection"
  [collection]
  (try
    (let [collection (if (and collection
                              (string?
                                collection))
                       (get-collection
                         collection)
                       collection)]
      (if (and collection
               (= MongoCollectionImpl
                  (type
                    collection))
           )
        (do
          (.drop
            collection)
          true)
        false))
    (catch Exception e
      (println e))
   ))

(defn build-document
  "Convert clojure like object into java Bson like object"
  [clj-document
   & [data-type]]
  (let [data-if-tree-fn (fn [c-value-p]
                          (if (map?
                                c-value-p)
                            (build-document
                              c-value-p)
                            (if (vector?
                                  c-value-p)
                              (build-document
                                c-value-p
                                "array")
                              (let [new-value (atom c-value-p)]
                                (when (instance?
                                        Boolean
                                        c-value-p)
                                  (reset!
                                    new-value
                                    (BsonBoolean.
                                      c-value-p))
                                 )
                                #_(when (instance?
                                          Long
                                          c-value-p)
                                  (reset!
                                    new-value
                                    (BsonInt64.
                                      c-value-p))
                                 )
                                (when (instance?
                                        String
                                        c-value-p)
                                  (reset!
                                    new-value
                                    (BsonString.
                                      c-value-p))
                                 )
                                (when (or (instance?
                                            Long
                                            c-value-p)
                                          (instance?
                                            Double
                                            c-value-p))
                                  (reset!
                                    new-value
                                    (BsonDouble.
                                      c-value-p))
                                 )
                                (when (instance?
                                        Date
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
    (when (or (instance?
                ArrayList
                data-value)
              (instance?
                BsonArray
                data-value))
      (reset!
        result-value
        [])
      (doseq [elem data-value]
        (swap!
          result-value
          conj
          (build-clojure-document
            elem))
       ))
    (when (or (instance?
                Document
                data-value)
              (instance?
                BsonDocument
                data-value))
      (reset!
        result-value
        {})
      (doseq [e-key (vec
                      (.keySet
                        data-value))]
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
                e-key))
            (build-clojure-document
              (.get
                data-value
                e-key))
           ))
       ))
    (when (not
            (or (instance?
                  ArrayList
                  data-value)
                (instance?
                  BsonArray
                  data-value)
                (instance?
                  Document
                  data-value)
                (instance?
                  BsonDocument
                  data-value))
           )
      (if (instance?
            BsonValue
            data-value)
        (reset!
          result-value
          (.getValue
            data-value))
        (reset!
          result-value
          data-value))
     )
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
  (when (and collection
             (or (string?
                   collection)
                 (= MongoCollectionImpl
                    (type
                      collection))
              ))
    (let [collection (if (and collection
                              (string?
                                collection))
                       (get-collection
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
 )

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
   _id
   & [projection-map]]
  (when (and collection
             (or (string?
                   collection)
                 (= MongoCollectionImpl
                    (type
                      collection))
              )
             _id
             (string?
               _id))
    (try
      (first
        (mongodb-find
          collection
          {:_id (BsonObjectId.
                  (ObjectId.
                    _id))}
          projection-map
          nil
          1))
      (catch Exception e
        (println
          (.getMessage
            e))
       ))
   ))

(defn mongodb-insert-one
  "Insert one record in particular collection"
  [collection
   insert-document]
  (if (and collection
           (or (string?
                 collection)
               (= MongoCollectionImpl
                  (type
                    collection))
            )
           insert-document
           (map?
             insert-document))
    (let [collection (if (string?
                           collection)
                       (get-collection
                         collection
                         BsonDocument)
                       collection)
          new-document (build-document
                         insert-document)]
      (.insertOne
        collection
        new-document)
      true)
    false))

(defn mongodb-insert-many
  "Insert many records in particular collection"
  [collection
   insert-documents-vector]
  (if (and collection
           (or (string?
                 collection)
               (= MongoCollectionImpl
                  (type
                    collection))
            )
           insert-documents-vector
           (vector?
             insert-documents-vector)
           (not
             (empty?
               insert-documents-vector))
       )
    (let [collection (if (string?
                           collection)
                       (get-collection
                         collection
                         BsonDocument)
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
        list-obj)
      true)
    false))

(defn mongodb-update-by-id
  "Update record by id in particular collection with update-document"
  [collection
   _id
   update-document]
  (when (and collection
             (or (string?
                   collection)
                 (= MongoCollectionImpl
                    (type
                      collection))
              )
             _id
             (string?
               _id)
             update-document
             (map?
               update-document))
    (let [collection (if (string?
                           collection)
                       (get-collection
                         collection)
                       collection)]
      (.updateOne
        collection
        (build-document
          {:_id (BsonObjectId.
                  (ObjectId.
                    _id))}
         )
        (build-document
          {:$set update-document}))
     ))
 )

(defn mongodb-update-one
  "Update one document that matches filter"
  [collection
   filter-map
   update-map]
  (when (and collection
             (or (string?
                   collection)
                 (= MongoCollectionImpl
                    (type
                      collection))
              )
             filter-map
             (map?
               filter-map)
             update-map
             (map?
               update-map))
    (let [collection (if (string?
                           collection)
                       (get-collection
                         collection)
                       collection)
          filter-doc (build-document
                       filter-map)
          update-map (if (or (contains?
                               update-map
                               :$set)
                             (contains?
                               update-map
                               :$addToSet))
                       update-map
                       {:$set update-map})
          update-doc (build-document
                       update-map)]
      (.updateOne
        collection
        filter-doc
        update-doc))
   ))

(defn mongodb-update-many
  "Update many documents that match filter"
  [collection
   filter-map
   update-map]
  (when (and collection
             (or (string?
                   collection)
                 (= MongoCollectionImpl
                    (type
                      collection))
              )
             filter-map
             (map?
               filter-map)
             update-map
             (map?
               update-map))
    (let [collection (if (string?
                           collection)
                       (get-collection
                         collection)
                       collection)
          filter-doc (build-document
                       filter-map)
          update-doc (build-document
                       {:$set update-map})]
      (.updateMany
        collection
        filter-doc
        update-doc))
   ))

(defn mongodb-delete-by-id
  "Delete record by _id"
  [collection
   _id]
  (when (and collection
             (or (string?
                   collection)
                 (= MongoCollectionImpl
                    (type
                      collection))
              )
             _id
             (string?
               _id))
    (let [collection (if (string?
                           collection)
                       (get-collection
                         collection)
                       collection)]
      (.deleteOne
        collection
        (build-document
          {:_id (BsonObjectId.
                  (ObjectId.
                    _id))}
         ))
     ))
 )

(defn mongodb-delete-one
  "Delete one record by filter"
  [collection
   filter-map]
  (when (and collection
             (or (string?
                   collection)
                 (= MongoCollectionImpl
                    (type
                      collection))
              )
             filter-map
             (map?
               filter-map))
    (let [collection (if (string?
                           collection)
                       (get-collection
                         collection)
                       collection)]
      (.deleteOne
        collection
        (build-document
          filter-map))
     ))
 )

(defn mongodb-delete-many
  "Delete records when filter is matched"
  [collection
   filter-map]
  (when (and collection
             (or (string?
                   collection)
                 (= MongoCollectionImpl
                    (type
                      collection))
              )
             filter-map
             (map?
               filter-map))
    (let [collection (if (string?
                           collection)
                       (get-collection
                         collection)
                       collection)]
      (.deleteMany
        collection
        (build-document
          filter-map))
     ))
 )

(defn mongodb-count
  "Count records from particular collection matching entity-filter"
  [collection
   & [entity-filter]]
  (when (and collection
             (or (string?
                   collection)
                 (= MongoCollectionImpl
                    (type
                      collection))
              )
             (or (nil?
                   entity-filter)
                 (and entity-filter
                      (map?
                        entity-filter))
              ))
    (let [collection (if (string?
                           collection)
                       (get-collection
                         collection)
                       collection)]
      (.count
        collection
        (build-document
          entity-filter))
     ))
 )

(defn mongodb-exists
  "Does any or records that match filter exists"
  [collection
   & [entity-filter]]
  (if (and collection
           (or (string?
                 collection)
               (= MongoCollectionImpl
                  (type
                    collection))
            )
           (or (nil?
                 entity-filter)
               (and entity-filter
                    (map?
                      entity-filter))
            ))
    (let [count-result (mongodb-count
                         collection
                         entity-filter)]
      (if (and count-result
               (number?
                 count-result))
        (< 0
           count-result)
        false))
    false))

(defn mongodb-create-index
  "Create mongo index on particular collection make it unique or not"
  [collection
   fields
   index-name
   & [unique
      expire-after-seconds]]
  (when (and collection
             (or (string?
                   collection)
                 (= MongoCollectionImpl
                    (type
                      collection))
              )
             fields
             (map?
               fields)
             index-name
             (string?
               index-name))
    (let [collection (if (string?
                           collection)
                       (get-collection
                         collection)
                       collection)
          index-options (-> (IndexOptions.)
                            (.unique (boolean
                                       unique))
                            (.name index-name))]
      (when (and expire-after-seconds
                 (number?
                   expire-after-seconds))
        (.expireAfter
          index-options
          expire-after-seconds
          java.util.concurrent.TimeUnit/SECONDS))
      (try
        (.createIndex
          collection
          (build-document
            fields)
          index-options)
        (catch Exception e
          (println
            (.getMessage
              e))
         ))
     ))
 )

(defn mongodb-list-indexes
  "List all mongo indexes of particular collection as clojure maps"
  [collection]
  (if (and collection
           (or (string?
                 collection)
               (= MongoCollectionImpl
                  (type
                    collection))
            ))
    (let [collection (if (string?
                           collection)
                       (get-collection
                         collection)
                       collection)]
      (if collection
        (let [indexes-list (.listIndexes
                             collection)
              indexes-vector (atom [])]
          (doseq [index indexes-list]
            (swap!
              indexes-vector
              conj
              (build-clojure-document
                index))
           )
          @indexes-vector)
        []))
    []))

(defn find-index
  "Helper function for index exists function"
  [indexes-vector
   index-name
   i]
  (when (and indexes-vector
             (vector?
               indexes-vector)
             index-name
             (string?
               index-name)
             i
             (number?
               i))
    (when (< i
             (count
               indexes-vector))
      (let [index (get
                    indexes-vector
                    i)
            index-name-db (:name index)]
        (if (= index-name-db
               index-name)
          index
          (recur
            indexes-vector
            index-name
            (inc
              i))
         ))
     ))
 )

(defn mongodb-get-index
  "Check if mongo index exists"
  [collection
   index-name]
  (when (and collection
             (or (string?
                   collection)
                 (= MongoCollectionImpl
                    (type
                      collection))
              )
             index-name
             (string?
               index-name))
    (let [indexes-vector (mongodb-list-indexes
                           collection)
          index-doc (find-index
                      indexes-vector
                      index-name
                      0)]
      index-doc))
 )

(defn mongodb-index-exists?
  "Check if mongo index exists"
  [collection
   index-name]
  (if (and collection
           (or (string?
                 collection)
               (= MongoCollectionImpl
                  (type
                    collection))
            )
           index-name
           (string?
             index-name))
    (let [indexes-vector (mongodb-list-indexes
                           collection)
          index-doc (find-index
                      indexes-vector
                      index-name
                      0)]
      (boolean
        index-doc))
    false))

(defn mongodb-drop-index
  "Drop mongo index on particular collection"
  [collection
   index-name]
  (if (and collection
           (or (string?
                 collection)
               (= MongoCollectionImpl
                  (type
                    collection))
            )
           index-name
           (string?
             index-name))
    (let [collection (if (string?
                           collection)
                       (get-collection
                         collection)
                       collection)]
      (try
        (.dropIndex
          collection
          index-name)
        true
        (catch Exception e
          (println
            (.getMessage
              e))
          false))
     )
    false))

