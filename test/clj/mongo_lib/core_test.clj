(ns mongo-lib.core-test
  (:require [clojure.test :refer :all]
            [mongo-lib.core :refer :all]))

(def db-uri
     (or (System/getenv "MONGODB_URI")
         (System/getenv "PROD_MONGODB")
         "mongodb://admin:passw0rd@127.0.0.1:27017/admin"))

(def db-name
     "test-db")

(defn create-db
  "Create database for testing"
  []
  (mongodb-connect
    db-uri
    db-name)
  (mongodb-insert-many
    "test-collection"
    [{:test-attribute "test-value1"
      :exclude-from-projection "exclude-from-projection-value"
      :test-same-attribute "test-same-value"
      :test-index-attribute 1
      :test-ttl-attribute (java.util.Date.)}
     {:test-attribute "test-value2"
      :test-same-attribute "test-same-value"
      :test-index-attribute 2
      :test-ttl-attribute (java.util.Date.)}
     {:test-attribute "test-value3"
      :test-index-attribute 3
      :test-ttl-attribute (java.util.Date.)}]))

(defn destroy-db
  "Destroy testing database"
  []
  (mongodb-drop-collection
    "test-collection")
  (mongodb-disconnect))

(defn before-and-after-tests
  "Before and after tests"
  [f]
  (create-db)
  (f)
  (destroy-db))

(use-fixtures :each before-and-after-tests)

(deftest test-mongodb-make-connection
  
  (testing "Test mongodb make connection"
    
    (let [mongodb-connection (mongodb-make-connection
                               nil)]
      
      (is
        (nil?
          mongodb-connection)
       )
      
     )
    
    (let [mongodb-connection (mongodb-make-connection
                               db-uri)]
      
      (is
        (not
          (nil?
            mongodb-connection))
       )
      
      (is
        (instance?
          com.mongodb.MongoClient
          mongodb-connection)
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-get-database
  
  (testing "Test mongodb get database"
    
    (let [mongo-database (mongodb-get-database
                           nil)]
      
      (is
        (nil?
          mongo-database)
       )
      
     )
    
    (let [mongo-database (mongodb-get-database
                           "test-db")]
      
      (is
        (not
          (nil?
            mongo-database))
       )
      
      (is
        (instance?
          com.mongodb.MongoDatabaseImpl
          mongo-database)
       )
      
     )
    
    (let [mongodb-connection (mongodb-make-connection
                               db-uri)
          mongo-database (mongodb-get-database
                           "test-db"
                           mongodb-connection)]
      
      (is
        (not
          (nil?
            mongo-database))
       )
      
      (is
        (instance?
          com.mongodb.MongoDatabaseImpl
          mongo-database)
       )
      
     )
    
   )
  
 )

(deftest test-mongo-drop-database
  
  (testing "Test mongo drop database"
    
    (let [result (mongo-drop-database
                   nil)]
      
      (is
        (false?
          result)
       )
      
     )
    
    (let [result (mongo-drop-database
                   "")]
      
      (is
        (false?
          result)
       )
      
     )
    
    (let [result (mongo-drop-database
                   "test-db")]
      
      (is
        (true?
          result)
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-connect
  
  (testing "Test mongodb connect"
    
    (let [mongo-db (mongodb-connect
                     nil
                     nil)]
      
      (is
        (false?
          mongo-db)
       )
      
     )
    
    (let [mongo-db (mongodb-connect
                     db-uri
                     nil)]
      
      (is
        (false?
          mongo-db)
       )
      
     )
    
    (let [mongo-db (mongodb-connect
                     nil
                     db-name)]
      
      (is
        (false?
          mongo-db)
       )
      
     )
    
    (let [mongo-db (mongodb-connect
                     db-uri
                     db-name)]
      
      (is
        (true?
          mongo-db)
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-disconnect
  
  (testing "Test mongodb disconnect"
    
    (let [back-up-conn @conn
          void (reset!
                 conn
                 nil)
          disconnected (mongodb-disconnect)]
      
      (is
        (false?
          disconnected)
       )
      
      (reset!
        conn
        back-up-conn)
      
     )
    
    (let [mongodb-connection (mongodb-make-connection
                               db-uri)
          disconnected (mongodb-disconnect
                         mongodb-connection)]
      
      (is
        disconnected
       )
      
     )
    
   )
  
 )

(deftest test-get-collection
  
  (testing "Test get collection"
    
    (let [mongo-collection (get-collection
                             nil)]
      
      (is
        (nil?
          mongo-collection)
       )
      
     )
    
    (let [back-up-db @db
          void (reset!
                 db
                 nil)
          mongo-collection (get-collection
                             nil)]
      
      (is
        (nil?
          mongo-collection)
       )
      
      (reset!
        db
        back-up-db)
      
     )
    
    (let [back-up-db @db
          void (reset!
                 db
                 nil)
          mongo-collection (get-collection
                             "test-collection")]
      
      (is
        (nil?
          mongo-collection)
       )
      
      (reset!
        db
        back-up-db)
      
     )
    
    (let [mongo-database (mongodb-get-database
                           "test-db")
          mongo-collection (get-collection
                             "test-collection"
                             nil
                             mongo-database)]
      
      (is
        (not
          (nil?
            mongo-collection))
       )
      
      (is
        (instance?
          com.mongodb.MongoCollectionImpl
          mongo-collection)
       )
      
      (is
        (not
          (nil?
            (.getNamespace
              mongo-collection))
         )
       )
      
      (let [mongo-namespace (.getNamespace
                              mongo-collection)]
        
        (is
          (= (.getFullName
               mongo-namespace)
             "test-db.test-collection")
         )
        
        (is
          (= (.getDatabaseName
               mongo-namespace)
             "test-db")
         )
        
        (is
          (= (.getCollectionName
               mongo-namespace)
             "test-collection")
         )
        
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-drop-collection
  
  (testing "Test mongodb drop collection"
    
    (let [result (mongodb-drop-collection
                   nil)]
      
      (is
        (not
          result)
       )
      
     )
    
    (let [result (mongodb-drop-collection
                   "test-collection")]
      
      (is
        result
       )
      
     )
    
   )
  
 )

(deftest test-build-document
  
  (testing "Test build document"
    
    (let [result (build-document
                   nil)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (instance?
          org.bson.BsonDocument
          result)
       )
      
     )
    
    (let [result (build-document
                   nil
                   "array")]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (instance?
          org.bson.BsonArray
          result)
       )
      
     )
    
    (let [result (build-document
                   {:test-attribute "test-value"})]
      
      (is
        (= "test-value"
           (.getValue
             (.get
               result
               "test-attribute"))
         )
       )
      
     )
    
    (let [result (build-document
                   {:test-attribute 1})]
      
      (is
        (= 1.0
           (.getValue
             (.get
               result
               "test-attribute"))
         )
       )
      
     )
    
    (let [result (build-document
                   {:test-attribute true})]
      
      (is
        (= true
           (.getValue
             (.get
               result
               "test-attribute"))
         )
       )
      
     )
    
    (let [result (build-document
                   {:test-attribute (java.util.Date.)})]
      
      (is
        (< (- (.getTime
                (java.util.Date.))
              (.getValue
                (.get
                  result
                  "test-attribute"))
            )
           (* 1000
              60))
       )
      
     )
    
    (let [result (build-document
                   [{:test-attribute "test-value1"}
                    {:test-attribute "test-value2"}]
                   "array")
          result1 (.get
                    result
                    0)
          result2 (.get
                    result
                    1)]
      
      (is
        (= "test-value1"
           (.getValue
             (.get
               result1
               "test-attribute"))
         )
       )
      
      (is
        (= "test-value2"
           (.getValue
             (.get
               result2
               "test-attribute"))
         )
       )
      
     )
    
   )
  
 )

(deftest test-build-collation
  
  (testing "Test build collation"
    
    (let [result (build-collation
                   nil)]
      
      (is
        (= (.getLocale
             result)
           "en_US")
       )
      
     )
    
    (let [result (build-collation
                   {:locale "sr"})]
      
      (is
        (= (.getLocale
             result)
           "sr")
       )
      
     )
    
   )
  
 )

(deftest test-build-clojure-document
  
  (testing "Test build clojure document"
    
    (let [result (build-clojure-document
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [bson-obj (org.bson.Document.)
          result (build-clojure-document
                   bson-obj)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (map?
          result)
       )
      
      (is
        (empty?
          result)
       )
      
     )
    
    (let [bson-obj (org.bson.BsonDocument.)
          result (build-clojure-document
                   bson-obj)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (map?
          result)
       )
      
      (is
        (empty?
          result)
       )
      
     )
    
    (let [bson-obj (java.util.ArrayList.)
          result (build-clojure-document
                   bson-obj)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (vector?
          result)
       )
      
      (is
        (empty?
          result)
       )
      
     )
    
    (let [bson-obj (org.bson.BsonArray.)
          result (build-clojure-document
                   bson-obj)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (vector?
          result)
       )
      
      (is
        (empty?
          result)
       )
      
     )
    
    ;; Document test
    
    (let [bson-obj (org.bson.Document.
                     "test-attribute"
                     "test-value")
          result (build-clojure-document
                   bson-obj)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (map?
          result)
       )
      
      (is
        (not
          (empty?
            result))
       )
      
      (is
        (= (:test-attribute result)
           "test-value")
       )
      
     )
    
    (let [bson-obj (org.bson.Document.
                     "test-attribute"
                     1)
          result (build-clojure-document
                   bson-obj)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (map?
          result)
       )
      
      (is
        (not
          (empty?
            result))
       )
      
      (is
        (= (:test-attribute result)
           1)
       )
      
     )
    
    (let [bson-obj (org.bson.Document.
                     "test-attribute"
                     true)
          result (build-clojure-document
                   bson-obj)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (map?
          result)
       )
      
      (is
        (not
          (empty?
            result))
       )
      
      (is
        (= (:test-attribute result)
           true)
       )
      
     )
    
    (let [bson-obj (org.bson.Document.
                     "test-attribute"
                     (java.util.Date.))
          result (build-clojure-document
                   bson-obj)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (map?
          result)
       )
      
      (is
        (not
          (empty?
            result))
       )
      
      (is
        (< (- (.getTime
                (java.util.Date.))
              (.getTime
                (:test-attribute result))
            )
           (* 1000
              60))
       )
      
     )
    
    ;; BsonDocument test
    
    (let [bson-obj (org.bson.BsonDocument.
                     "test-attribute"
                     (org.bson.BsonString.
                       "test-value"))
          result (build-clojure-document
                   bson-obj)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (map?
          result)
       )
      
      (is
        (not
          (empty?
            result))
       )
      
      (is
        (= (:test-attribute result)
           "test-value")
       )
      
     )
    
    (let [bson-obj (org.bson.BsonDocument.
                     "test-attribute"
                     (org.bson.BsonDouble.
                       1))
          result (build-clojure-document
                   bson-obj)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (map?
          result)
       )
      
      (is
        (not
          (empty?
            result))
       )
      
      (is
        (= (:test-attribute result)
           1.0)
       )
      
     )
    
    (let [bson-obj (org.bson.BsonDocument.
                     "test-attribute"
                     (org.bson.BsonBoolean.
                       true))
          result (build-clojure-document
                   bson-obj)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (map?
          result)
       )
      
      (is
        (not
          (empty?
            result))
       )
      
      (is
        (= (:test-attribute result)
           true)
       )
      
     )
    
    (let [bson-obj (org.bson.BsonDocument.
                     "test-attribute"
                     (org.bson.BsonDateTime.
                       (.getTime
                         (java.util.Date.))
                      ))
          result (build-clojure-document
                   bson-obj)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (map?
          result)
       )
      
      (is
        (not
          (empty?
            result))
       )
      
      (is
        (< (- (.getTime
                (java.util.Date.))
              (:test-attribute result))
           (* 1000
              60))
       )
      
     )
    
    ;; ArrayList test
    
    (let [array-list (java.util.ArrayList.)
          doc-obj1 (org.bson.Document.
                     "test-attribute"
                     "test-value1")
          doc-obj2 (org.bson.Document.
                     "test-attribute"
                     "test-value2")
          void (.add
                 array-list
                 doc-obj1)
          void (.add
                 array-list
                 doc-obj2)
          result (build-clojure-document
                   array-list)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (vector?
          result)
       )
      
      (is
        (not
          (empty?
            result))
       )
      
      (is
        (= 2
           (count
             result))
       )
      
      (is
        (= (:test-attribute
             (first
               result))
           "test-value1")
       )
      
      (is
        (= (:test-attribute
             (get
               result
               1))
           "test-value2")
       )
      
     )
    
    ;; BsonArray test
    
    (let [bson-array (org.bson.BsonArray.)
          bson-obj1 (org.bson.BsonDocument.
                      "test-attribute"
                      (org.bson.BsonString.
                        "test-value1"))
          bson-obj2 (org.bson.BsonDocument.
                      "test-attribute"
                      (org.bson.BsonString.
                        "test-value2"))
          void (.add
                 bson-array
                 bson-obj1)
          void (.add
                 bson-array
                 bson-obj2)
          result (build-clojure-document
                   bson-array)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (vector?
          result)
       )
      
      (is
        (not
          (empty?
            result))
       )
      
      (is
        (= 2
           (count
             result))
       )
      
      (is
        (= (:test-attribute
             (first
               result))
           "test-value1")
       )
      
      (is
        (= (:test-attribute
             (get
               result
               1))
           "test-value2")
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-find
  
  (testing "Test mongodb find"
    
    (let [result (mongodb-find
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-find
                   "test-collection")]
      
      (is
        (vector?
          result)
       )
      
     )
    
    (let [test-database (mongodb-get-database
                          "test-db")
          collection (get-collection
                       "test-collection"
                       nil
                       test-database)
          result (mongodb-find
                   collection)]
      
      (is
        (vector?
          result)
       )
      
     )
    
    (let [test-database (mongodb-get-database
                          "test-db")
          collection (get-collection
                       "test-collection"
                       nil
                       test-database)
          result (mongodb-find
                   collection)]
      
      (is
        (vector?
          result)
       )
      
     )
    
    (let [result (mongodb-find
                   "test-collection"
                   {:test-attribute "test-value1"})]
      
      (is
        (vector?
          result)
       )
      
      (is
        (= (count
             result)
           1)
       )
      
     )
    
    (let [result (mongodb-find
                   "test-collection"
                   {:test-attribute "test-value1"}
                   {:exclude-from-projection false})]
      
      (is
        (not
          (contains?
            result
            :exclude-from-projection))
       )
      
     )
    
    (let [result (mongodb-find
                   "test-collection"
                   nil
                   nil
                   {:test-attribute 1})
          first-element (first
                          result)
          last-element (last
                         result)]
      
      (is
        (= "test-value1"
           (:test-attribute first-element))
       )
      
      (is
        (= "test-value3"
           (:test-attribute last-element))
       )
      
     )
    
    (let [result (mongodb-find
                   "test-collection"
                   nil
                   nil
                   {:test-attribute -1})
          first-element (first
                          result)
          last-element (last
                         result)]
      
      (is
        (= "test-value3"
           (:test-attribute first-element))
       )
      
      (is
        (= "test-value1"
           (:test-attribute last-element))
       )
      
     )
    
    (let [result (mongodb-find
                   "test-collection"
                   nil
                   nil
                   nil
                   1)
          first-element (first
                          result)]
      
      (is
        (= "test-value1"
           (:test-attribute first-element))
       )
      
     )
    
    (let [result (mongodb-find
                   "test-collection"
                   nil
                   nil
                   nil
                   1
                   1)
          first-element (first
                          result)]
      
      (is
        (= "test-value2"
           (:test-attribute first-element))
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-find-one
  
  (testing "Test mongodb find one"
    
    (let [result (mongodb-find-one
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-find-one
                   "test-collection")]
      
      (is
        (= (:test-attribute result)
           "test-value1")
       )
      
     )
    
    (let [result (mongodb-find-one
                   "test-collection"
                   {:test-attribute "test-value2"})]
      
      (is
        (= (:test-attribute result)
           "test-value2")
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-find-by-id
  
  (testing "Test mongodb find by id"
    
    (let [result (mongodb-find-by-id
                   nil
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-find-by-id
                   "test-collection"
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-find-by-id
                   "test-collection"
                   "")]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [mongo-obj (mongodb-find-one
                      "test-collection")
          result (mongodb-find-by-id
                   "test-collection"
                   (:_id mongo-obj))]
      
      (is
        (not
          (nil?
            result)
         )
       )
      
     )
    
    (let [mongo-obj (mongodb-find-one
                      "test-collection")
          result (mongodb-find-by-id
                   "test-collection"
                   (:_id mongo-obj)
                   {:exclude-from-projection false})]
      
      (is
        (not
          (nil?
            result)
         )
       )
      
      (is
        (not
          (contains?
            result
            :exclude-from-projection))
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-insert-one
  
  (testing "Test mongodb insert one"
    
    (let [result (mongodb-insert-one
                   nil
                   nil)]
      
      (is
        (false?
          result)
        )
      
     )
    
    (let [result (mongodb-insert-one
                   "test-collection"
                   nil)]
      
      (is
        (false?
          result)
        )
      
     )
    
    (let [result (mongodb-insert-one
                   nil
                   {:test-attribute "inserted-obj-value"})]
      
      (is
        (false?
          result)
        )
      
     )
    
    (let [result (mongodb-insert-one
                   "test-collection"
                   ["test-vector-value"])]
      
      (is
        (false?
          result)
        )
      
     )
    
    (let [result (mongodb-insert-one
                   "test-collection"
                   {:test-attribute "inserted-obj-value"})]
      
      (is
        (true?
          result)
       )
      
      (let [inserted-obj (mongodb-find-one
                           "test-collection"
                           {:test-attribute "inserted-obj-value"})]
        
        (is
          (not
            (nil?
              inserted-obj))
         )
        
        (is
          (= (:test-attribute inserted-obj)
             "inserted-obj-value")
         )
        
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-insert-many
  
  (testing "Test mongodb insert many"
    
    (let [result (mongodb-insert-many
                   nil
                   nil)]
      
      (is
        (false?
          result)
       )
      
     )
    
    (let [result (mongodb-insert-many
                   "test-collection"
                   nil)]
      
      (is
        (false?
          result)
       )
      
     )
    
    (let [result (mongodb-insert-many
                   nil
                   [])]
      
      (is
        (false?
          result)
       )
      
     )
    
    (let [result (mongodb-insert-many
                   "test-collection"
                   [])]
      
      (is
        (false?
          result)
       )
      
     )
    
    (let [result (mongodb-insert-many
                   "test-collection"
                   [{:test-attribute "insert-many-value1"}
                    {:test-attribute "insert-many-value2"}])]
      
      (is
        (true?
          result)
       )
      
      (let [inserted-obj-1 (mongodb-find-one
                             "test-collection"
                             {:test-attribute "insert-many-value1"})
            inserted-obj-2 (mongodb-find-one
                             "test-collection"
                             {:test-attribute "insert-many-value2"})]
        
        (is
          (= (:test-attribute inserted-obj-1)
             "insert-many-value1")
         )
        
        (is
          (= (:test-attribute inserted-obj-2)
             "insert-many-value2")
         )
        
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-update-by-id
  
  (testing "Test mongodb update by id"
    
    (let [result (mongodb-update-by-id
                   nil
                   nil
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-update-by-id
                   "test-collection"
                   nil
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-update-by-id
                   nil
                   ""
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-update-by-id
                   nil
                   nil
                   {:test-attribute-update "test-value-updated"})]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [obj-to-update (mongodb-find-one
                          "test-collection"
                          {:test-attribute "test-value1"})
          result (mongodb-update-by-id
                   "test-collection"
                   (:_id obj-to-update)
                   {:test-attribute-update "test-value-updated"})]
      
      (is
        (= (.getMatchedCount
             result)
           1)
       )
      
      (is
        (= (.getModifiedCount
             result)
           1)
       )
      
      (let [updated-obj (mongodb-find-by-id
                          "test-collection"
                          (:_id obj-to-update))]
        
        (is
          (= (:test-attribute-update updated-obj)
             "test-value-updated")
         )
        
        (is
          (= (:test-attribute updated-obj)
             "test-value1")
         )
        
       )
      
     )
    
    (let [obj-to-update (mongodb-find-one
                          "test-collection"
                          {:test-attribute "test-value1"})
          test-collection (get-collection
                            "test-collection")
          result (mongodb-update-by-id
                   test-collection
                   (:_id obj-to-update)
                   {:test-attribute-update "test-value-updated-2"})]
      
      (is
        (= (.getMatchedCount
             result)
           1)
       )
      
      (is
        (= (.getModifiedCount
             result)
           1)
       )
      
      (let [updated-obj (mongodb-find-by-id
                          "test-collection"
                          (:_id obj-to-update))]
        
        (is
          (= (:test-attribute-update updated-obj)
             "test-value-updated-2")
         )
        
        (is
          (= (:test-attribute updated-obj)
             "test-value1")
         )
        
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-update-one
  
  (testing "Test mongodb update one"
    
    (let [result (mongodb-update-one
                   nil
                   nil
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-update-one
                   "test-collection"
                   nil
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-update-one
                   nil
                   ""
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-update-one
                   nil
                   nil
                   {:test-attribute-update "test-value-updated"})]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-update-one
                   "test-collection"
                   {}
                   {:test-attribute-update "test-value-updated"})]
      
      (is
        (= (.getMatchedCount
             result)
           1)
       )
      
      (is
        (= (.getModifiedCount
             result)
           1)
       )
      
      (let [updated-obj (mongodb-find-one
                          "test-collection"
                          {:test-attribute-update "test-value-updated"})]
        
        (is
          (not
            (nil?
              updated-obj))
         )
        
       )
      
     )
    
    (let [test-collection (get-collection
                            "test-collection")
          result (mongodb-update-one
                   test-collection
                   {:test-attribute "test-value1"}
                   {:test-attribute-update "test-value-updated-2"})]
      
      (is
        (= (.getMatchedCount
             result)
           1)
       )
      
      (is
        (= (.getModifiedCount
             result)
           1)
       )
      
      (let [updated-obj (mongodb-find-one
                          test-collection
                          {:test-attribute-update "test-value-updated-2"})]
        
        (is
          (not
            (nil?
              updated-obj))
         )
        
        (is
          (map?
            updated-obj)
         )
        
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-update-many
  
  (testing "Test mongodb update many"
    
    (let [result (mongodb-update-many
                   nil
                   nil
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-update-many
                   "test-collection"
                   nil
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-update-many
                   nil
                   ""
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-update-many
                   nil
                   nil
                   {:test-attribute-update "test-value-updated"})]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-update-many
                   "test-collection"
                   {}
                   {:test-attribute-update "test-value-updated"})]
      
      (is
        (= (.getMatchedCount
             result)
           3)
       )
      
      (is
        (= (.getModifiedCount
             result)
           3)
       )
      
      (let [updated-obj (mongodb-find
                          "test-collection"
                          {:test-attribute-update "test-value-updated"})]
        
        (is
          (not
            (nil?
              updated-obj))
         )
        
        (is
          (vector?
            updated-obj)
         )
        
        (is
          (= (count
               updated-obj)
             3)
         )
        
       )
      
     )
    
    (let [test-collection (get-collection
                            "test-collection")
          result (mongodb-update-many
                   test-collection
                   {:test-same-attribute "test-same-value"}
                   {:test-attribute-update "test-value-updated-2"})]
      
      (is
        (= (.getMatchedCount
             result)
           2)
       )
      
      (is
        (= (.getModifiedCount
             result)
           2)
       )
      
      (let [updated-obj (mongodb-find
                          test-collection
                          {:test-attribute-update "test-value-updated-2"})]
        
        (is
          (not
            (nil?
              updated-obj))
         )
        
        (is
          (vector?
            updated-obj)
         )
        
        (is
          (= (count
               updated-obj)
             2)
         )
        
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-delete-by-id
  
  (testing "Test mongodb delete by id"
    
    (let [result (mongodb-delete-by-id
                   nil
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-delete-by-id
                   "test-collection"
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-delete-by-id
                   nil
                   "")]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (is
      (thrown?
        java.lang.IllegalArgumentException
        (mongodb-delete-by-id
          "test-collection"
          ""))
     )
    
    (let [to-delete-obj (mongodb-find-one
                          "test-collection"
                          {:test-attribute "test-value1"})
          result (mongodb-delete-by-id
                   "test-collection"
                   (:_id to-delete-obj))]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (= (.getDeletedCount
             result)
           1)
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-delete-one
  
  (testing "Test mongodb delete one by filter"
    
    (let [result (mongodb-delete-one
                   nil
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-delete-one
                   "test-collection"
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-delete-one
                   nil
                   {})]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-delete-one
                   "test-collection"
                   {:test-same-attribute "test-same-value"})]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (= (.getDeletedCount
             result)
           1)
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-delete-many
  
  (testing "Test mongodb delete many by filter"
    
    (let [result (mongodb-delete-many
                   nil
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-delete-many
                   "test-collection"
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-delete-many
                   nil
                   {})]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-delete-many
                   "test-collection"
                   {:test-same-attribute "test-same-value"})]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (= (.getDeletedCount
             result)
           2)
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-count
  
  (testing "Test mongodb count"
    
    (let [result (mongodb-count
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-count
                   "test-collection")]
      
      (is
        (number?
          result)
       )
      
      (is
        (= result
           3)
       )
      
     )
    
    (let [result (mongodb-count
                   "test-collection"
                   {:test-attribute "test-value1"})]
      
      (is
        (number?
          result)
       )
      
      (is
        (= result
           1)
       )
      
     )
    
    (let [result (mongodb-count
                   "test-collection"
                   "")]
      
      (is
        (nil?
          result)
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-exists
  
  (testing "Test mongodb exists"
    
    (let [result (mongodb-exists
                   nil)]
      
      (is
        (false?
          result)
       )
      
     )
    
    (let [result (mongodb-exists
                   "test-collection")]
      
      (is
        (true?
          result)
       )
      
     )
    
    (let [result (mongodb-exists
                   "test-collection"
                   {})]
      
      (is
        (true?
          result)
       )
      
     )
    
    (let [result (mongodb-exists
                   "test-collection"
                   {:test-attribute "test-value1"})]
      
      (is
        (true?
          result)
       )
      
     )
    
    (let [result (mongodb-exists
                   "test-collection"
                   {:test-attribute "test-value4"})]
      
      (is
        (false?
          result)
       )
      
     )
    
    (let [result (mongodb-exists
                   "test-collection"
                   "")]
      
      (is
        (false?
          result)
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-create-index
  
  (testing "Test mongodb create index"
    
    (let [collection nil
          fields nil
          index-name nil
          unique nil
          expire-after-seconds nil
          result (mongodb-create-index
                   collection
                   fields
                   index-name
                   unique
                   expire-after-seconds)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [collection "test-collection"
          fields nil
          index-name nil
          unique nil
          expire-after-seconds nil
          result (mongodb-create-index
                   collection
                   fields
                   index-name
                   unique
                   expire-after-seconds)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [collection "test-collection"
          fields {:test-attribute 1}
          index-name nil
          unique nil
          expire-after-seconds nil
          result (mongodb-create-index
                   collection
                   fields
                   index-name
                   unique
                   expire-after-seconds)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [collection "test-collection"
          fields {:test-attribute 1}
          index-name "test-index"
          unique nil
          expire-after-seconds nil
          result (mongodb-create-index
                   collection
                   fields
                   index-name
                   unique
                   expire-after-seconds)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (= result
           "test-index")
       )
      
     )
    
    (let [collection "test-collection"
          fields {:test-attribute 1}
          index-name "test-index"
          unique true
          expire-after-seconds nil
          result (mongodb-create-index
                   collection
                   fields
                   index-name
                   unique
                   expire-after-seconds)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [collection "test-collection"
          fields {:test-index-attribute 1}
          index-name "test-unique-index"
          unique true
          expire-after-seconds nil
          result (mongodb-create-index
                   collection
                   fields
                   index-name
                   unique
                   expire-after-seconds)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (= result
           "test-unique-index")
       )
      
     )
    
    (let [collection "test-collection"
          fields {:test-ttl-attribute 1}
          index-name "test-ttl-index"
          unique nil
          expire-after-seconds 30
          result (mongodb-create-index
                   collection
                   fields
                   index-name
                   unique
                   expire-after-seconds)]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (= result
           "test-ttl-index")
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-list-indexes
  
  (testing "Test mongodb list indexes"
    
    (let [result (mongodb-list-indexes
                   nil)]
      
      (is
        (vector?
          result)
       )
      
      (is
        (empty?
          result)
       )
      
     )
    
    (let [result (mongodb-list-indexes
                   "")]
      
      (is
        (vector?
          result)
       )
      
      (is
        (empty?
          result)
       )
      
     )
    
    (let [result (mongodb-list-indexes
                   "test-collection")]
      
      (is
        (vector?
          result)
       )
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (= (count
             result)
           1)
       )
      
     )
    
   )
  
 )

(deftest test-find-index
  
  (testing "Test find-index"
    
    (let [indexes-vector nil
          index-name nil
          result (find-index
                   indexes-vector
                   index-name
                   0)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [indexes-vector []
          index-name nil
          result (find-index
                   indexes-vector
                   index-name
                   0)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [indexes-vector []
          index-name ""
          result (find-index
                   indexes-vector
                   index-name
                   0)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [indexes-vector [{:name "find-index-test"}]
          index-name "find-index-test"
          result (find-index
                   indexes-vector
                   index-name
                   0)]
      
      (is
        (= result
           {:name "find-index-test"})
       )
      
     )
    
    (let [indexes-vector [{:name "find-index-test1"}
                          {:name "find-index-test2"}]
          index-name "find-index-test2"
          result (find-index
                   indexes-vector
                   index-name
                   0)]
      
      (is
        (= result
           {:name "find-index-test2"})
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-get-index
  
  (testing "Test mongodb get index"
    
    (let [result (mongodb-get-index
                   nil
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-get-index
                   "test-collection"
                   nil)]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-get-index
                   "test-collection"
                   "")]
      
      (is
        (nil?
          result)
       )
      
     )
    
    (let [result (mongodb-get-index
                   "test-collection"
                   "_id_")]
      
      (is
        (not
          (nil?
            result))
       )
      
      (is
        (map?
          result)
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-index-exists?
  
  (testing "Test mongodb index exists"
    
    (let [result (mongodb-index-exists?
                   nil
                   nil)]
      
      (is
        (false?
          result)
       )
      
     )
    
    (let [result (mongodb-index-exists?
                   "test-collection"
                   nil)]
      
      (is
        (false?
          result)
       )
      
     )
    
    (let [result (mongodb-index-exists?
                   "test-collection"
                   "")]
      
      (is
        (false?
          result)
       )
      
     )
    
    (let [result (mongodb-index-exists?
                   "test-collection"
                   "_id_")]
      
      (is
        (true?
          result)
       )
      
     )
    
   )
  
 )

(deftest test-mongodb-drop-index
  
  (testing "Test mongodb drop index"
    
    (let [result (mongodb-drop-index
                   nil
                   nil)]
      
      (is
        (false?
          result)
       )
      
     )
    
    (let [result (mongodb-drop-index
                   "test-collection"
                   nil)]
      
      (is
        (false?
          result)
       )
      
     )
    
    (let [result (mongodb-drop-index
                   "test-collection"
                   "")]
      
      (is
        (false?
          result)
       )
      
     )
    
    (let [result (mongodb-drop-index
                   "test-collection"
                   "not-existing-index")]
      
      (is
        (false?
          result)
       )
      
     )
    
    (mongodb-create-index
      "test-collection"
      {:test-index-attribute 1}
      "test-unique-index"
      true)
    
    (let [result (mongodb-drop-index
                   "test-collection"
                   "test-unique-index")]
      
      (is
        (true?
          result)
       )
      
     )
    
   )
  
 )

