(ns mongo-lib.parser
  (:require [clojure.string :as cs :refer [index-of]]
            [clojure.java.shell :refer [sh]]))

(def obj-start
     "{")

(def obj-end
     "}")

(def list-start
     "[")

(def list-end
     "]")

(def quote-char
     "\"")

(def apostrophe
     "'")

(def property-separator
     ",")

(def key-value-separator
     ":")

(def blank-space
     " ")

(def before-data
     ["NumberLong("])

(def closing-parentheses
     ")")

(defn return-minimal-index
  "Return minimal index from passed vector"
  [indexes-vector]
  (let [lazy-seq (map
                   (fn [param]
                     (if (number? param)
                       param
                       (Integer/MAX_VALUE))
                    )
                   indexes-vector)]
    (apply
      min
      lazy-seq))
  )

(defn read-string-fn
  "Values that in mongo db have function like presentation as NumberLong(#)
   this function removes text and reads rest of string to convert it to clojure data type"
  [obj-as-string]
  (let [obj-atom (atom obj-as-string)]
    (doseq [text before-data]
      (swap!
        obj-atom
        cs/replace
        text
        ""))
    (swap!
      obj-atom
      cs/replace
      closing-parentheses
      "")
    (read-string
      @obj-atom))
 )

(defn substring-r-out
  "Removes everything before index from r-out string atom"
  [r-out
   index]
  (swap!
    r-out
    (fn [atom-value
         start-index
         end-index]
      (.substring
        atom-value
        start-index
        end-index))
    (inc index)
    (count
      @r-out))
  r-out)

(defn parse-mongo
  "Parses mongo objects from string into clojure map or vector representations"
  [r-out
   object
   obj-start-level
   level
   & [obj-key
      obj-value]]
  (let [obj-start-index (index-of
                          @r-out
                          obj-start)
        list-start-index (index-of
                           @r-out
                           list-start)
        obj-end-index (index-of
                        @r-out
                        obj-end)
        list-end-index (index-of
                         @r-out
                         list-end)
        quote-char-index (index-of
                           @r-out
                           quote-char)
        property-separator-index (index-of
                                   @r-out
                                   property-separator)
        key-value-separator-index (index-of
                                    @r-out
                                    key-value-separator)
        case-param (return-minimal-index
                     [obj-start-index
                      list-start-index
                      obj-end-index
                      list-end-index
                      quote-char-index
                      property-separator-index
                      key-value-separator-index])
       append-new-object (fn [bson-out
                              current-object
                              new-object-key
                              new-object
                              start-level
                              & [end-index]]
                           ;(println "---------------------")
                           ;(println start-level)
                           ;(println level)
                           ;(println @bson-out)
                           ;(println @current-object)
                           ;(println new-object-key)
                           ;(println new-object)
                           (when (or (= (+ start-level
                                           2)
                                        level)
                                     (= (inc start-level)
                                        level)
                                     (and (= start-level
                                             0)
                                          (= level
                                             0)))
                             (if (map? @current-object)
                               (swap!
                                 current-object
                                 assoc
                                 (keyword
                                   new-object-key)
                                 new-object)
                               (swap!
                                 current-object
                                 conj
                                 new-object))
                            )
                           (if (or (empty? @bson-out)
                                   end-index)
                             new-object
                             (if (and (= start-level
                                         level)
                                      (not= level
                                            0))
                               @current-object
                               (parse-mongo
                                 bson-out
                                 current-object
                                 start-level
                                 (if (= level
                                        0)
                                   level
                                   (inc level))
                                ))
                            ))]
    ;(println "---------------------")
    ;(println obj-start-level)
    ;(println level)
    ;(println @r-out)
    ;(println @object)
    ;(println obj-key)
    ;(println obj-value)
    ;(Thread/sleep 100)
    (cond 
      (= case-param
         obj-start-index) (let [new-object (parse-mongo
                                             (substring-r-out
                                               r-out
                                               obj-start-index)
                                             (atom {})
                                             level
                                             (inc level))]
                            (append-new-object
                              r-out
                              object
                              obj-key
                              new-object
                              level))
      (= case-param
         obj-end-index) (if-let [new-object obj-value]
                          (do
                            (substring-r-out
                              r-out
                              obj-end-index)
                            (append-new-object
                              r-out
                              object
                              obj-key
                              new-object
                              obj-start-level
                              true))
                          (let [new-object (read-string-fn
                                             (.substring
                                               @r-out
                                               0
                                               obj-end-index))]
                            (substring-r-out
                              r-out
                              obj-end-index)
                            (append-new-object
                              r-out
                              object
                              obj-key
                              new-object
                              obj-start-level
                              true))
                         )
      (= case-param
         list-start-index) (let [new-object (parse-mongo
                                              (substring-r-out
                                                r-out
                                                list-start-index)
                                              (atom [])
                                              level
                                              (inc level))]
                             (append-new-object
                               r-out
                               object
                               obj-key
                               new-object
                               obj-start-level))
      (= case-param
         list-end-index) (if-let [new-object obj-value]
                           (do
                             (substring-r-out
                               r-out
                               list-end-index)
                             (append-new-object
                               r-out
                               object
                               obj-key
                               new-object
                               obj-start-level
                               true))
                           (let [new-object (read-string-fn
                                              (.substring
                                                @r-out
                                                0
                                                list-end-index))]
                             (substring-r-out
                               r-out
                               list-end-index)
                             (append-new-object
                               r-out
                               object
                               obj-key
                               new-object
                               obj-start-level
                               true))
                          )
      (= case-param
         quote-char-index) (let [closing-quote-char-index (index-of
                                                            (deref
                                                              (substring-r-out
                                                                r-out
                                                                quote-char-index))
                                                            quote-char)
                                 string-value (.substring
                                                @r-out
                                                0
                                                closing-quote-char-index)
                                 [obj-key
                                  obj-value] (if obj-key
                                               [obj-key
                                                string-value]
                                               [string-value
                                                nil])]
                             (substring-r-out
                               r-out
                               closing-quote-char-index)
                             (parse-mongo
                               r-out
                               object
                               obj-start-level
                               (inc level)
                               obj-key
                               obj-value))
      (= case-param
         property-separator-index) (let [new-object (if obj-value
                                                      obj-value
                                                      (read-string-fn
                                                        (.substring
                                                          @r-out
                                                          0
                                                          property-separator-index))
                                                     )]
                                     (substring-r-out
                                       r-out
                                       property-separator-index)
                                     (append-new-object
                                       r-out
                                       object
                                       obj-key
                                       new-object
                                       obj-start-level))
      (= case-param
         key-value-separator-index) (let [new-object (parse-mongo
                                                       (substring-r-out
                                                         r-out
                                                         key-value-separator-index)
                                                       object
                                                       level
                                                       (inc level)
                                                       obj-key)]
                                      (append-new-object
                                        r-out
                                        object
                                        obj-key
                                        new-object
                                        level))
      (not
        (or (= case-param
               obj-start-index)
            (= case-param
               obj-end-index)
            (= case-param
               list-start-index)
            (= case-param
               list-end-index)
            (= case-param
               quote-char-index)
            (= case-param
               property-separator-index)
            (= case-param
               key-value-separator-index))) @object))
 )

(defn read-mongo
  "Test function"
  []
  (try
    (let [result (sh "mongo" "--quiet" "localhost:27017/personal-organiser-db" :in "DBQuery.shellBatchSize=3000;db.grocery.find();")
          r-exit (:exit result)
          r-out (:out result)
          r-err (:err result)]
      ;(println r-exit)
      ;(println r-out)
      ;(println r-err)
      (parse-mongo
        (atom r-out)
        (atom [])
        0
        0))
    (catch Exception e
      (.printStackTrace e))
   ))

