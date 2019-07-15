(defproject org.clojars.vladimirmarkovic86/mongo-lib "0.2.11"
  :description "Mongo library"
  :url "http://github.com/VladimirMarkovic86/mongo-lib"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.mongodb/mongodb-driver "3.9.0"]
                 ]

  :min-lein-version "2.0.0"
  
  :source-paths ["src/clj"]
  :test-paths ["test/clj"]
  
  :jar-exclusions [#"README.md$"
                   #"LICENSE$"])

