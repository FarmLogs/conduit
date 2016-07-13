(defproject com.farmlogs.conduit "0.2.1"
  :description "Provides reliable publishing via RMQ."
  :license {:name "The MIT License (MIT)"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.novemberain/langohr "3.4.1"]
                 [com.stuartsierra/component "0.3.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/tools.logging "0.3.1"]]

  :profiles {:dev {:dependencies
                   [[org.clojure/test.check "0.9.0"]
                    [ch.qos.logback/logback-classic "1.1.2" :exclusions [org.slf4j/slf4j-api]]
                    [org.slf4j/jul-to-slf4j "1.7.7"]
                    [org.slf4j/jcl-over-slf4j "1.7.7"]
                    [org.slf4j/log4j-over-slf4j "1.7.7"]
                    [criterium "0.4.3"]]}}
  :repositories {"farmlogs-internal"
                 {:url "s3p://fl-maven-repo/mvn"
                  :username ~(System/getenv "AMAZON_KEY")
                  :passphrase ~(System/getenv "AMAZON_SECRET")}})
