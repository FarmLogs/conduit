(defproject com.farmlogs.messaging "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.novemberain/langohr "3.4.1"]
                 [com.stuartsierra/component "0.3.0"]
                 [org.clojure/core.async "0.2.374"]

                 [org.clojure/tools.logging "0.3.1"]
                 [ch.qos.logback/logback-classic "1.1.2" :exclusions [org.slf4j/slf4j-api]]
                 [org.slf4j/jul-to-slf4j "1.7.7"]
                 [org.slf4j/jcl-over-slf4j "1.7.7"]
                 [org.slf4j/log4j-over-slf4j "1.7.7"]]

  :profiles {:dev {:dependencies [[org.clojure/test.check "0.9.0"]]}})
