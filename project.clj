(defproject tesser "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.clearspring.analytics/stream "2.7.0"]
                 [interval-metrics "1.0.0"]
                 [org.clojure/math.combinatorics "0.0.8"]
                 [org.clojure/math.numeric-tower "0.0.4"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.6.0"]
                                  [org.clojars.achim/multiset "0.1.0-SNAPSHOT"]
                                  [criterium "0.4.3"]
                                  [org.clojure/test.check "0.5.9"]]}})
