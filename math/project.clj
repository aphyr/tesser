(defproject tesser.math "1.0.0"
  :description "Concurrent folds for statistical analysis"
  :url "http://github.com/aphyr/tesser"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[tesser.core "1.0.0"]
;                 [com.tdunning/t-digest "3.0"]
                 [com.clearspring.analytics/stream "2.7.0"]
                 [org.hdrhistogram/HdrHistogram "2.1.2"]
                 [org.clojure/math.combinatorics "0.0.8"]
                 [org.clojure/math.numeric-tower "0.0.4"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.6.0"]
                                  [org.clojars.achim/multiset "0.1.0-SNAPSHOT"]
                                  [criterium "0.4.3"]
                                  [org.clojure/test.check "0.7.0"]]}})
