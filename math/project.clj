(defproject tesser.math "1.0.6"
  :description "Concurrent folds for statistical analysis"
  :url "http://github.com/aphyr/tesser"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[tesser.core "1.0.6-SNAPSHOT"]
;                 [com.tdunning/t-digest "3.0"]
                 [com.clearspring.analytics/stream "2.9.8"]
                 ; 2.1.10 changed quantiles; see https://github.com/HdrHistogram/HdrHistogram/issues/194
                 [org.hdrhistogram/HdrHistogram "2.1.9"]
                 [org.clojure/math.combinatorics "0.1.6"]
                 [org.clojure/math.numeric-tower "0.0.5"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.11.1"]
                                  [org.clojars.achim/multiset "0.1.1-SNAPSHOT"]
                                  [criterium "0.4.6"]
                                  [org.clojure/test.check "1.1.1"]]}})
