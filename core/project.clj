(defproject tesser.core "1.0.8-SNAPSHOT"
  :description "Composable concurrent folds for Clojure."
  :url "http://github.com/aphyr/tesser"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[interval-metrics "1.0.1"]]
  :jvm-opts ["-server"
             "-Xms1024m"
             "-Xmx1024m"
;             "-Dcom.sun.management.jmxremote"
;             "-XX:+UnlockCommercialFeatures"
;             "-XX:+FlightRecorder"
             ]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.12.0"]
                                  [org.clojars.achim/multiset "0.1.1-SNAPSHOT"]
                                  [criterium "0.4.6"]
                                  [org.clojure/test.check "1.1.1"]]
                   ; :jvm-opts ["-XX:-OmitStackTraceInFastThrow"]
                   }}
  :test-selectors {:default #(not-any? % [:stress :bench])
                   :focus   :focus
                   :bench   :bench
                   :stress  :stress})
