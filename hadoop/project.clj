(defproject tesser.hadoop "1.0.3"
  :description "Tesser: Hadoop support via Parkour."
  :url "http://github.com/aphyr/tesser"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :java-source-paths ["src/"]
  :javac-options ["-target" "1.5"
                  "-source" "1.5"]
  :repositories
  {"cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos/"}
  :dependencies [[tesser.core "1.0.3"]
                 [tesser.math "1.0.3"]
                 [org.clojure/data.fressian "0.2.0"]
                 [com.damballa/parkour "0.5.4"]
                 [org.codehaus.jsr166-mirror/jsr166y "1.7.0"]
                 [proteus "0.1.4"]]
  :profiles {:dev
             {:dependencies [[org.clojure/clojure "1.9.0"]
                             [org.clojars.achim/multiset "0.1.0-SNAPSHOT"]
                             [criterium "0.4.3"]
                             [org.clojure/test.check "0.7.0"]]}
             :provided
             {:dependencies
              ; Just so we can compile our Writable
              [[org.apache.hadoop/hadoop-client "2.0.0-mr1-cdh4.3.0"
                :exclusions [org.slf4j/slf4j-api]]
               ; for compiling without tesser.math
               [com.clearspring.analytics/stream "2.7.0"]]}})
