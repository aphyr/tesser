(defproject tesser-hadoop-demo "0.1.0-SNAPSHOT"
  :description "An example Hadoop job with Tesser"
  :url "https://github.com/aphyr/tesser"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos/"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [tesser.hadoop "0.1.0-SNAPSHOT"]]
  :main tesser.hadoop.demo.core
  :jvm-opts ["-server"]
  :profiles
  {:uberjar {:aot [tesser.hadoop.demo.core]}
   :provided {:dependencies
              ; Replace this with the appropriate hadoop client for
              ; your hadoop cluster
              [[org.apache.hadoop/hadoop-client "2.0.0-mr1-cdh4.3.0"]]}})
