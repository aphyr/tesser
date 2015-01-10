(defproject tesser-hadoop-demo "0.1.0-SNAPSHOT"
  :description "An example Hadoop job with Tesser"
  :url "https://github.com/aphyr/tesser"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  ; You may need to add a repository for your hadoop client if it isn't in
  ; maven-central
  :repositories {"cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos/"}
  ; You'll want to depend on clojure and tesser.hadoop, plus any libraries
  ; you'd like to use.
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [tesser.hadoop "0.1.0-SNAPSHOT"
                  ; Hadoop loves to conflict with everything, so you may
                  ; have to exclude some things pulled in via tesser.hadoop's
                  ; parkour deps
                  :exclusions [org.codehaus.jackson/jackson-core-asl
                               org.codehaus.jackson/jackson-mapper-asl]]]
  :jvm-opts ["-server"]
  ; This is the namespace where Hadoop will look for a -main defn when called
  ; with hadoop -jar <uberjar>
  :main tesser.hadoop.demo.core
  :profiles
  ; We AOT-compile the main namespace when providing an uberjar to Hadoop
  {:uberjar {:aot [tesser.hadoop.demo.core]}
   :provided {:dependencies
              ; Replace this with the appropriate hadoop client for
              ; your hadoop cluster
              [[org.apache.hadoop/hadoop-client "2.0.0-mr1-cdh4.3.0"]]}})
