(defproject tesser.all "0.1.0-SNAPSHOT"
  :description "Not a real library; just a placeholder for unifying all the docs."
  :url "http://github.com/aphyr/tesser"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos/"}
  :dependencies [[tesser.core   "0.1.0-SNAPSHOT"]
                 [tesser.math   "0.1.0-SNAPSHOT"]
                 [tesser.hadoop "0.1.0-SNAPSHOT"
                  :exclusions [org.codehaus.jackson/jackson-mapper-asl
                               org.codehaus.jackson/jackson-core-asl]]]
  :codox {:sources ["../core/src"
                    "../math/src"
                    "../hadoop/src"]
          ; Huh, codox strips off a master.
          :src-dir-uri "http://github.com/aphyr/tesser/blob/master/master/"
          :src-linenum-anchor-prefix "L"
          :defaults {:doc/format :markdown}}
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.6.0"]]}
             ; Parkour won't compile without hadoop
             :provided
             {:dependencies
              [[org.apache.hadoop/hadoop-client "2.0.0-mr1-cdh4.3.0"
                :exclusions [; Breaks codox via incompatible asm
                             ; FUCK EVERYTHING GRAAAAR
                             asm]]]}})
