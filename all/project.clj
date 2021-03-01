(defproject tesser.all "1.0.4"
  :description "Not a real library; just a placeholder for unifying all the docs."
  :url "http://github.com/aphyr/tesser"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos/"}
  :dependencies [[tesser.core   "1.0.4"]
                 [tesser.math   "1.0.4"]
;                 [tesser.hadoop "1.0.3"
;                  :exclusions [org.codehaus.jackson/jackson-mapper-asl
;                               org.codehaus.jackson/jackson-core-asl]]
                ]
  :codox {:source-paths ["../core/src"
                         "../math/src"
;                         "../hadoop/src"
                         ]
          :output-path "doc/"
          :source-uri "http://github.com/aphyr/tesser/blob/{version}/{filepath}#L{line}"
          :metadata {:doc/format :markdown}}
  :plugins [[lein-codox "0.10.7"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.0"]
                                  ]}
             ; Parkour won't compile without hadoop
;             :provided
;             {:dependencies
;              [[org.apache.hadoop/hadoop-client "2.0.0-mr1-cdh4.3.0"
;                :exclusions [; Breaks codox via incompatible asm
;                             ; FUCK EVERYTHING GRAAAAR
;                             asm]]]}})
})
