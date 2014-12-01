(defproject tesser.all "0.1.0-SNAPSHOT"
  :description "Not a real library; just a placeholder for unifying all the docs."
  :url "http://github.com/aphyr/tesser"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[tesser.core "0.1.0-SNAPSHOT"]
                 [tesser.math "0.1.0-SNAPSHOT"]]
  :codox {:sources ["../core/src/"
                    "../math/src"]
          :src-dir-uri "http://github.com/aphyr/tesser/blob/master/"
          :src-linenum-anchor-prefix "L"
          :defaults {:doc/format :markdown}}
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.6.0"]]}})
