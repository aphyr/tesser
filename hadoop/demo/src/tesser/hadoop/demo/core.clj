(ns tesser.hadoop.demo.core
  (:gen-class)
  (:require [tesser [core :as t]
                    [hadoop :as h]]
            [parkour [tool :as tool]]
            [parkour.io [text :as text]]
            [clojure.pprint :refer [pprint]]))

;; First, define the fold that we want to run. Note that all this work takes
;; place in the Hadoop cluster: mappers map and compute local frequency
;; distributions, and the reducer merges those frequency distributions and
;; sorts/reverses in the post-combine phase.
(defn char-freq
  "Returns a fold that computes a frequency distribution over characters in
  input strings. Takes a single argument for case normalization: :upper,
  :lower, or nil to leave strings untransformed."
  [normalizer]
  (->> ; Normalize case
       (t/map (case normalizer
           :upper #(.toUpperCase ^String %)
           :lower #(.toLowerCase ^String %)
           nil    identity))
       ; Break up strings into characters
       (t/mapcat seq)
       ; And take a frequency distribution
       (t/frequencies)
       ; Sort the final results by their frequency
       (t/post-combine (partial sort-by second))
       ; In descending order
       (t/post-combine reverse)))

;; On the local JVM, we'll set up a Hadoop job and tell it to evaluate
;; (char-freq :lower) on our inputs, writing results to the work directory.
(defn run
  "Takes a parkour jobconf and runs char-freq over the given input files,
  writing temporary results to work-dir. When the Hadoop job is complete,
  returns the results of the fold to the local JVM."
  [conf [work-dir input-file]]
  (h/fold conf
          (text/dseq input-file)
          work-dir
          ; Note that we pass the fold-creating function as a var, and can
          ; provide arguments to the function, which will be automatically
          ; serialized and sent to each Hadoop worker to create local instances
          ; of the fold as needed. Namespaced vars work fine too.
          #'char-freq :lower))

;; Hadoop machinery

(defn tool
  "Top-level hadoop runner. Returns an exit code."
  [conf & args]
  (try
    (pprint (run conf args))
    0 ; Return succesfully

    ; The default Parkour exception handling is pretty minimal; we'll print out
    ; our own here.
    (catch clojure.lang.ExceptionInfo e
      (let [data (ex-data e)]
        (if (h/error? data)
          (h/print-error data)
          (throw e)))

      ; Exit status
      1)))

(defn -main [& args]
  (System/exit (tool/run tool args)))
