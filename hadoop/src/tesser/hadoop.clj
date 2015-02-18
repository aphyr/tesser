(ns tesser.hadoop
  "Helps you run a fold on Hadoop!"
    (:require [clojure [string :as str]
               [pprint :refer [pprint]]]
              [clojure.core.reducers :as r]
              [clojure.data.fressian :as fress]
              tesser.hadoop.serialization
              [tesser
               [utils :refer :all]
               [core :as t]]
              [parkour [conf :as conf]
               [fs :as fs]
               [mapreduce :as mr]
               [graph :as pg]
               [tool :as tool]
               [wrapper :as wrapper]]
              [parkour.io [text :as text]
               [seqf :as seqf]
               [sample :as sample]])
  (:import (tesser.hadoop_support FressianWritable)
           (java.io DataInput
                    DataOutput
                    IOException
                    EOFException
                    ByteArrayInputStream
                    ByteArrayOutputStream)
           (java.nio ByteBuffer)
           (org.apache.hadoop.io Text
                                 LongWritable
                                 NullWritable
                                 BytesWritable
                                 Writable)
           (org.apache.hadoop.mapred JobPriority)))

(defn resolve+
  "Resolves a symbol to a var, requiring the namespace if necessary. If the
  namespace doesn't exist, throws just like `clojure.core/require`. If the
  symbol doesn't exist after requiring, returns nil."
  [sym]
  (or (resolve sym)
      (let [ns (->> sym str (re-find #"(.+)\/") second symbol)]
        (require ns)
        (resolve sym))))

(defn rehydrate-fold
  "Takes the name of a function that generates a fold (a symbol) and args for
  that function, and invokes the function with args to build a fold, which is
  then compiled and returned."
  [fold-name fold-args]
  (-> fold-name
      resolve+
      deref
      (apply fold-args)
      t/compile-fold))

(defn serialize-error
  "Convert an exception to an error."
  [state input e]
  ; Log locally so we'll have something in the hadoop logs
  (.printStackTrace e)
  {::error?  true
   :class   (.getName (class e))
   :message (.getMessage e)
   :string  (.toString e)
   :trace   (->> (.getStackTrace e)
                 (map (fn [^StackTraceElement frame]
                        (str (.getClassName frame) " "
                             (.getMethodName frame) " ("
                             (.getFileName frame) ":"
                             (.getLineNumber frame) ")")))
                 (str/join "\n"))
   :state state
   :input input})

(defn error?
  "Is this an error object?"
  [x]
  (and (map? x)
       (::error? x)))

(defn print-error
  "Print an error to *err*."
  [e]
  (locking *out*
    (binding [*out* *err*]
      (println)
      (println "## Hadoop Error:" (:class e))
      (println)
      (println "State prior to reduction error:")
      (pprint (:state e))
      (println)
      (println "Input that caused reduction error:")
      (println (:input e))
      (println)
      (println (:class e))
      (println (:message e))
      (println (:string e))
      (println (:trace e)))))

(defn fold-mapper
  "A generic, stateful hadoop mapper for applying a fold to a Hadoop dataset.
  This function returns a mapper for fold defined by make-fold
  applied to fold-name & additional args."
  {::mr/source-as :vals
   ::mr/sink-as   :vals}
  [fold-name fold-args input]
  (list (try (let [fold (rehydrate-fold fold-name fold-args)
                   red  (:reducer fold)
                   post (:post-reducer fold)]
               (post
                 (reduce (fn [acc line]
                           (try
                             (red acc line)
                             (catch Exception e
                               (reduced (serialize-error acc line e)))))
                         ((:reducer-identity fold))
                         input)))
             (catch Exception e
               (serialize-error nil nil e)))))

(defn fold-reducer
  "This function returns a parkour reducer for fold defined by make-fold
  applied to fold-name & additional args"
  {::mr/source-as :vals
   ::mr/sink-as   :vals}
  [fold-name fold-args input]
  (list (try (let [fold (rehydrate-fold fold-name fold-args)
                   combiner (:combiner fold)
                   combined (reduce (fn [acc x]
                                      (try
                                        (if (error? x)
                                          (reduced x)
                                          (combiner acc x))
                                        (catch Exception e
                                          (reduced (serialize-error acc x e)))))
                                    ((:combiner-identity fold))
                                    input)]
               (if (error? combined)
                 combined
                 ((:post-combiner fold) combined)))
             (catch Exception e
               (serialize-error nil nil e)))))

(defn fold*
  "Takes a Parkour graph and applies a fold to it. Takes a var for a function,
  taking `args`, which constructs a fold. Returns a new (unexecuted) graph.
  The output of this job will be a single-element Fressian structure containing
  the results of the fold applied to the job's inputs."
  [graph fold-var & args]
  (let [fold-name (var->sym fold-var)]
    (-> graph
        (pg/map #'fold-mapper fold-name args)
        (pg/partition [NullWritable FressianWritable])
        (pg/reduce #'fold-reducer fold-name args))))

(defonce job-name-counter
  (atom (rand-int 1000)))

(defn gen-job-name!
  "Generates a new job name. Job names start at a random small integer and
  increment sequentially from there. Job names are printed to stderr when
  generated."
  []
  (let [n (str "tesser-" (swap! job-name-counter inc))]
    (binding [*out* *err*]
      (println "\n## Job" n "\n"))
    n))

(defn execute
  "Like `parkour.graph/execute`, but specialized for folds. Takes a parkour
  graph, a jobconf, and a job name. Executes the job, then returns a sequence
  of fold results. Job names will be automatically generated if not provided."
  ([graph conf]
   (execute graph conf (gen-job-name!)))
  ([graph conf job-name]
   ; For each phase, extract the first tuple, then the value.
   (map (comp second reduce-first)
        (pg/execute graph conf job-name))))

(defn dsink
  "Given a work directory and a string name for this file, builds a dsink for a
  fold to dump its output to, in `[NullWritable FressianWritable]` format."
  [work-dir file-name]
  (->> file-name
       (fs/path work-dir)
       (seqf/dsink [NullWritable FressianWritable])))

(defn fold
  "A simple, all-in-one fold operation. Takes a jobconf, workdir, input dseq,
  var which points to a fold function, and arguments for the fold function.
  Runs the fold against the dseq and returns its results. Names output dsink
  after fold symbol. On error, throws an `ex-info`."
  [conf input workdir fold-var & args]
  (let [in       (pg/input input)
        path     (name (var->sym fold-var))]
    (try
      (let [x (-> (apply fold* in fold-var args)
                  (pg/output (dsink workdir path))
                  (execute conf)
                  first)]
        (when (error? x) (throw (ex-info "Hadoop fold error" x)))
        x))))

(defn identity-mapper
  "Does nothing in the map step."
  {::mr/source-as :vals
   ::mr/sink-as   :vals}
  [records]
  (r/map identity records))

(defn partition-randomly
  ^long [key val ^long nparts]
  (rand-int nparts))

(defn set-one-reducer
  "Takes a jobconf, returns a jobconf with mapred.reduce.tasks set to 1."
  [conf]
  (conf/assoc! conf "mapred.reduce.tasks" 1))

(defn fold-reducer-without-post-combiner
  "Like fold-reducer, but omits :post-combiner so it can be used in reduce tasks that are continued in multiple jobs."
  {::mr/source-as :vals
   ::mr/sink-as   :vals}
  [fold-name fold-args input]
  (list (try (let [fold (rehydrate-fold fold-name fold-args)
                   combiner (:combiner fold)
                   combined (reduce (fn [acc x]
                                      (try
                                        (if (error? x)
                                          (reduced x)
                                          (combiner acc x))
                                        (catch Exception e
                                          (reduced (serialize-error acc x e)))))
                                    ((:combiner-identity fold))
                                    input)]
               combined)
             (catch Exception e
               (serialize-error nil nil e)))))

(defn fold-reduce-twice
  "Like fold, but in two stages (jobs). First job uses the number of reduce tasks specified in mapred.reduce.tasks.
  The second job does nothing in the map step, then uses one reduce task."
  [conf input workdir fold-var & args]
  (let [fold-name  (var->sym fold-var)
        path       (name fold-name)
        path1      (str path "-1")
        path2      (str path "-2")
        kv-classes [NullWritable FressianWritable]

        run (fn [conf graph]
              (let [x (-> graph
                          (execute conf)
                          first)]
                (when (error? x) (throw (ex-info "Hadoop fold error" x)))
                x))]
    (run conf
      (-> (pg/input input)
          (pg/map #'fold-mapper fold-name args)
          (pg/partition kv-classes #'partition-randomly)
          (pg/reduce #'fold-reducer-without-post-combiner fold-name args)
          (pg/output (dsink workdir path1))))
    (run (set-one-reducer conf)
      (-> (pg/input (dsink workdir path1))
          (pg/map #'identity-mapper)
          (pg/partition kv-classes)
          (pg/reduce #'fold-reducer fold-name args)
          (pg/output (dsink workdir path2))))))
