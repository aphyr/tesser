(ns tesser.simple
  "Drop-in replacement for `reducers/fold` and `core/reduce`, where order
  doesn't matter."
  (:refer-clojure :exclude [reduce])
  (:require [tesser.core :as t]
            [tesser.utils :refer :all]))

(defn fold
  "Folds over a collection using a parallel reduce-combine strategy. Collection
  is partitioned into chunks of approximately `n` (default 512), and folded
  over with `t/tesser`. Unlike reducers `fold`, this function does not preserve
  order."
  ([reducef coll]
   (fold reducef reducef coll))
  ([combinef reducef coll]
   (fold 512 combinef reducef coll))
  ([n combinef reducef coll]
   (t/tesser (partition-all-fast n coll)
             (t/fold {:reducer  reducef
                      :combiner combinef}))))

(defn reduce
  "Like `clojure.core/reduce, but parallel, using `t/tesser` over 512-element
  chunks. Unlike `core/reduce`, does not preserve order, init must be an
  identity element, f must be associative, etc."
  ([f init coll]
   (t/tesser (partition-all-fast 512 coll)
             (t/fold {:reducer  f
                      :identity (constantly init)}))))
