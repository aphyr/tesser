(ns tesser.bench-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]]
            [multiset.core :refer [multiset]]
            [criterium.core :refer [with-progress-reporting quick-bench bench]]
            [tesser.core :as t]
            [tesser.simple :as s]
            [tesser.utils :refer :all]
            [clojure.core.reducers :as r]
            [clojure.set :as set]))

(def n 100000)

(defn long-ary []
  (->> #(rand-int n)
       repeatedly
       (take n)
       long-array))

(defn long-vec []
  (->> #(rand-int n)
       repeatedly
       (take n)
       vec))

;(deftest array-simple-reduce-sum
;  (let [a (long-ary)]
;    (prn "(reduce + 0 long-array)")
;    (with-progress-reporting
;      (quick-bench (reduce + 0 a)))

;    (prn "(s/reduce + 0 long-array)")
;    (with-progress-reporting
;      (quick-bench (s/reduce + 0 a)))))

(deftest ^:bench array-map-filter-fold-sum
  (let [a (long-vec)]
    (prn "reducers")
    (quick-bench (->> a
                      (r/map inc)
                      (r/filter even?)
                      (r/fold +)))

    (prn "tesser")
    (quick-bench (->> (t/map inc)
                      (t/filter even?)
                      (t/fold +)
                      (t/tesser (partition-all-fast 1024 a))))))

(deftest ^:bench stress
  (let [a (long-vec)]
    (dotimes [i 10000000]
      (->> (t/map inc)
           (t/filter even?)
           (t/fold +)
           (t/tesser (partition-all-fast 1024 a))))))
