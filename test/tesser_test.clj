(ns tesser-test
  (:require [clojure.test :refer :all]
            [clojure.math.numeric-tower :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]]
            [multiset.core :refer [multiset]]
            [tesser.utils :refer :all]
            [tesser :as t]))

(def test-count
    1e2)

(deftest map-sum-test
  (is (= (->> (t/map inc)
              (t/sum)
              (t/tesser [[1 2 3] [4 5 6] []]))
         27)))

;(deftest facet-mean-test
;  (is (= (->> (t/facet)
;              (t/mean)
;              (t/tesser [[{:x 1, :y 2} {}]
;                        [{:y 3, :z 4}]]))
;         {:x 1, :y 2, :z 4})))

;; Utility functions
(defn approx=
  "Equal to within err fraction, or if one is zero, to within err absolute."
  [err x y]
  (if (or (zero? x) (zero? y))
    (< (- err) (- x y) err)
    (< (- 1 err) (/ x y) (+ 1 err))))

(def =ish
  "Almost equal"
  (partial approx= 1/1000))

(defn chunks
  "Given a generator for inputs, returns a generator that builds
  sequences of sequences of inputs."
  [input-gen]
  (gen/vector (gen/vector input-gen) 0 5))

(defn flatten1
  "Flattens a single level."
  [seq-of-seqs]
  (apply concat seq-of-seqs))

;; Tests

(defspec map-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/map inc)
                            (t/map (partial * 2))
                            (t/into (multiset))
                            (t/tesser chunks))
                       (->> chunks
                            flatten1
                            (map inc)
                            (map (partial * 2))
                            (into (multiset)))))))

(defspec keep-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/keep #(when (even? %) (inc %)))
                            (t/into (multiset))
                            (t/tesser chunks))
                       (->> chunks
                            flatten1
                            (keep #(when (even? %) (inc %)))
                            (into (multiset)))))))

(defspec filter-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/filter odd?)
                            (t/into (multiset))
                            (t/tesser chunks))
                       (->> chunks
                            flatten1
                            (filter odd?)
                            (into (multiset)))))))

(defspec remove-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/remove odd?)
                            (t/into (multiset))
                            (t/tesser chunks))
                       (->> chunks
                            flatten1
                            (remove odd?)
                            (into (multiset)))))))

(defspec into-vec-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (sort (t/tesser chunks (t/into [])))
                       (sort (flatten1 chunks))))))

(defspec into-set-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/into #{}))
                       (set (flatten1 chunks))))))

;; Splitting folds

(defspec facet-spec
  test-count
  ; Sum over maps of keywords to ints
  (prop/for-all [chunks (chunks
                          (gen/resize 4 (gen/map (gen/elements [:a :b :c :d :e])
                                                 gen/int)))]
                (is (= (->> (t/facet)
                            (t/sum)
                            (t/tesser chunks))
                       (->> chunks
                            flatten1
                            (apply merge-with + {}))))))

(defspec fuse-spec
  test-count
  ; sum, set, and multiset over ints
  (prop/for-all [chunks (chunks gen/int)]
                (let [inputs (flatten1 chunks)]
                  (is (= (->> (t/fuse {:sum      (t/sum)
                                       :set      (t/into #{})
                                       :multiset (t/into (multiset))})
                              (t/tesser chunks))
                         {:sum      (reduce + inputs)
                          :set      (set inputs)
                          :multiset (into (multiset) inputs)})))))

;; Numeric folds

(defspec sum-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/sum))
                        (reduce + 0 (flatten1 chunks))))))

(defn mean
  [coll]
  (assert (not (empty? coll)))
  (/ (reduce + coll) (count coll)))

(defspec mean-spec
  test-count
  (prop/for-all [chunks (gen/such-that (partial some not-empty)
                                       (chunks gen/int))]
                (is (== (t/tesser chunks (t/mean))
                        (mean (flatten1 chunks))))))


(defn variance
  [coll]
  (/ (->> coll
          (map #(expt (- % (mean coll)) 2))
          (reduce +))
     (max (dec (count coll)) 1)))

(defspec variance-spec
  test-count
  (prop/for-all [chunks (gen/such-that (partial some not-empty)
                                       (chunks gen/int))]
                (=ish (t/tesser chunks (t/variance))
                      (variance (flatten1 chunks)))))

(defspec standard-deviation-spec
  test-count
  (prop/for-all [chunks (gen/such-that (partial some not-empty)
                                       (chunks gen/int))]
                (=ish (t/tesser chunks (t/standard-deviation))
                      (sqrt (variance (flatten1 chunks))))))
