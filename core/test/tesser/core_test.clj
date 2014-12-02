(ns tesser.core-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]]
            [multiset.core :refer [multiset]]
            [tesser.utils :refer :all]
            [tesser.core :as t]))

(def test-count 1e2)

(defn option
  "Generator that may return nil."
  [gen]
  (gen/one-of [(gen/return nil) gen]))

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

(defspec mapcat-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/mapcat range)
                            (t/filter even?)
                            (t/into (multiset))
                            (t/tesser chunks))
                       (->> chunks
                            flatten1
                            (mapcat range)
                            (filter even?)
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

(defspec take-spec
  test-count
  ; Our chunks will be random partitionings of the integers, and we'll take
  ; them into a vector, then verify it contains n unique elements.
  (prop/for-all [n     gen/pos-int
                 sizes (gen/vector gen/pos-int 0 10)]
;                (prn) (prn)
                (let [total  (reduce + sizes)
                      chunks (->> sizes
                                  (reduce (fn [[start chunks] size]
                                            [(+ start size)
                                             (->> start
                                                  (iterate inc)
                                                  (take size)
                                                  (conj chunks))])
                                          [0 []])
                                  second)]
;                  (prn :n n :chunks chunks)
                  (let [x (->> (t/take n)
                               (t/into [])
                               (t/tesser chunks))]
;                    (prn :total total :n n :result x)
                    (is (and (or (= (count x) n)         ; Exactly n inputs
                                 (and (< (count x) n) ; Not enough to hit n
                                      (= (count x) total)))
                             ; Unique
                             (= (count x)
                                (count (set x)))))))))

(defspec take-take-spec
  test-count
  ; Our chunks will be random partitionings of the integers, and we'll take
  ; them into a vector, then verify it contains n unique elements. Performing
  ; two takes verifies that the reduced optimizations compose well. :)
  (prop/for-all [n     gen/pos-int
                 sizes (gen/vector gen/pos-int 0 10)]
;                (prn) (prn)
                (let [total  (reduce + sizes)
                      chunks (->> sizes
                                  (reduce (fn [[start chunks] size]
                                            [(+ start size)
                                             (->> start
                                                  (iterate inc)
                                                  (take size)
                                                  (conj chunks))])
                                          [0 []])
                                  second)]
;                  (prn :n n :chunks chunks)
                  (let [x (->> (t/take (inc n))
                               (t/take n)
                               (t/into [])
                               (t/tesser chunks))]
;                    (prn :total total :n n :result x)
                    (is (and (or (= (count x) n)         ; Exactly n inputs
                                 (and (< (count x) n) ; Not enough to hit n
                                      (= (count x) total)))
                             ; Unique
                             (= (count x)
                                (count (set x)))))))))

(defspec post-combine-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/map inc)
                            (t/map str)
                            (t/into #{})
                            (t/post-combine sort)
                            (t/post-combine (partial map read-string))
                            (t/tesser chunks))
                       (->> (flatten1 chunks)
                            (map inc)
                            (map str)
                            (into #{})
                            sort
                            (map read-string))))))

;; Splitting folds

(defspec facet-spec
  test-count
  ; Sum over maps of keywords to ints
  (prop/for-all [chunks (chunks
                          (gen/resize 4 (gen/map (gen/elements [:a :b :c :d :e])
                                                 gen/int)))]
                (is (= (->> (t/facet)
                            (t/min)
                            (t/tesser chunks))
                       (->> chunks
                            flatten1
                            (apply merge-with min {}))))))

(defspec fuse-spec
  test-count
  ; sum, set, and multiset over ints
  (prop/for-all [chunks (chunks gen/int)]
                (let [inputs (flatten1 chunks)]
                  (is (= (->> (t/fuse {:max      (t/max)
                                       :set      (t/into #{})
                                       :multiset (t/into (multiset))})
                              (t/tesser chunks))
                         {:max      (when-not (empty? inputs)
                                      (reduce max inputs))
                          :set      (set inputs)
                          :multiset (into (multiset) inputs)})))))

;; Basic reductions

(defspec count-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/count))
                       (count (flatten1 chunks))))))

(defspec set-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/set))
                       (set (flatten1 chunks))))))

(defspec frequencies-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/frequencies))
                       (frequencies (flatten1 chunks))))))

(defspec some-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/some #{1}))
                       (some #{1} (flatten1 chunks))))))

(defspec any-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (let [e           (t/tesser chunks (t/any))
                      candidates  (set (flatten1 chunks))]
                  (is (or (contains? candidates e)
                          (and (empty? candidates) (nil? e)))))))

;; Predicate folds
(defspec empty?-spec
  test-count
  (prop/for-all [chunks (chunks (option gen/boolean))]
                (is (= (t/tesser chunks (t/empty?))
                       (empty? (flatten1 chunks))))))

(defspec every?-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/every? odd?))
                       (every? odd? (flatten1 chunks))))))

(defspec not-every?-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/not-every? odd?))
                       (not-every? odd? (flatten1 chunks))))))

;; Comparable folds

(defspec max-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (let [m (t/tesser chunks (t/max))]
                  (if (every? empty? chunks)
                    (nil? m)
                    (= m (reduce max (flatten1 chunks)))))))

(defspec min-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (let [m (t/tesser chunks (t/min))]
                  (if (every? empty? chunks)
                    (nil? m)
                    (= m (reduce min (flatten1 chunks)))))))

(defspec range-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (let [inputs (flatten1 chunks)]
                  (= (t/tesser chunks (t/range))
                     (if (every? empty? chunks)
                       [nil nil]
                       [(reduce min inputs) (reduce max inputs)])))))
