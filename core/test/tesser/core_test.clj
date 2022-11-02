(ns tesser.core-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]]
            [clojure.core.reducers :as r]
            [multiset.core :refer [multiset]]
            [tesser.utils :refer :all]
            [tesser.core :as t]))

(def test-opts {:num-tests 100
                :par 256})

(prn test-opts)

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

(defspec chunk-spec
  test-opts
  (prop/for-all [inputs (gen/vector gen/int)]
                (is (= (->> (t/map inc)
                            (t/frequencies)
                            (t/tesser (t/chunk 10 inputs)))
                       (->> inputs
                            (map inc)
                            frequencies)))))

(defspec fold-full-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/filter even?)
                            (t/fold {:reducer-identity  vector
                                     :reducer           conj
                                     :post-reducer      (comp vec rseq)
                                     :combiner-identity sorted-set
                                     :combiner          conj
                                     :post-combiner     reverse})
                            (t/tesser chunks))
                       (->> chunks
                            (map (fn [chunk]
                                   (->> chunk
                                        (filter even?)
                                        (into [])
                                        rseq
                                        vec)))
                            (into (sorted-set))
                            reverse)))))

(defspec fold-reducer-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/fold {:identity hash-set
                                     :reducer  conj
                                     :combiner into})
                            (t/tesser chunks))
                       (->> chunks flatten1 set)))))


(defspec fold-fn-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/fold +)
                            (t/tesser chunks))
                       (->> chunks flatten1 (reduce +))))))

(defspec transform-spec
  {:test-count 1000
   :par        1}
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/transform #(assoc % :combiner-identity
                                                 (constantly #{:hi})))
                            (t/set)
                            (t/tesser chunks))
                       (->> chunks flatten1 (cons :hi) set)))))

(defspec wrap-transform-spec
  {:test-count 1000
   :par        1}
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/set)
                            (t/wrap-transform #(assoc % :combiner-identity
                                                      (constantly #{:hi})))
                            (t/tesser chunks))
                       (->> chunks flatten1 (cons :hi) set)))))

(defspec map-spec
  test-opts
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

(defspec replace-spec
  test-opts
  (prop/for-all [chunks (chunks (option gen/boolean))]
                (let [subs {true nil
                            nil false
                            false true}]
                  (is (= (->> (t/replace subs)
                              (t/into (multiset))
                              (t/tesser chunks))
                         (->> chunks
                              flatten1
                              (replace subs)
                              (into (multiset))))))))

(defspec mapcat-spec
  test-opts
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
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/keep #(when (even? %) (inc %)))
                            (t/into (multiset))
                            (t/tesser chunks))
                       (->> chunks
                            flatten1
                            (keep #(when (even? %) (inc %)))
                            (into (multiset)))))))

(defspec filter-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/filter odd?)
                            (t/into (multiset))
                            (t/tesser chunks))
                       (->> chunks
                            flatten1
                            (filter odd?)
                            (into (multiset)))))))

(defspec remove-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/remove odd?)
                            (t/into (multiset))
                            (t/tesser chunks))
                       (->> chunks
                            flatten1
                            (remove odd?)
                            (into (multiset)))))))

(defspec reduce-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (->> (t/map inc)
                            (t/reduce + 0)
                            (t/tesser chunks))
                       (->> chunks
                            flatten1
                            (r/map inc)
                            (reduce + 0))))))

(defspec into-vec-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (sort (t/tesser chunks (t/into [])))
                       (sort (flatten1 chunks))))))

(defspec into-set-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/into #{}))
                       (set (flatten1 chunks))))))

(defspec take-spec
  test-opts
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
  test-opts
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
  test-opts
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

(defspec group-by-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (let [g #(mod % 3)]
                  (is (= (->> (t/group-by g)
                              (t/into (multiset))
                              (t/tesser chunks))
                         (->> (flatten1 chunks)
                              (group-by g)
                              (map (fn [[k vs]] [k (apply multiset vs)]))
                              (into {})))))))

(defspec group-by-post-reducer-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (let [g #(mod % 3)]
                  (is (= (->> (flatten1 chunks)
                              (group-by g)
                              (map-vals (partial reduce max 0)))
                         (->> (t/group-by g)
                              ; In each group, find maximum, but with a weird
                              ; intermediate datatype
                              (t/fold {:reducer
                                       (fn
                                         ([] [:secret 0])
                                         ([[_ m]] m)
                                         ([[_ m] x] [:secret (max m x)]))})
                              (t/tesser chunks)))))))


(defspec facet-spec
  test-opts
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
  test-opts
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
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/count))
                       (count (flatten1 chunks))))))

(defspec set-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/set))
                       (set (flatten1 chunks))))))

(defspec frequencies-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/frequencies))
                       (frequencies (flatten1 chunks))))))

(defspec some-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/some #{1}))
                       (some #{1} (flatten1 chunks))))))

(defspec any-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (let [e           (t/tesser chunks (t/any))
                      candidates  (->> chunks
                                       (filter seq)
                                       (map first)
                                       set)]
                  (is (or (contains? candidates e)
                          (and (empty? candidates) (nil? e)))))))

(defspec last-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (let [e          (t/tesser chunks (t/last))
                      candidates  (->> chunks
                                       (filter seq)
                                       (map last)
                                       set)]
                  (is (or (contains? candidates e)
                          (and (empty? candidates) (nil? e)))))))

;; Predicate folds
(defspec empty?-spec
  test-opts
  (prop/for-all [chunks (chunks (option gen/boolean))]
                (is (= (t/tesser chunks (t/empty?))
                       (empty? (flatten1 chunks))))))

(defspec every?-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/every? odd?))
                       (every? odd? (flatten1 chunks))))))

(defspec not-every?-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (t/not-every? odd?))
                       (not-every? odd? (flatten1 chunks))))))

;; Comparable folds

(defspec max-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (let [m (t/tesser chunks (t/max))]
                  (if (every? empty? chunks)
                    (nil? m)
                    (= m (reduce max (flatten1 chunks)))))))

(defspec min-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (let [m (t/tesser chunks (t/min))]
                  (if (every? empty? chunks)
                    (nil? m)
                    (= m (reduce min (flatten1 chunks)))))))

(defspec range-spec
  test-opts
  (prop/for-all [chunks (chunks gen/int)]
                (let [inputs (flatten1 chunks)]
                  (= (t/tesser chunks (t/range))
                     (if (every? empty? chunks)
                       [nil nil]
                       [(reduce min inputs) (reduce max inputs)])))))
