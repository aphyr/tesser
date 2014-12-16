(ns tesser.math-test
  (:require [clojure.test :refer :all]
            [clojure.math.numeric-tower :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]]
            [multiset.core :refer [multiset]]
            [tesser.utils :refer :all]
            [tesser.core :as t]
            [tesser.math :as m]))

(def test-count 1e2)

(deftest map-sum-test
  (is (= (->> (t/map inc)
              (m/sum)
              (t/tesser [[1 2 3] [4 5 6] []]))
         27)))

;; Utility functions
(defn approx=
  "Equal to within err fraction, or if one is zero, to within err absolute."
  ([err x y]
   (if (or (zero? x) (zero? y))
     (< (- err) (- x y) err)
     (< (- 1 err) (/ x y) (+ 1 err))))
  ([err x y & more]
   (->> more
        (cons y)
        (every? (partial approx= err x)))))

(def =ish
  "Almost equal"
  (partial approx= 1/1000))

(defn chunks
  "Given a generator for inputs, returns a generator that builds
  sequences of sequences of inputs."
  [input-gen]
  (gen/vector (gen/vector input-gen) 0 5))

; Bug in simple-check: ints don't grow that fast
(def bigger-ints (gen/sized (fn [size] (gen/resize (* size size) gen/int))))

(defn flatten1
  "Flattens a single level."
  [seq-of-seqs]
  (apply concat seq-of-seqs))

;; Numeric folds

(defspec sum-spec
  test-count
  (prop/for-all [chunks (chunks gen/int)]
                (is (= (t/tesser chunks (m/sum))
                        (reduce + 0 (flatten1 chunks))))))

(defn mean
  [coll]
  (assert (not (empty? coll)))
  (/ (reduce + coll) (count coll)))

(defspec mean-spec
  test-count
  (prop/for-all [chunks (gen/such-that (partial some not-empty)
                                       (chunks gen/int))]
                (is (== (t/tesser chunks (m/mean))
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
                (=ish (t/tesser chunks (m/variance))
                      (variance (flatten1 chunks)))))

(defspec standard-deviation-spec
  test-count
  (prop/for-all [chunks (gen/such-that (partial some not-empty)
                                       (chunks gen/int))]
                (=ish (t/tesser chunks (m/standard-deviation))
                      (sqrt (variance (flatten1 chunks))))))

(defn covariance
  [fx fy coll]
  (let [coll (filter fx (filter fy coll))]
    (if (empty? coll)
      nil
      (let [mean-x (mean (map fx coll))
            mean-y (mean (map fy coll))]
        (double (/ (reduce + (map #(* (- (fx %) mean-x)
                                      (- (fy %) mean-y))
                                  coll))
                   (count coll)))))))

(defspec covariance-spec
  test-count
  ; Take maps like {}, {:x 1}, {:x 2 :y 3} and compute covariance
  (prop/for-all [chunks (chunks (gen/map (gen/elements [:x :y]) gen/int))]
                (is (= (->> (m/covariance :x :y)
                            (t/tesser chunks))
                       (covariance :x :y (flatten1 chunks))))))

(defspec covariance-matrix-spec
  test-count
  (prop/for-all [chunks (chunks (gen/map (gen/elements [:x :y :z]) gen/int))]
                (let [inputs (flatten1 chunks)]
                  (is (= (->> (m/covariance-matrix {"x" :x "y" :y "z" :z})
                              (t/tesser chunks))
                         ; NOTE: depends on math.combinatorics order; fragile
                         ; but easy to fix.
                         (let [xy (covariance :x :y inputs)
                               xz (covariance :x :z inputs)
                               yz (covariance :y :z inputs)]
                           {["x" "y"] xy
                            ["x" "z"] xz
                            ["y" "x"] xy
                            ["y" "z"] yz
                            ["z" "x"] xz
                            ["z" "y"] yz}))))))

(defn correlation
  [fx fy coll]
  "http://mathworld.wolfram.com/CorrelationCoefficient.html"
  (let [coll (filter fx (filter fy coll))]
    (when-not (empty? coll)
      (let [xs (map fx coll)
            ys (map fy coll)
            mx (mean (map fx coll))
            my (mean (map fy coll))
            mxs (map #(- % mx) xs)
            mys (map #(- % my) ys)]
        (try
          (/ (reduce + (map * mxs mys))
             (sqrt (* (reduce + (map * mxs mxs))
                      (reduce + (map * mys mys)))))
          (catch ArithmeticException e
            nil))))))

(defspec correlation-spec
  test-count
  (prop/for-all [chunks (chunks (gen/map (gen/elements [:x :y]) gen/int))]
                (is (= (->> (m/correlation :x :y)
                            (t/tesser chunks))
                       (correlation :x :y (flatten1 chunks))))))

(defspec correlation-matrix-spec
  test-count
  (prop/for-all [chunks (chunks (gen/map (gen/elements [:x :y :z]) gen/int))]
                (let [inputs (flatten1 chunks)]
                  (is (= (->> (m/correlation-matrix {"x" :x "y" :y "z" :z})
                              (t/tesser chunks))
                         (let [xy (correlation :x :y inputs)
                               xz (correlation :x :z inputs)
                               yz (correlation :y :z inputs)]
                           {["x" "y"] xy
                            ["x" "z"] xz
                            ["y" "x"] xy
                            ["y" "z"] yz
                            ["z" "x"] xz
                            ["z" "y"] yz}))))))

(comment
(defspec t-digest-test
  1e3
  (prop/for-all [chunks
                 (->> (chunks gen/int)
                      (gen/such-that (partial some not-empty))
                      (gen/fmap (partial map (partial map #(double (/ % 3))))))]
                (let [inputs (sort (flatten1 chunks))
                      n      (count inputs)
                      digest (t/tesser chunks (m/t-digest {}))]
                  (if (= n 1)
                    ; Special case: only one input
                    (= (first inputs)
                       (m/quantile digest 0)
                       (m/quantile digest 1/3)
                       (m/quantile digest 1/2)
                       (m/quantile digest 2/3)
                       (m/quantile digest 1))
                    ; General case: lots of inputs
                    (->> inputs
                         (map-indexed (fn [i x]
                                        (let [q (/ i (dec n))]
                                          (or (<= (- x 1e-10)
                                                  (.quantile digest q)
                                                  (nth inputs (min (inc i)
                                                                   (dec n))))
                                              (prn :i i :q q :x x :actual
                                                   (m/quantile digest q))))))
                         (every? true?)))))))


(comment
(defspec t-digest-test
  1e3
  (prop/for-all [chunks
                 (->> (chunks gen/int)
                      (gen/such-that (partial some not-empty))
                      (gen/fmap (partial map (partial map #(double (/ % 3))))))]
                (let [inputs (sort (flatten1 chunks))
                      n      (count inputs)
                      digest (t/tesser chunks (m/t-digest {}))]
                  (if (= n 1)
                    ; Special case: only one input
                    (= (first inputs)
                       (m/quantile digest 0)
                       (m/quantile digest 1/3)
                       (m/quantile digest 1/2)
                       (m/quantile digest 2/3)
                       (m/quantile digest 1))
                    ; General case: lots of inputs
                    (->> inputs
                         (map-indexed (fn [i x]
                                        (let [q (/ i (dec n))]
                                          (or (<= (- x 1e-10)
                                                  (.quantile digest q)
                                                  (nth inputs (min (inc i)
                                                                   (dec n))))
                                              (prn :i i :q q :x x :actual
                                                   (m/quantile digest q))))))
                         (every? true?)))))))
