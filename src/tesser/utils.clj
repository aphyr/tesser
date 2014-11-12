(ns tesser.utils
  "Toolbox."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.math.numeric-tower :refer :all]))

(defn prepend
  "Prepends a single value to the beginning of a sequence. O(1) for sequences
  using cons, O(n) for vectors. Returns a singleton vector when coll is nil."
  [coll element]
  (cond (nil? coll)    [element]
        (vector? coll) (vec (cons element coll))
        true           (cons element coll)))

(defn append
  "Appends a single value to the end of a sequence. O(1); uses conj for
  vectors, concat for other seqs."
  [coll element]
  (if (vector? coll)
    (conj coll element)
    (concat coll (list element))))

(defn update
  "Given a map, a key, a function f, and optional args, returns a copy of map
  with that key set to (f current-val arg1 arg2 ...). Like update-in, but only
  one level deep."
  ([m k f]              (assoc m k (f (get m k))))
  ([m k f a]            (assoc m k (f (get m k) a)))
  ([m k f a b]          (assoc m k (f (get m k) a b)))
  ([m k f a b c]        (assoc m k (f (get m k) a b c)))
  ([m k f a b c & args] (assoc m k (apply f (get m k) a b c args))))

(defn poll!
  "Given an atom pointing to a sequence, pulls the first element off the
  sequence atomically and returns it."
  [a]
  (when-let [xs @a]
    (if (compare-and-set! a xs (next xs))
      (first xs)
      (recur a))))

(defn map-vals
  "Maps over a key-value map, returning a new map by transforming each value
  with (f v)."
  [f m]
  (->> m
       (reduce (fn [m [k v]]
                 (assoc! m k (f v)))
               (transient {}))
       persistent!))

(defn index-by
  "Given an indexing function f and a collection of xs, return a map of (f x)
  -> x."
  [f xs]
  (persistent!
    (reduce (fn [m x] (assoc! m (f x) x))
            (transient {})
            xs)))

(defn atanh
  "Express inverse hyperbolic tangent in terms of logarithms."
  [r]
  (/ (java.lang.Math/log (/ (+ 1 r)
                            (- 1 r)))
     2))

(defn sigma-Z
  "Compute standard error for Fisher-Z transformation."
  [n]
  (/ 1 (sqrt (- n 3))))

(defn power-r
  "Given linear corr coeff, r, and sample size n use the Fisher-Z transformation
  and its standard error to determine whether the correlation is statistically
  significant at the 5% level. If it is, return 1. Otherwise, return 0." 
  [r,n,nsd]
  (if (< n 5)
    0
    (if (#{-1,1} r)
      1
      (if (< 0 (- (abs (atanh r)) (* nsd (sigma-Z n))))
        1
        0))))

(defn path-fn
  "Takes a path for get-in and converts it to a function that operates on
  associative structures."
  [path]
  (fn [x] (get-in x path)))

; Extracting key path structure

(defn key-path-compare
  "Special comparator for building key path structures."
  [a b]
  ;  (prn a b)
  (cond
    (identical? a b) 0

    (and (sequential? a)
         (sequential? b))
    (loop [as a
           bs b]
      (let [a (first as)
            b (first bs)]
        (cond (and (nil? a) (nil? b))  0
              (nil? b)                 1
              (nil? a)                -1
              true                    (let [c (key-path-compare a b)]
                                        (if-not (zero? c)
                                          c
                                          (recur (next as) (next bs)))))))

    true
    (try (compare a b)
         (catch ClassCastException e
           (compare (name a) (name b))))))

(def key-path-set
  "Initial set for key paths"
  (sorted-set-by key-path-compare))

(defn class-sym
  "Symbol for the class of x. Nil's class sym is nil."
  [x]
  (when x
    (->> x class .getSimpleName symbol)))

(defn key-paths
  "Given a clojure object, yields a set of all paths through that structure.
  Keywords as map keys are distinct elements in a path; keys like strings or
  non-map collections are represented as symbols for their corresponding class."
  ([x] (key-paths [] x))
  ([parent-path x]
   (cond
     (or (sequential? x) (set? x))
     (->> x
          (map (partial key-paths (->> x class-sym (conj parent-path))))
          (reduce set/union key-path-set))

     (map? x)
     (->> x
          (map (fn [[k v]]
                 (if (keyword? k)
                   (key-paths (conj parent-path k) v)
                   (key-paths (->> k class-sym (conj parent-path)) v))))
          (reduce set/union key-path-set))

     true
     (->> x class-sym (conj parent-path) (conj key-path-set)))))

(defn complete-triangular-matrix
  "Given a map of [x y] keys to values, returns a map where *both* [x y] and [y
  x] point to identical values. Useful for pairwise comparisons which compute
  triangular matrices but want to return a full matrix."
  [m]
  (->> m (map (fn [[[x y] value]] [[y x] value])) (into m)))
