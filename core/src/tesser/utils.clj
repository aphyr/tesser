(ns tesser.utils
  "Toolbox."
  (:require [clojure [set :as set]
                     [string :as str]
                     [walk :as walk]]
            [clojure.core.reducers :as r]))

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

;; A mutable pair datatype, intended for use during singlethreaded reductions.
(defprotocol Pair
  (a [pair] "Returns the first element in the Pair.")
  (b [pair] "Returns the second element in the Pair.")
  (set-a! [pair a'] "Set the first element in the Pair.")
  (set-b! [pair b'] "Set the second element in the Pair.")
  (set-both! [pair a' b'] "Set both the first and second element in the pair."))

(deftype UnsafePair [^:unsynchronized-mutable a ^:unsynchronized-mutable b]
  Pair
  (a [_] a)
  (b [_] b)
  (set-a! [this a'] (set! a a') this)
  (set-b! [this b'] (set! b b') this)
  (set-both! [this a' b']
    (set! a a')
    (set! b b')
    this))

(defn unsafe-pair
  "Constructs a new unsynchronized pair object."
  ([] (UnsafePair. nil nil))
  ([a b] (UnsafePair. a b)))

(defn successive-pairs
  "A much faster version of (partition 2 1 coll) which generates vectors, not
  lazy seqs."
  ([coll] (successive-pairs (first coll) (next coll)))
  ([prev coll]
   (lazy-seq
     (when-let [s (seq coll)]
       (let [x (first s)]
         (cons [prev x] (successive-pairs x (next coll))))))))

(defn differences
  "A seq of the differences between successive elements in a collection.

    (differences [1 2 4 5 2])
    ; (1 2 1 -3)"
  [coll]
  (->> coll
       successive-pairs
       (map (fn [[x x']] (- x' x)))))

(defn cumulative-sums
  "A seq of the cumulative sums of all elements in `coll`, starting at `init`
  or the first element of `coll` if `init` is not provided. The integral to
  `differences` differential.

    (cumulative-sums 1 [1 2 1 -3])
    ; (1 2 4 5 2)"
  ([coll]
   (reductions + coll))
  ([init coll]
   (reductions + init coll)))

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

(defn path-fn
  "Takes a path for get-in and converts it to a function that operates on
  associative structures."
  [path]
  (fn [x] (get-in x path)))


(defn complete-triangular-matrix
  "Given a map of [x y] keys to values, returns a map where *both* [x y] and [y
  x] point to identical values. Useful for pairwise comparisons which compute
  triangular matrices but want to return a full matrix."
  [m]
  (->> m (map (fn [[[x y] value]] [[y x] value])) (into m)))

(defn first-non-nil-reducer
  "A reducing function that simply returns the first non-nil element in the
  collection."
  [_ x]
  (when-not (nil? x) (reduced x)))

(defmacro scred
  "Helper for short-circuiting nested reduction functions which can emit
  reduced values. Given the name of a function that could emit a reduced
  value, and an expression:

    (scred rfn [1 (rfn x y)])

  Expands to code that converts the expression to a reduced value whenever
  the underlying function emits a reduced value:

    (let [acc (rfn x y)]
      (if (reduced? acc)
        (let [acc @acc] (reduced [1 acc]))
        [1 acc]))

  scred does not interpret lexical scope, so don't rebind rfn in expr.
  Uses prewalk, so the outermost fn is where scred will cut out an expr.
  Keep this as simple as possible, haha."
  [rfn-name expr]
  (let [acc          (gensym 'acc)
        reduced-expr (promise)
        expr         (walk/prewalk (fn [form]
                                     ; Match (rfn ...)
                                     (if (and (list? form)
                                              (= rfn-name (first form)))
                                       ; Snarf the expression for later
                                       (do (assert
                                             (not (realized? reduced-expr)))
                                           (deliver reduced-expr form)
                                           acc)
                                       form))
                                   expr)
        reduced-expr @reduced-expr]
    (assert reduced-expr)
    `(let [~acc ~reduced-expr]
       (if (reduced? ~acc)
         (let [~acc (deref ~acc)] (reduced ~expr))
         ~expr))))

(defn partition-vec
  "partitions a vector into groups of n (somewhat like partition)
   but uses subvec for speed.
  (partition-vec 2 [1]) => ([1])
  (partition-vec 2 [1 2 3]) => ([1 2] [3])
  Unlike partition, won't return empty subsequences in cases like
  (partition-vec 2 [1])
  Useful for supplying vectors to tesser.core/tesser."
  ([v] (partition-vec 512 v))
  ([^long n v]
   (let [total-size (count v)]
     (loop [start 0
            end (min n total-size)
            out (transient [])]
     (let [curr (subvec v start end)]
       (if (< (- end start) n)
         (persistent! (conj! out curr))
         (recur
           end
           (min (+ end n) total-size)
           (conj! out curr))))))))
