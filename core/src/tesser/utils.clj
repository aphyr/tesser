(ns tesser.utils
  "Toolbox."
  (:import (java.lang.reflect Array))
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
  "Constructs a new unsynchronized mutable pair object, suitable for
  single-threaded mutation."
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
  "A seq of the differences between successive elements in a collection. For
  example,

      (differences [1 2 4 5 2])
      ; (1 2 1 -3)"
  [coll]
  (->> coll
       successive-pairs
       (map (fn [[x x']] (- x' x)))))

(defn cumulative-sums
  "A seq of the cumulative sums of all elements in `coll`, starting at `init`
  or the first element of `coll` if `init` is not provided. If `differences`
  provides differentials, `cumulative-sums` provides integrals.

      (cumulative-sums 1 [1 2 1 -3])
      ; (1 2 4 5 2)"
  ([coll]
   (reductions + coll))
  ([init coll]
   (reductions + init coll)))

(defmacro unless-update [body]
  (when (neg? (compare [1 7 0] (mapv (partial get *clojure-version*) [:major :minor :incremental])))
    body))

(unless-update
  (defn update
    "Given a map, a key, a function f, and optional args, returns a copy of map
    with that key set to (f current-val arg1 arg2 ...). Like update-in, but only
    one level deep."
    ([m k f]              (assoc m k (f (get m k))))
    ([m k f a]            (assoc m k (f (get m k) a)))
    ([m k f a b]          (assoc m k (f (get m k) a b)))
    ([m k f a b c]        (assoc m k (f (get m k) a b c)))
    ([m k f a b c & args] (assoc m k (apply f (get m k) a b c args)))))

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

(defn var->sym
  "Converts a var to fully qualified symbol."
  [^clojure.lang.Var v]
  (symbol (name (.name (.ns v))) (name (.sym v))))

(defn complete-triangular-matrix
  "Given a map of `[x y]` keys to values, returns a map where *both* `[x y]`
  and `[y x]` point to identical values. Useful for pairwise comparisons which
  compute triangular matrices but want to return a full matrix."
  [m]
  (->> m (map (fn [[[x y] value]] [[y x] value])) (into m)))

(defn first-non-nil-reducer
  "A reducing function that simply returns the first non-nil element in the
  collection."
  [_ x]
  (when-not (nil? x) (reduced x)))

(defn reduce-first
  "clojure.core/first, but for for reducibles."
  [reducible]
  (reduce (fn [_ x] (reduced x)) nil reducible))

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

(defmacro def-type-predicate
  "Takes an instance of an object and defines a function that tests an object
  to see if its class is an instance of the exemplar's."
  [name exemplar]
  `(let [c# (class ~exemplar)]
    (defn ~name [x#] (instance? c# x#))))

(def-type-predicate bytes?    (byte-array 0))
(def-type-predicate shorts?   (short-array 0))
(def-type-predicate ints?     (int-array 0))
(def-type-predicate longs?    (long-array 0))
(def-type-predicate floats?   (float-array 0))
(def-type-predicate doubles?  (double-array 0))
(def-type-predicate objects?  (object-array 0))

(defmacro reducible-slice
  "A reducible slice of an indexed collection. Expands into a reified
  CollReduce which uses `(getter coll ... i)` to return the `i`th element.
  Defined as a macro so we can do primitive agets, which are waaaay faster for
  arrays. Slice will have maximum length n, and starts at index i0."
  [getter coll length offset]
  `(reify
     clojure.core.protocols/CollReduce

     (coll-reduce [this# f#]
       (clojure.core.protocols/coll-reduce this# f# (f#)))

     (coll-reduce [_ f# init#]
       (let [length#  (long ~length)
             offset#  (long ~offset)
             i-final# (dec (min (count ~coll) (+ offset# length#)))]
       (loop [i#    offset#
              acc#  init#]
         (let [acc# (f# acc# (~getter ~coll i#))]
           (if (or (= i# i-final#)
                   (reduced? acc#))
             acc#
             (recur (inc i#) acc#))))))))

; Slices over primitive arrays

(defn reducible-slice-bytes
  [^bytes ary chunk-size offset]
  (reducible-slice aget ary chunk-size offset))

(defn reducible-slice-shorts
  [^shorts ary chunk-size offset]
  (reducible-slice aget ary chunk-size offset))

(defn reducible-slice-ints
  [^ints ary chunk-size offset]
  (reducible-slice aget ary chunk-size offset))

(defn reducible-slice-longs
  [^longs ary chunk-size offset]
  (reducible-slice aget ary chunk-size offset))

(defn reducible-slice-floats
  [^floats ary chunk-size offset]
  (reducible-slice aget ary chunk-size offset))

(defn reducible-slice-doubles
  [^doubles ary chunk-size offset]
  (reducible-slice aget ary chunk-size offset))

(defn reducible-slice-objects
  [^objects ary chunk-size offset]
  (reducible-slice aget ary chunk-size offset))

(defn chunk-array
  "Partitions an array into reducibles of size `chunk-size` (like
  chunk), but faster."
  ([^long chunk-size ary]
   (let [slicer (cond
                  (bytes? ary)    reducible-slice-bytes
                  (shorts? ary)   reducible-slice-shorts
                  (ints? ary)     reducible-slice-ints
                  (longs? ary)    reducible-slice-longs
                  (floats? ary)   reducible-slice-floats
                  (doubles? ary)  reducible-slice-doubles
                  (objects? ary)  reducible-slice-objects)]
     (->> (range 0 (count ary) chunk-size)
          (map (partial slicer ary chunk-size))))))

(defn chunk-vec
  "Partitions a vector into reducibles of size n (somewhat like partition-all)
  but uses subvec for speed.

      (chunk-vec 2 [1])     ; => ([1])
      (chunk-vec 2 [1 2 3]) ; => ([1 2] [3])

  Useful for supplying vectors to tesser.core/tesser."
  ([^long n v]
   (let [c (count v)]
     (->> (range 0 c n)
          (map #(subvec v % (min c (+ % n))))))))

(defn reducible-chunk
  "Like partition-all, but only emits reducibles. Faster for vectors and
  arrays. May return chunks of any reducible type. Useful for supplying colls
  to tesser.

      (->> [1 2 3 4 5 6 7 8]
           (chunk 2)
           (map (partial into [])))
      ; => ([1 2] [3 4] [5 6] [7 8])"
  [^long n coll]
  (cond
    (vector? coll)           (chunk-vec n coll)
    (.isArray (class coll))  (chunk-array n coll)
    true                     (partition-all n coll)))

(defn maybe-unary
  "Not all functions used in `tesser/fold` and `tesser/reduce` have a
  single-arity form. This takes a function `f` and returns a fn `g` such that
  `(g x)` is `(f x)` unless `(f x)` throws ArityException, in which case `(g
  x)` returns just `x`."
  [f]
  (fn wrapper
    ([] (f))
    ([x] (try
           (f x)
           (catch clojure.lang.ArityException e
             x)))
    ([x y] (f x y))
    ([x y & more] (apply f x y more))))
