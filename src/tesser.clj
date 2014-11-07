(ns tesser
  "\"Now we will tesser, we will wrinkle again. Do you understand?\" \"No,\"
  Meg said flatly. Mrs. Whatsit sighed. \"Explanations are not easy when they
  are about things for which your civilization still has no words. Calvin
  talked about traveling at the speed of light. You understand that, little
  Meg?\" \"Yes,\" Meg nodded. \"That, of course, is the impractical, long way
  around. We have learned to take short cuts wherever possible.\" \"Sort of
  like in math?\" Meg asked. \"Like in math.\"

  -- Madeline L'Engle, *A Wrinkle In Time*.

  Tesser structures partly-concurrent folds.

  `(tesser some-collection a-fold)` uses `a-fold` to combine some information
  from every element in `some-collection`. Like reducers and transducers, it
  takes *fold* as the ultimate collection operation. Unlike transducers, Tesser
  folds are not sequential. They include an explicitly concurrent reduction
  over arbitrary partitions of the collection, and a sequential reduction over
  *those* reduced values, termed *combine*. Like transducers, Tesser folds can
  also map and filter their inputs, and perform post-processing transformation
  after each reduction and the combine step.

  Tesser folds can be composed with the standard collection threading idioms,
  and evaluated against a collection of collections using (tesser colls fold).

  (->> (t/map inc)
       (t/filter even?)
       (t/reduce +)
       (t/tesser [[1 2 3] [4 5 6]]))
  ; => 2 + 4 + 6 = 12"
  (:refer-clojure :exclude [map keep filter remove count range frequencies into])
  (:import (com.clearspring.analytics.stream.quantile QDigest)
           (java.lang.Math))
  (:require [tesser.utils :refer :all]
            [interval-metrics.core :as metrics]
            [interval-metrics.measure :as measure]
            [clojure.math.numeric-tower :refer :all]
            [clojure.math.combinatorics :as combo]
            [clojure.core.reducers :as r]
            [clojure.pprint :refer [pprint]]
            [clojure.core :as core]
            [clojure.walk :as walk]
            [clojure.set  :as set]))

; Representing a transform
;
; We need to compose folds *out of order*--e.g. (->> (map inc) (map str))
; should increment *first*, then convert to strings, just like sequence
; operations for map and filter. So we have something like
;
; [[:map inc] [:map str]]
;
; And want to make a function that behaves like
;
; => (fn [x] (str (inc x)))
;
; This is pretty straightforward with (comp)--but breaks down when we want to
; do something like (filter).
;
; [[:filter odd?] [:map inc]]
;
; => (fn [x] (if (odd? x) (inc x)))
;
; We need the filter to be able to *choose* whether to invoke the next
; transformation.  Comp is no longer appropriate. Instead, we'll represent each
; transformation by a *function* that takes the next transformation and
; generates a wrapping transformation. This allows us to compose
; transformations in *any* order, then compile them by invoking each function
; with its successor in reverse order.
;
; [(fn [next-transform] (fn [x] (if (odd? x) (next-transform x))))
;  (fn [next-transform] (fn [x] (next-transform (inc x))))]
;
; But map and filter are both stateless transformations. We want to bring some
; *state* along the fold: an accumulator, as used by a reducing function. `map`
; and `filter` ignore their accumulators:
;
; [(fn [nt] (fn [acc x] (if (odd? x) (next-transform acc x))))
;  (fn [nt] (fn [acc x] (next-transform acc (inc x))))]
;
; But a stateful transformation like (take 5) requires that we track *two*
; pieces of accumulator state: our *own* accumulator, and the downstream
; accumulator:
;
; (fn take [nt]
;   (fn [[remaining nt-acc] x]
;     (if (pos? remaining)
;       [(dec remaining) (nt nt-acc x)]
;       (reduced [remaining nt-acc]))))
;
; Note, however, that we do *not* have to be aware of the downstream
; transformation's downstream accumulators--we only need to track our own
; state, and the state for the next transformation. There's a problem though:
; this transform returns [0 nt-return-value], when we just wanted
; nt-return-value. We need a way to transform the intermediate accumulator into
; some final value. We'll return a *map* of two functions: one for reducing,
; and one to transform the final accumulator:
;
; (fn take [nt]
;   {:reduce (fn [[remaining nt-acc] x]
;              (if (pos? remaining)
;                [(dec remaining) (nt nt-acc x)]
;                (reduced [remaining nt-acc])))
;    :complete (fn [[remaining nt-acc]]
;                nt-acc)})
;
; Clojure's transducers call this a *completing* function, and instead of a
; map, encodes it as an alternate, single-arity variant of the reducing
; function.
;
; (fn take [nt]
;   (fn [[remaining nt-acc]]
;     nt-acc)
;   (fn [[remaining nt-acc] x]
;     (if (pos? remaining)
;       [(dec remaining) (nt nt-acc x)]
;       (reduced [remaining nt-acc]))))
;
; One more piece of the reduce puzzle: we have to encode the *initial* value
; for the accumulator into the transformation as well. In transducers, this is
; encoded as a zero-arity variant of the function. We'll wrap it in *another*
; function to specify the limit for (take).
;
; (fn take [limit]
;   (fn [nt]
;     (fn [] [limit (nt)])
;     (fn [[remaining nt-acc]]
;       nt-acc)
;     (fn [[remaining nt-acc] x]
;       (if (pos? remaining)
;         [(dec remaining) (nt nt-acc x)]
;         (reduced [remaining nt-acc])))))
;
; Unlike transducers, we need to generalize to *concurrent* reductions. We need
; to combine the results of multiple reduce phases--and we may not be able to
; use the 2-arity reduce function again. We need *another* two-arity function.
; One option might be to use a map:
;
; (fn take [limit]
;   (fn [nt]
;     {:identity (fn [] [limit ((:identity nt))])
;     ...}))
;
; Or we could use a protocol:
;
; (fn take [limit]
;   (fn [nt]
;     (reify Tesser
;       (identity [_] [limit (identity nt)])
;       (reduce [_ [remaining nt-acc] input]
;         (if (pos? remaining)
;           [(dec remaining) (reduce nt nt-acc)]
;           (reduced [remaining nt-acc])))
;       (complete [_ [remaining nt-acc]] nt-acc))))
;
; The map version is less succinct and does not enforce type constraints, but
; is easier to compose: we can just use assoc and update to create derivative
; streams without adding boilerplate wrappers. The map version also has
; function signatures suitable for passing directly to (clojure.core/reduce),
; whereas the protocol variant requires a wrapper: (clojure.core/reduce (fn
; [acc input] (.reduce fold acc input))).
;
; Generalizing (take) to a concurrent context is hard if we don't know the
; collection sizes in advance, but we can relax it slightly to take-at-most,
; which reduces over at most n elements from the underlying collection. Note
; that there is no ordering guarantee about *which* n elements will be
; selected, because reduction happens concurrently.
;
; So, the general form of a tesser transform is:
;
; (fn take-at-most [limit]
;   (fn [{:keys [identity reducer post-reducer combiner post-combiner]}]
;     {:identity     (fn [] [0 (identity)])
;      :reducer      (fn [[count acc] input]
;                      (if (< count limit)
;                        [(inc count) (reducer acc input)]
;                        (reduced [count acc])))
;      :post-reducer (fn [[count acc]] [count (post-reducer acc)])
;      :combiner     (fn [[count1 acc1] [count2 acc2]]
;                      (let [count (+ count1 count2)]
;                        ; Only fold if it wouldn't put us over the limit
;                        ; Not ideal, but you get the point
;                        (if (<= count limit)
;                          [count (combiner acc1 acc2)]
;                          [count1 acc1])))
;      :post-combiner (comp post-combiner second)}))
;
; ## Composing transforms
;
; A *fold* is a sequence of transforms which can be compiled together into a
; single pass over the underlying dataset. We write them literally as a
; sequence:
;
; [(fn take-at-most ...) (fn map ...) (fn sum ...)]
;
; When composing folds, it'd be nice if we could build them up *piecewise*,
; like sequences, and pass them around. Composition with (comp) only allows us
; to *wrap* transformations around the outside, but we'd like to apply
; transformations to the *inside* of the fold. For example:
;
; (def dataset       (t/map json/parse-string))
; (def subject-words (->> dataset
;                         (t/map :subject)
;                         (t/mapcat split-words)
;                         (t/frequencies))
;
; This pattern is exactly what you'd write with the Clojure sequence API. But
; notice that we need to call parse-string *then* :subject *then* split-words.
; What we *have* is backwards: (t/mapcat split-words (t/map :subject)). So we
; *defer* compilation of the fold, representing it as a sequence until we're
; ready to invoke it.
;
; Therefore, functions in this namespace like (map), (filter), etc take
; *sequences* of transforms and return sequences of transforms. If no sequence
; is provides, they construct a new single-element sequence.

;; Compiling and executing folds

(defn assert-compiled-fold
  "Is this a valid compiled fold?"
  [f]
  (assert (fn? (:identity f)) (str (pr-str f) " is missing an :identity fn"))
  (assert (fn? (:reducer f))  (str (pr-str f) " is missing a :reducer fn"))
  (assert (fn? (:post-reducer f))
          (str (pr-str f) " is missing a :post-reducer fn"))
  (assert (fn? (:combiner f)) (str (pr-str f) " is missing a :combiner fn"))
  (assert (fn? (:post-combiner f))
          (str (pr-str f) " is missing a :post-combiner fn"))
  f)

(defn compile-fold
  "Compiles a fold (a sequence of transforms, each represented as a function
  taking the next transform) to a single map like

  {:identity (fn [] ...),
   :reducer  (fn [acc x] ...)
   ...}"
  [fold]
  (->> fold
       reverse
       (reduce (fn [compiled adhere] (adhere compiled))
               nil)
       assert-compiled-fold))

(defn tesser
  "Compiles a fold and applies it to a sequence of sequences of inputs. Runs
  num-procs * 2 threads for the parallel (reducer) portion of the fold."
  [seqs fold]
  (let [fold    (compile-fold fold)
        t0      (System/nanoTime)
        threads (* 2 (.. Runtime getRuntime availableProcessors))
        queue   (atom seqs)
        results (atom [])
        chunks  (atom 0)
        stats   (measure/periodically 1 (println @chunks "chunks read"))]
    (try
      (let [workers (->> threads
                         core/range
                         (core/map
                           (fn worker [i]
                             (future
                               (while
                                 (when-let [seq (poll! queue)]
                                   (let [result (->> seq
                                                     (reduce (:reducer fold)
                                                             ((:identity fold)))
                                                     ((:post-reducer fold)))]
                                     (swap! results conj result)
                                     (swap! chunks inc)
                                     true)))))))]
        (try
          ; Wait for workers
          (core/mapv deref workers)

          ; Stop printing stats
          (stats)

          (let [out   (->> results
                           deref
                           (reduce (:combiner fold) ((:identity fold)))
                           ((:post-combiner fold)))
                t1    (System/nanoTime)]
            (when (< 1e8 (- t1 t0))
              (println (format "Time: %.2f seconds" (/ (- t1 t0) 1e9))))
            out)

          (finally
            ; Ensure workers are dead
            (core/mapv future-cancel workers))))

      (finally
        ; Stop printing stats
        (stats)

        ; Force seq realization so we can close filehandles
        (dorun seqs)))))


; Defining transforms

(defmacro deftransform
  "We're trying to build functions that look like...

    (defn map
      \"Takes a function `f` and an optional fold. Returns a version of the
      fold which finally calls (f element) to transform each element.\"
      ([f]
       (map f []))
      ([f fold]
       (append fold
               (fn adhere [{:keys [reducer] :as downstream}]
                 (assoc downstream :reducer
                        (fn reducer [acc input] (reducer acc (f input)))))))))

  Which involves a fair bit of shared boilerplate: the single-arity variant of
  the transform, the append/prepend logic, the annealing function and its
  destructuring bind, etc. We'll wrap these up in an anaphoric macro called
  `deftransform`. Within the body, `identity-`, `reducer-`, `post-reducer-`,
  `combiner-`, `post-combiner-` are all bound to the downstream transform's
  component functions, and `downstream` is bound to the downstream transform
  itself."
  [name docstring args & body]
  `(defn ~name ~docstring
     ; Version without fold argument
     ([~@args] (~name ~@args []))
     ; Version with fold argument
     ([~@args fold#]
      (append fold#
              (fn adhere [~'downstream]
                (let ~'[identity-      (:identity downstream)
                        reducer-       (:reducer downstream)
                        post-reducer-  (:post-reducer downstream)
                        combiner-      (:combiner downstream)
                        post-combiner- (:post-combiner downstream)]
                  ~@body))))))

;; General transformations

(deftransform map
  "Takes a function `f` and an optional fold. Returns a version of the fold
  which finally calls (f input) to transform each element."
  [f]
  (assoc downstream :reducer (fn reducer [acc x]
                               (reducer- acc (f x)))))

(deftransform keep
  "Takes a function `f` and an optional fold. Returns a version of the fold
  which finally calls (f input) to transform each element, and passes it on to
  subsequent transforms only when the result of (f input) is truthy."
  [f]
  (assoc downstream :reducer (fn reducer [acc x]
                               (if-let [x' (f x)]
                                 (reducer- acc x')
                                 acc))))

(deftransform filter
  "Takes a predicate function `pred` and an optional fold. Returns a version of
  the fold which only passes on inputs to subsequent transforms when (pred
  input) is truthy."
  [pred]
  (assoc downstream :reducer (fn reducer [acc x]
                               (if (pred x)
                                 (reducer- acc x)
                                 acc))))

(deftransform remove
  "Takes a predicate function `pred` and an optional fold. Returns a version of
  the fold which only passes on inputs to subsequent transforms when (pred
  input) is nil or false."
  [pred]
  (assoc downstream :reducer (fn reducer [acc x]
                               (if (pred x)
                                 acc
                                 (reducer- acc x)))))

(deftransform into
  "Adds all inputs to the given collection using conj. Ordering of elements
  from distinct chunks is undefined.

  TODO: distinct identities for reducer and combiner would allow us to conj
  into (empty coll) in reducer, then start with coll for combine. Doesn't
  really save much for injective collections, but for sets/maps could be more
  efficient."
  [coll]
  (assert (nil? downstream))
  {:identity      vector
   :reducer       conj
   :post-reducer  identity
   :combiner      core/concat
   :post-combiner (partial core/into coll)})

;; Splitting folds

(deftransform facet
  "Your inputs are maps, and you want to apply a fold to each value
  independently. Facet generalizes a fold over a single value to operate on
  maps of keys to those values, returning a map of keys to the results of the
  fold over all values for that key. Each key gets an independent instance of
  the fold.

  For instance, say you have inputs like

  {:x 1, :y 2}
  {}
  {:y 3, :z 4}

  Then the fold

  (->> (facet)
       (mean))

  returns a map for each key's mean value:

  {:x 1, :y 2, :z 4}"
  []
  {:identity      (constantly {})
   :reducer       (fn reducer [acc m]
                    ; Fold value m into accumulator map
                    (reduce (fn [acc [k v]]
                              ; Fold in each kv pair in m
                              (assoc acc k
                                     (reducer-
                                       ; TODO: only invoke downstream identity
                                       ; when necessary
                                       (get acc k (identity-)) v)))
                            acc
                            m))
   :post-reducer  identity
   :combiner      (fn combiner [m1 m2]
                    (merge-with combiner- m1 m2))
   :post-combiner (fn post-combiner [m]
                    (map-vals post-combiner- m))})

(deftransform fuse
  "You've got several folds, and want to execute them in one pass. Fuse is the
  function for you! It takes a map from keys to folds, like

    (->> (map parse-person)
         (fuse {:age-range    (->> (map :age) (range))
                :colors-prefs (->> (map :favorite-color) (frequencies))})
         (tesser people))

  And returns a map from those same keys to the results of the corresponding
  folds:

    {:age-range   [0 74],
     :color-prefs {:red        120
                   :blue       312
                   :watermelon 1953
                   :imhotep    1}}

  Note that this fold only invokes `parse-person` once for each record, and
  completes in a single pass. If we ran the age and color folds independently,
  it'd take two passes over the dataset--and require parsing every person
  *twice*.

  Fuse and facet both return maps, but generalize over different axes. Fuse
  applies a fixed set of *independent* folds over the *same* inputs, where
  facet applies the *same* fold to a dynamic set of keys taken from the
  inputs.

  Note that fuse compiles the folds you pass to it, so you need to build them
  completely *before* fusing. The fold `fuse` returns can happily be combined
  with other transformations at its level, but its internal folds are sealed
  and opaque."
  [fold-map]
  (assert (nil? downstream))
  (let [ks             (vec (keys fold-map))
        folds          (mapv (comp compile-fold (partial get fold-map)) ks)
        reducers       (core/map :reducer folds)
        combiners      (core/map :combiner folds)]
    ; We're gonna project into a particular key basis vector for the
    ; reduce/combine steps
    {:identity      (if (empty? fold-map)
                       (constantly []) ; juxt can't take zero args
                       (apply juxt (core/map :identity folds)))
      :reducer       (fn reducer [accs x]
                       (mapv (fn [f acc] (f acc x))
                             reducers accs))
      :post-reducer  identity
      :combiner      (fn combiner [accs1 accs2]
                       (mapv (fn [f acc1 acc2] (f acc1 acc2))
                             combiners accs1 accs2))
     ; Then inflate the vector back into a map
     :post-combiner (comp (partial zipmap ks)
                          ; After having applied the post-combiners
                          (fn post-com [xs] (mapv (fn [f x] (f x))
                                                  (core/map :post-combiner
                                                            folds)
                                                  xs)))}))

;; Numeric folds

(deftransform sum
  "Finds the sum of numeric elements."
  []
  (assert (nil? downstream))
  {:identity      (constantly 0)
   :reducer       +
   :post-reducer  core/identity
   :combiner      +
   :post-combiner core/identity})

(deftransform mean
  "Finds the arithmetic mean of numeric elements."
  []
  (assert (nil? downstream))
  {:identity      (constantly [0 0])
   :reducer       (fn reducer [[s c] x]
                    [(+ s x) (inc c)])
   :post-reducer  identity
   :combiner      (fn combiner [x y] (core/map + x y))
   :post-combiner (fn post-combiner [x]
                    (double (/ (first x) (max 1 (last x)))))})

