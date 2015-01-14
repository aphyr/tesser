(ns tesser.core
  "The essential folds: `map`, `mapcat`, `take`, `filter`, `some`, `any?`,
  `into`, etc, plus common fold combinators.

  > \"Now we will tesser, we will wrinkle again. Do you understand?\" \"No,\"
  > Meg said flatly. Mrs. Whatsit sighed. \"Explanations are not easy when they
  > are about things for which your civilization still has no words. Calvin
  > talked about traveling at the speed of light. You understand that, little
  > Meg?\" \"Yes,\" Meg nodded. \"That, of course, is the impractical, long way
  > around. We have learned to take short cuts wherever possible.\" \"Sort of
  > like in math?\" Meg asked. \"Like in math.\"
  >
  > -- Madeline L'Engle, *A Wrinkle In Time*.

  Tesser structures partly-concurrent folds.

  `(tesser some-collections a-fold)` uses `a-fold` to combine some information
  from every element in `some-collections`: a collection of collections of
  inputs. Like reducers and transducers, it takes *fold* as the ultimate
  collection operation. Unlike transducers, Tesser folds are not sequential.
  They include an explicitly concurrent reduction over arbitrary partitions of
  the collection, and a sequential reduction over *those* reduced values,
  termed *combine*. Like transducers, Tesser folds can also map and filter
  their inputs, and perform post-processing transformation after each reduction
  and the combine step.

  Tesser folds can be composed with the standard collection threading idioms,
  and evaluated against a collection of collections using `(tesser colls
  fold)`.

      (->> (t/map inc)
           (t/filter even?)
           (t/reduce +)
           (t/tesser [[1 2 3] [4 5 6]]))
      ; => 2 + 4 + 6 = 12"
  (:refer-clojure :exclude [map mapcat keep filter remove count min max range
                            frequencies into set some take empty? every?
                            not-every? replace group-by reduce])
  (:require [tesser.utils :refer :all]
            [interval-metrics.core :as metrics]
            [interval-metrics.measure :as measure]
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
       (core/reduce (fn [compiled build] (build compiled))
                    nil)
       assert-compiled-fold))

(defn tesser
  "Compiles a fold and applies it to a sequence of sequences of inputs. Runs
  num-procs threads for the parallel (reducer) portion of the fold. Reducers
  take turns combining their results, which prevents unbounded memory
  consumption by the reduce phase."
  [seqs fold]
  (let [fold     (compile-fold fold)
        t0       (System/nanoTime)
        threads  (.. Runtime getRuntime availableProcessors)
        queue    (atom seqs)
        combiner (fn combiner [combined x]
                   (if (reduced? combined)
                     combined
                     ((:combiner fold) combined x)))
        combined (atom ((:identity fold)))
        chunks   (atom 0)]
;        stats    (measure/periodically 1 (println @chunks "chunks processed"))]
    (try
      (let [workers (->> threads
                         core/range
                         (core/map
                           (fn worker [i]
                             (future
                               (while
                                 (when-let [seq (poll! queue)]
                                   ; Concurrent reduction
                                   (let [result (->> seq
                                                     (core/reduce
                                                       (:reducer fold)
                                                       ((:identity fold)))
                                                     ((:post-reducer fold)))]

                                     ; Sequential combine phase
                                     (let [combined'
                                           (locking combined
                                             (swap! combined combiner result))]

                                         ; Update stats
;                                         (swap! chunks inc)

                                         ; Abort early if reduced.
                                         (not (reduced? combined'))))))))))]
        (try
          ; Wait for workers
          (core/mapv deref workers)

          ; Stop printing stats
;          (stats)

          (let [combined @combined
                ; Unwrap reduced
                combined (if (reduced? combined) @combined combined)

                ; Postcombine
                result ((:post-combiner fold) combined)

                t1    (System/nanoTime)]
;            (when (< 1e8 (- t1 t0))
;              (println (format "Time: %.2f seconds" (/ (- t1 t0) 1e9))))
            result)

          (finally
            ; Ensure workers are dead
            (core/mapv future-cancel workers))))

      (finally
        ; Stop printing stats
;        (stats)

        ; Force seq realization so we can close filehandles
        (dorun seqs)))))

; Defining transforms

(defmacro deftransform*
  "We're trying to build functions that look like...

      (defn map
        \"Takes a function `f` and an optional fold. Returns a version of the
        fold which finally calls (f element) to transform each element.\"
        ([f]
         (map f []))
        ([f fold]
         (append fold
                 (fn build [{:keys [reducer] :as downstream}]
                   (assoc downstream :reducer
                          (fn reducer [acc input] (reducer acc (f input)))))))))

  Which involves a fair bit of shared boilerplate: the single-arity variant of
  the transform, the append/prepend logic, the annealing function and its
  destructuring bind, etc. We'll wrap these up in an anaphoric macro called
  `deftransform`, which takes a function (e.g. `append`) to conjoin this
  transform with the fold. Within the body, `identity-`, `reducer-`,
  `post-reducer-`, `combiner-`, `post-combiner-` are all bound to the
  downstream transform's component functions, and `downstream` is bound to the
  downstream transform itself."
  [conjoiner name docstring args & body]
  `(defn ~name ~docstring
     ; Version without fold argument
     ([~@args] (~name ~@args []))
     ; Version with fold argument
     ([~@args fold#]
      (~conjoiner fold#
                  (fn build [~'downstream]
                    (let ~'[identity-      (:identity downstream)
                            reducer-       (:reducer downstream)
                            post-reducer-  (:post-reducer downstream)
                            combiner-      (:combiner downstream)
                            post-combiner- (:post-combiner downstream)]
                      ~@body))))))

(defmacro deftransform
  "Deftransform, assuming transforms should be appended to the end of the fold;
  e.g. innermost."
  [& args]
  `(deftransform* append ~@args))

(defmacro defwraptransform
  "Like deftransform, but prepends the given transform to the beginning of the
  fold; e.g. outermost."
  [& args]
  `(deftransform* prepend ~@args))

;; General transformations

(deftransform map
  "Takes a function `f` and an optional fold. Returns a version of the fold
  which finally calls (f input) to transform each element."
  [f]
  (assoc downstream :reducer (fn reducer [acc x]
                               (reducer- acc (f x)))))

(defn replace
  "Given a map of replacement pairs, maps any inputs which are keys in the map
  to their corresponding values. Leaves unrecognized inputs alone.

      (->> (t/replace {:x false})
           (t/into [])
           (t/tesser [[:x :y]]))
      ; => [false :y]"
  [m & [f]]
  (->> f
       (map (fn sub [x] (get m x x)))))

(deftransform mapcat
  "Takes a function `f` and an optional fold. Returns a version of the fold
  which finally calls (f input) to transform each element. (f input) should
  return a *sequence* of inputs which will be fed to the downstream transform
  independently."
  [f]
  (assoc downstream :reducer (fn reducer [acc input]
                               (core/reduce reducer- acc (f input)))))

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

(deftransform take
  "Like clojure.core/take, limits the number of inputs passed to the downstream
  transformer to exactly n, or if fewer than n inputs exist in total, all
  inputs.

  Space complexity note: take's reducers produce log2(chunk-size) reduced
  values per chunk, ranging from 1 to chunk-size/2 inputs, rather than a single
  reduced value for each chunk. See the source for *why* this is the case."
  [n]
  ; This is a little complicated.
  ;
  ; If a chunk has n more more inputs, our job is simple: simply stop
  ; reduction after that many inputs have accrued, and return the reduced
  ; result. Any one of these reduced values will suffice for the final value.
  ;
  ; |------|
  ; [x x x x .] [. . . .] [. . .]
  ;
  ; If a chunk has fewer than n inputs, we may need to combine reduced values
  ; from more than one collection. In the limiting case, all chunks together
  ; have fewer than n inputs, and we simply combine the final reductions from
  ; each chunk.
  ;
  ; |---------------------------------|
  ; [x x x] [x x x x] [x x]
  ;
  ; What if there are enough inputs altogether to reach n, but no single chunk
  ; has enough?
  ;
  ; |---------------------------------|
  ; [x . . . . . .] [x . . . . . . . . . .]
  ; [. x x . . . .] [. x x . . . . . . . .]
  ; [. . . x x x x] [. . . x x x x . . . .]
  ;                 [. . . . . . . x x x x]
  ;
  ; For each chunk we've computed a sequence of intermediate reductions over
  ; disjoint powers-of-two inputs, capped to the size of the chunk. Because the
  ; set of inputs to any pair of chunks are disjoint, any combination of these
  ; reductions is valid.
  ;
  ; Now the question is, can we find a combination which covers n inputs? Or,
  ; equivalently, can we express any number up to the total number of inputs as
  ; the sum of some subset of a set of integers like:
  ;
  ; [1 2 4 8 (1 <= x1 <= 16)] [1 2 4 8 16 (1 <= x2 <= 32)] ...
  ;
  ; where x1, x2, ... are the remainder reductions from the end of each chunk.
  ;
  ; It is sufficient to show that we can reduce any individual chunk to an
  ; arbitrary number of inputs.
  ;
  ; [1 2 4 8 ... b (0 < x < b)]
  ;
  ; Every number from 0 to 2b - 1 is expressible as a sum of the powers of
  ; two--all numbers excluding x. This is a straightforward binary
  ; representation.
  ;
  ; 0      []
  ; 1      [1]
  ; 2      [2]
  ; 3      [1 2]
  ; ...
  ; 2b - 1 [1 2 4 8 ... b]
  ;
  ; What about the numbers from 2b to 2b - 1 + x? Well every one of *those*
  ; numbers is of the form y + x, where y <= 2b - 1. We have x, and we also
  ; know from the preceding binary argument that we can express every number
  ; from 0 to 2b-1 as a combination of the remaining elements.
  ;
  ; Therefore we can express a reduction over any number of inputs up to the
  ; size of the chunk using only that chunk. By extension, we can express a
  ; reduction over any number of inputs up to n.
  ;
  ; Because we know each chunk's reductions can independently express any
  ; number of inputs up to the chunk size, we can process each chunk's
  ; reductions in the combiner independently, instead of retaining extra state
  ; from chunk to chunk. We simply fold in the largest reduction possible
  ; without blowing past n, and when a chunk is exhausted, move to the next.
  ;
  ; As our intermediate data structure for reduce, we'll take a list of
  ; (input-count, reduced-state, input-count-2, reduced-state-2, ...)
  ; largest-to-smallest order.
  [n]
  (assert (not (neg? n)))
  {:identity      (fn identity [] (list 0 (identity-)))

   :reducer       (fn reducer [[c acc & finished :as reductions] input]
                    ; TODO: limit to n
                    (if (zero? c)
                      ; Overwrite the 0 pair
                      (scred reducer-
                             (list 1 (reducer- acc input)))
                      (let [limit (Math/pow 2 (dec (/ (core/count reductions) 2)))]
                        (if (<= limit c)
                          ; We've filled this chunk; proceed to the next.
                          (scred reducer-
                                 (cons 1 (cons (reducer- (identity-) input)
                                               reductions)))
                          ; Chunk ain't full yet; keep going.
                          (scred reducer-
                                 (cons (inc c) (cons (reducer- acc input)
                                                     finished)))))))

   :post-reducer (fn post-reducer [reductions]
                   (->> reductions
                        (partition 2)
                        (core/mapcat (fn [[n acc]] (list n (post-reducer-
                                                             acc))))))

   :combiner     (fn combiner [outer-acc reductions]
                   (let [acc' (core/reduce (fn merger [acc [c2 x2]]
                                        (let [[c1 x1] acc]
                                          (if (= n c1)
                                            ; Done
                                            (reduced acc)
                                            ; OK, how big would we get if we
                                            ; merged x2?
                                            (let [c' (+ c1 c2)]
                                              (if (< n c')
                                                ; Too big; pass
                                                acc
                                                ; Within bounds; take it!
                                                (scred combiner-
                                                       (list c' (combiner-
                                                                  x1 x2))))))))
                                      outer-acc
                                      (partition 2 reductions))]

                     ; Break off as soon as we have n elements
                     (if (= n (first acc'))
                       (reduced acc')
                       acc')))

   :post-combiner (comp post-combiner- second)})

(deftransform fold
  "Like `reducers/fold`, turns a combiner and a reducer function into a fold.
  Unlike `reducers/fold`, this `fold` doesn't take a collection: it just
  returns a fold which can be applied to a collection via `tesser`. Why?  You
  might want to compose the fold with something else using `fuse`, map it with
  `post-combine`, etc etc.

  Follows the clojure reducers and transducers conventions for arities:

  - `(f)` returns a new identity element for the reduce/combine phase
  - `(f acc input)` folds elements in the reduce/combine phases.
  - `(f acc)` post-reduces/post-combines, unless `(f acc)` throws
    clojure.lang.ArityException, in which case we return `acc` directly.

  `tesser.core/fold` should be a straightforward replacement for
  `clojure.core.reducers/fold`, except you'll do the chunking yourself:

      (->> (t/fold set/union                ; combine via set union
                   (fn ([] #{})             ; identity is empty set
                       ([s x] (conj s x)))) ; conj into set
           (t/tesser [[1 2 3] [4 5 6]]))
      ; => #{1 4 6 3 2 5}"
  [combinef reducef]
  (assert (nil? downstream))
  {:identity      reducef
   :reducer       reducef
   :post-reducer  (maybe-unary reducef)
   :combiner      combinef
   :post-combiner (maybe-unary combinef)})

(deftransform reduce
  "A fold that uses the same function for the reduce and combine phases. Unlike
  normal Clojure reduce, this reduce doesn't take a collection: it just returns
  a fold which can be applied to a collection via `tesser`. Why? You might want
  to compose the reduction with something else using `fuse`, map it with
  `post-combine`, etc etc.

  Follows the clojure reducers and transducers conventions for arities:

  - `(constantly init)` is used to generate identity elements.
  - `(f acc input)` folds elements in the reduce and combine phases.
  - `(f acc)` post-reduces and post-combines, unless `(f acc)` throws
    clojure.lang.ArityException, in which case we return `acc` directly.

  This means you can use probably `(reduce f init)` as a phase anywhere `f` is
  associative and commutative, and where `init` is immutable.

      (->> (t/map inc)
           (t/reduce + 0)
           (t/tesser [[1 2 3] [4 5 6]]))
      ; => 27

  Due to technical limitations Tesser can't distinguish between

      (reduce + upstream-fold)

  where we're transforming an uncompiled fold by adding a reduce phase, and

      (reduce + 0)

  where we're defining a new phase out of thin air with 0 as the initial value.
  Consequently, we *always* interpret the second argument as an initial value.
  We *don't* provide an equivalent for `(reduce +)` yet. Someday. Use `(fold +
  +)` or `(reduce + (+))` instead."
  [f init]
  (assert (nil? downstream))
  {:identity      (constantly init)
   :reducer       f
   :post-reducer  (maybe-unary f)
   :combiner      f
   :post-combiner (maybe-unary f)})

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

(defwraptransform post-combine
  "Transforms the output of a fold by applying a function to it.

  For instance, to find the square root of the mean of a sequence of numbers,
  try

      (->> (t/mean) (t/post-combine sqrt) (t/tesser nums))

  For clarity in ->> composition, post-combine composes in the opposite
  direction from map, filter, etc. It *prepends* a transform to the given fold
  instead of *appending* one. This means post-combines take effect in the same
  order you'd expect from ->> with normal function calls:

      (->> (t/mean)                 (->> (mean nums)
           (t/post-combine sqrt)         (sqrt)
           (t/post-combine inc))         (inc))"
  [f]
  (assoc downstream :post-combiner
         (fn post-combiner [x]
           (f (post-combiner- x)))))

;; Splitting folds

(deftransform group-by
  "Every input belongs to exactly one category, and you'd like to apply a fold
  to each category separately.

  Group-by takes a function that returns a category for every element, and
  returns a map of those categories to the results of the downstream fold
  applied to the inputs in that category.

  For instance, say we have a collection of particles of various types, and
  want to find the highest mass of each particle type:

      (->> (t/group-by :type)
           (t/map :mass)
           (t/max)
           (t/tesser [[{:name :electron, :type :lepton, :mass 0.51}
                       {:name :muon,     :type :lepton, :mass 105.65}
                       {:name :up,       :type :quark,  :mass 1.5}
                       {:name :down,     :type :quark,  :mass 3.5}]]))
      ; => {:lepton 105.65, :quark 3.5}"
  [category-fn]
  {:identity        (constantly {})
   :reducer         (fn reducer [acc input]
                      (let [category (category-fn input)]
                        (assoc acc category
                               (reducer-
                                 ; TODO: invoke downstream identity only when
                                 ; necessary.
                                 (get acc category (identity-)) input))))
   :post-reducer    identity
   :combiner        (fn combiner [m1 m2]
                      (merge-with combiner- m1 m2))
   :post-combiner   (fn post-combiner [m]
                      (map-vals post-combiner- m))})

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
                    (core/reduce (fn [acc [k v]]
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
    {:identity      (if (core/empty? fold-map)
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

;; Basic reductions

(deftransform count
  "How many inputs are there?"
  []
  (assert (nil? downstream))
  {:identity (constantly 0)
   :reducer  (fn reducer [c _] (inc c))
   :post-reducer identity
   :combiner +
   :post-combiner identity})

(deftransform set
  "A hash-set of distinct inputs."
  []
  (assert (nil? downstream))
  {:identity      (constantly #{})
   :reducer       conj
   :post-reducer  identity
   :combiner      set/union
   :post-combiner identity})

(deftransform frequencies
  "Like clojure.core/frequencies, returns a map of inputs to the number of
  times those inputs appeared in the collection."
  []
  (assert (nil? downstream))
  {:identity hash-map
   :reducer  (fn add [freqs x]
               (assoc freqs x (inc (get freqs x 0))))
   :post-reducer identity
   :combiner (partial merge-with +)
   :post-combiner identity})

(deftransform some
  "Returns the first logical true value of (pred input). If no such satisfying
  input exists, returns nil.

  This is potentially *less* efficient than clojure.core/some because each
  reducer has to find a matching element independently, and they have no way to
  communicate when one has found an element. In the worst-case scenario,
  requires N calls to `pred`. However, unlike clojure.core/some, this version
  is parallelizable--which can make it more efficient when the element is rare.

      (t/tesser [[1 2 3] [4 5 6]] (t/some #{1 2}))
      ; => 1"
  [pred]
  (assert (nil? downstream))
  {:identity      (constantly nil)
   :reducer       (fn reducer [_ x] (when-let [v (pred x)] (reduced v)))
   :post-reducer  identity
   :combiner      first-non-nil-reducer
   :post-combiner identity})

(deftransform any
  "Returns any single input from the collection. O(chunks)."
  []
  (assert (nil? downstream))
  {:identity      (constantly nil)
   :reducer       first-non-nil-reducer
   :post-reducer  identity
   :combiner      first-non-nil-reducer
   :post-combiner identity})

;; Predicate folds

(defn empty?
  "Returns true iff no inputs arrive; false otherwise.
  For instance:

      (t/tesser [[]] (t/empty?))
      ; => true"
  [& [f]]
  (->> f
       (map (fn [_] true))
       (some true?)
       (post-combine (complement boolean))))

(defn every?
  "True iff every input satisfies the given predicate, false otherwise. For
  instance:

      (t/tesser [[1 3 5]] (t/every? odd?))
      ; => true"
  [pred & [f]]
  (->> f
       (remove pred)
       (empty?)))

(defn not-every?
  "True if there exists an input which does *not* satisfy the given predicate.
  For instance:

      (t/tesser [[1 3 5] [6]] (t/not-every? odd?))
      ; => true"
  [pred & [f]]
  (->> f
       (every? pred)
       (post-combine not)))

;;TODO: distinct?, not-any?

;; Comparable folds

(deftransform extremum
  "Finds the largest element using a comparison function (default: `compare`)."
  [compare]
  (assert (nil? downstream))
  (letfn [(extremum-reducer [m x]
            (cond (nil? m)             x
                  (nil? x)             m
                  (<= 0 (compare x m)) x
                  true                 m))]
    {:identity      (constantly nil)
     :reducer       extremum-reducer
     :post-reducer  identity
     :combiner      extremum-reducer
     :post-combiner identity}))

(defn min
  "Finds the smallest value using `compare`."
  [& [f]]
  (->> f (extremum (comp - compare))))

(defn max
  "Finds the largest value using `compare`."
  [& [f]]
  (->> f (extremum compare)))

(defn range
  "Returns a pair of `[smallest largest]` inputs, using `compare`."
  [& [f]]
  (->> f
       (fuse {:min (min)
              :max (max)})
       (post-combine (juxt :min :max))))
