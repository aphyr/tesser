| API docs | Clojars | Purpose |
|----------|---------|---------|
| [tesser.core](http://aphyr.github.io/tesser/tesser.core.html) | [tesser.core](https://clojars.org/tesser.core) | The core library and essential folds |
| [tesser.math](http://aphyr.github.io/tesser/tesser.math.html) | [tesser.math](https://clojars.org/tesser.math) | Statistical folds: means, correlations, covariance matrices, quantiles, etc. |
| [tesser.hadoop](http://aphyr.github.io/tesser/tesser.hadoop.html) | [tesser.hadoop](https://clojars.org/tesser.hadoop) | Run folds on Hadoop via Parkour |

# Tesser

> "Now we will tesser, we will wrinkle again. Do you understand?" "No,"
> Meg said flatly. Mrs. Whatsit sighed. "Explanations are not easy when they
> are about things for which your civilization still has no words. Calvin
> talked about traveling at the speed of light. You understand that, little
> Meg?" "Yes," Meg nodded. "That, of course, is the impractical, long way
> around. We have learned to take short cuts wherever possible." "Sort of
> like in math?" Meg asked. "Like in math."

— Madeline L'Engle, *A Wrinkle In Time*.

## A Motivating Example

You've got a big pile of data--say, JSON in files on disk, or TSVs in
Hadoop--and you'd like to reduce over that data: computing some statistics,
searching for special values, etc. You might want to find the median housing
price in each city given a collection of all sales, or find the total mass of
all main-sequence stars in a region of sky, or search for an anticorrelation
between vaccine use and the prevalence of a disease. These are all *folds*:
collapsing a collection of data into a smaller value.

In Clojure, we're used to writing programs like

```clj
(->> stars
     (filter main-sequence?)
     (map :mass)
     (reduce +))
```

But this reduction is *singlethreaded*, and can only run on a single machine.
You've got 48 cores in your desktop computer. Why aren't they all helping?

```clj
(require '[tesser.core :as t])
(->> (t/filter main-sequence)
     (t/map :mass)
     (t/fold +)
     (t/tesser (partition 100 stars)))
```

Tesser goes much deeper, but this is the essence: writing understandable,
composable, *fast* programs for exploring datasets.

## A Clojure Library for Concurrent & Commutative Folds

Tesser gives us a library for building up *folds*, and applying those folds to
a collection of *inputs*, divided into *chunks*. Chunks are reduced with
maximal parallelism, and the results of those reductions are reduced together.
We call the concurrent reduction `:reducer`, and the serial reduction
`:combiner`.

![Reduce/combine diagram](/img/reduce-combine.jpg)

In order to reduce over a chunk, we need an *initial value*. A
`:reducer-identity` function generates this initial value. Once the reduction
is complete, we may want to transform it using a `:post-reducer` function--for
instance, converting from a transient to a persistent data structure, or
discarding internal reducer state.

![Diagram of reducer identities and post-reduce](/img/reducer-identity-post.jpg)

Likewise, we need a `:combiner-identity` function to generate an initial value
for the combine reduction, and a final `:post-combine` function to transform
the combiner's output.

![Diagram of combiner identity and post-combine](/img/combiner-identity-post.jpg)

In Tesser, we represent a compiled fold as map of six functions:

```clj
{:reducer-identity  (fn [] ...)
 :reducer           (fn [accumulator input] ...)
 :post-reducer      (fn [accumulator] ...)
 :combiner-identity (fn [] ...)
 :combiner          (fn [accumulator post-reducer-result])
 :post-combiner     (fn [accumulator] ...)}
```

For instance, here's a fold to find the sum of all inputs. While the reducer
and combiner often have the same accumulator type and identities,
this is not always the case.

```clj
  {:reducer-identity  (constantly 0)
   :reducer           +
   :post-reducer      identity
   :combiner-identity (constantly 0)
   :combiner          +
   :post-combiner     identity}
```

## Core

[Tesser.core](http://aphyr.github.io/tesser/tesser.core.html) looks a lot like
the Clojure seq API, and many of its functions have similar names. Their
semantics differ, however: Tesser folds do not preserve the order of inputs,
and when executed, they run in *parallel*.

Applying a fold using `tesser.core/tesser` uses mutiple threads proportional to
processor cores. Unlike reducers, we don't use the Java forkjoin pool, just
plain old threads. I've seen too many weird performance issues compared to
regular threads.

```clj
(require '[tesser.core :as t])

(t/tesser [[1 2 3] [4 5 6]] (t/into [] (t/map str)))
=> ["4" "5" "6" "1" "2" "3"]

(->> (t/map inc)      ; Increment each number
     (t/filter odd?)  ; Take only odd numbers
     (t/take 5)       ; *which* five odd numbers are selected is arbitrary
     (t/mapcat range) ; Explode each odd number n into the numbers from 0 to n
     (t/frequencies)  ; Compute the frequency of appearances
     (t/tesser (partition 3 (range 100))))
=> {0 5, 7 2, 20 1, 27 1, 1 4, 24 1, 4 3, 15 2, 21 1, 13 2, 22 1, 6 2, 28 1, 25 1, 17 1, 3 3, 12 2, 2 4, 23 1, 19 1, 11 2, 9 2, 5 2, 14 2, 26 1, 16 2, 10 2, 18 1, 8 2}
```

Fold combinators like
[facet](http://aphyr.github.io/tesser/tesser.core.html#var-facet) and
[fuse](http://aphyr.github.io/tesser/tesser.core.html#var-fuse) allow multiple
reductions to be done in a single pass, possibly sharing expensive operations
like deserialization. This is a particularly effective way of working with a
set of data files on disk or in Hadoop.

Given JSON records about a codebase like

```clj
{"year":         2004,
 "lines-of-code" {"ruby": 100,
                  "c":    1693}}
```

We can find the range of years *and* the total lines of code in each language
in a single pass.

```clj
(->> (t/map #(json/parse-string % true))
     (t/fuse {:year-range (t/range (t/map :year))
              :total-code (->> (t/map :lines-of-code)
                               (t/facet)
                               (t/reduce + 0)})
     (t/tesser records))

=> {:year-range [1986 2014]
    :total-code {:c    153423,
                 :ruby 4578,
                 :tcl  3453
                 :bf   1}}
```

Ready to get started? Start with the [tesser.core docs](http://aphyr.github.io/tesser/tesser.core.html)

## Math

[Tesser.math](http://aphyr.github.io/tesser/tesser.math.html) provides
statistical folds. Some, like sum, mean, and correlation, are exact. Others,
like quantiles, are estimates.

For instance, to find a Pearson correlation matrix between ln(age), height, and
weight; and in the same pass, to find the total number of samples:

```clj
(require '[tesser.math :as m])
(->> (t/fuse {:count (t/count)
              :corrs (m/correlation-matrix {:log-age #(Math/ln (:age %))
                                            :height  :height
                                            :weight  :weight})})
     (t/tesser records))

=> {:count 7123525
    :corrs {[:log-age :weight] 0.74
            [:log-age :height] 0.86
            [:weight  :height] 0.91
            ... and the same keys in symmetric order ...}}
```

Ready? [To the tesser.math API!](http://aphyr.github.io/tesser/tesser.math.html)

## Hadoop

The [tesser.hadoop API](http://aphyr.github.io/tesser/tesser.hadoop.html) takes
Tesser folds and distributes them using the
[Parkour](https://github.com/damballa/parkour) Hadoop library. You can test
your folds locally, then run them on a cluster to reduce over huge datasets.

```clj
(require '[tesser [core :as t]
                  [math :as m]
                  [hadoop :as h]])

(defn analyze
  "A fold that analyzes measurements of trees from a certain location."
  [location]
  (->> (t/map parse-record)
       (t/filter #(= location (:location %)))
       (t/fuse {:count (t/count)
                :oldest (->> (t/map :age)
                             (t/max))
                :corrs (m/correlation-matrix
                         {:age          :age
                          :log-mass     #(Math/log (:mass %))
                          :growth-rings :growth-rings
                          :humidity     :humdity})})))

(h/fold conf
        (text/dseq "hdfs:/some/file/part-*")
        "hdfs:/tmp/tesser"
        #'analyze "Redwood National Park")
```

See the [Hadoop demo
project](https://github.com/aphyr/tesser/tree/master/hadoop/demo) for an
example of how to run a fold in Hadoop.

## An integrative example

Here's a real-world example, drawn from a user profile verification suite:

```clj
(require '[tesser.core :as t]
          [tesser.math :as m]
          [tesser.quantiles :as q])

(defn fold
  "Computes aggregate statistics over profiles by country, income, age range,
  and gender."
  []
  (t/fuse
    {:total     (t/count)
     :country   (->> (t/map (comp :country :geo))
                     (t/frequencies)
                     (t/post-combine (partial into (sorted-map))))
     :income    (->> (t/map (comp :value :income))
                     (t/remove nil?)
                     (m/digest (partial q/dual q/hdr-histogram))
                     (t/post-combine q/distribution))
     :no-income (->> (t/remove (comp :value :income))
                     (t/count))
     :age-range (->> (t/map (comp :range :age :demographic))
                     (t/frequencies)
                     (t/post-combine (partial into (sorted-map))))
     :gender    (->> (t/map (fn [u]
                             (let [g (-> u :demographic :gender)
                                   m (-> g :male :confidence)
                                   f (-> g :female :confidence)]
                               (cond (not (or m f)) nil
                                     (not m)        :female
                                     (not f)        :male
                                     (<= -1/2 (- m f) 1/2) :rich-tapestry
                                     (<= m f)       :female
                                     :else          :male))))
                     (t/frequencies))}))
```

Here's another fold, from a namespace that analyzes behavioral segments (e.g.
commuters).  We're using the `metric-fold` function to do the same type of
numeric reduction (a range and a quantile digest) over two different fields:
`:percentile` and `:confidence`. Note that `metric-fold` takes an argument `f`:
the fold that it will transform.

```clj
(defn metric-fold
  "A fold which analyzes the range and quantile distribution of a number,
  returing a map of :range and :q-digest. Drops nils."
  [f]
  (->> f
       (t/remove nil?)
       (t/fuse {:q-digest (m/digest (partial q/dual q/hdr-histogram))
                :range    (t/range)})))

(defn fold
  "Analyzes behavioral segments. Emits nested maps where the first key is the
  segment name (:leisure_seeker), the second key is one of :percentile or
  :confidence, and the third key is one of :q-digest or :range."
  []
  (->> (t/map :behavioral)
       (t/map :segments)
       ; For each distinct segment...
       (t/facet)
       ; Could facet here, but fuse is a lil more efficient.
       (t/fuse
         {:percentile (->> (t/map :percentile) (metric-fold))
          :confidence (->> (t/map :confidence) (metric-fold))})))
```

Because the folds aren't bound to any particular datatype, we can combine them using `fuse` and transform their inputs using a parser--in a single pass.

```clj
(defn main-fold [opts]
  (->> (t/map (file/parser (:format opts)))
       (t/fuse {:behavioral-segments (behavioral-segments/fold)
                :demographics        (demographics/fold)})))
```

Because these folds are collection-indepedent, and defined in small chunks, we
can write small tests to verify each folds behavior indepedently, then compose
them into larger programs. We're free to name transformations at any level just
by binding them to `let` variables or `defn`s, or to build complex folds in a
single pass.

## Invariants

In order for Tesser to execute a fold concurrently, a fold must obey some
simple invariants.

- All functions should be deterministic functions purely of their inputs.
  Constructing two copies of the same fold on, say, different nodes in a
  cluster should result in equivalent behavior. You *cannot* squirrel away state
  in a lexical closure, for instance.
- Accumulators may be mutable, and folds never modify the same accumulator
  concurrently. You may mutate the accumulator in a reducer, combiner, or
  post-fn without synchronization.
- Reducers and combiners must be associative: `(f a (f b c))` = `(f (f a b)
  c)`.
- Reducers and combiners must be commutative: `(f a b)` = `(f b a)`.
- Reducers and combiners may emit `reduced` values, which force immediate
  completion of that particular reduce or combine. If a reducer emits a reduced
  value, it has no impact on the execution of other reducers, or the combiner.

## Vs Reducers and Transducers

Clojure's reducers and transducers embody sequential folds: they move from left
to right over a sequence. Reducers also includes a less-well-known
*hierarchical* fold which parallelizes a reduction via Java's fork-join pool,
but this reduction is still fundamentally ordered and local to a single
machine.

Tesser explores a different niche. It offers:

- *Commutativity.* Tesser folds must not depend on the order of inputs.
- *Concurrency.* Reductions over independent chunks require no coordination,
  making them good candidates for distributed contexts like Hadoop.
- *Stream fusion.* Like Reducers and Transducers, `map`, `filter`, etc. are all
  folded into a single reduction function. Intermediate values are
  stack-allocated, reducing GC load.
- *Collection independence.* Like Transducers, Tesser folds are abstract
  transformations and can be re-used against varying types of collections.

## Contributors

- [Kyle Kingsbury](mailto:aphyr@aphyr.com)
- [Natasha Whitney](mailto:natiwhitney@gmail.com)
- Factual, Inc

## License

Eclipse Public License v1.0, same as Clojure.
