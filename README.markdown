# Tesser

> "Now we will tesser, we will wrinkle again. Do you understand?" "No,"
> Meg said flatly. Mrs. Whatsit sighed. "Explanations are not easy when they
> are about things for which your civilization still has no words. Calvin
> talked about traveling at the speed of light. You understand that, little
> Meg?" "Yes," Meg nodded. "That, of course, is the impractical, long way
> around. We have learned to take short cuts wherever possible." "Sort of
> like in math?" Meg asked. "Like in math."

— Madeline L'Engle, *A Wrinkle In Time*.

## A Clojure Library for Concurrent & Commutative Folds

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

## Installation

Via Clojars, as usual.

- [tesser.core](https://clojars.org/tesser.core) - The core library and
  essential folds
- [tesser.math](https://clojars.org/tesser.math) - Statistical folds: means,
  correlations, covariance matrices, quantiles, etc.
- tesser.hadoop - Run folds on Hadoop. Coming soon!

## Core

[Tesser.core](http://aphyr.github.io/tesser/tesser.core.html) looks a lot like
the Clojure seq API, but without preserving order, and running in parallel.
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

Given JSON records about a codebase like like

```clj
{"year":         2004,
 "lines-of-code" {"ruby": 100,
                  "c":    1693}}
```

We can find the range of years *and* the total lines of code in each language
in a single pass.

```clj
(require '[tesser.math :as m])
(->> (t/map #(json/parse-string % true))
     (t/fuse {:year-range (t/range (t/map :year))
              :total-code (->> (t/map :lines-of-code)
                               (t/facet)
                               (m/sum))})
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
like quantiles, are estimates. Quantiles and histograms are still a WIP.

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

Coming soon.

## Contributors

Kyle Kingsbury <aphyr@aphyr.com>
Natasha Whitney
Factual, Inc

## License

Eclipse Public License v1.0, same as Clojure.
