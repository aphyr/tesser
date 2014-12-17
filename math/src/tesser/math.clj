(ns tesser.math
  "Folds over numbers! Calculate sums, means, variance, standard deviation,
  covariance and linear correlations, and matrices thereof, plus quantiles and
  histograms estimates backed by probabilistic QDigests."
  (:require [tesser.core :as t :refer [deftransform]]
            [tesser.utils :refer :all]
            [tesser.quantiles :as q]
            [clojure.core.reducers :as r]
            [clojure.math.numeric-tower :refer :all]
            [clojure.math.combinatorics :as combo]
            [clojure.set  :as set]
            [clojure.core :as core]))

(deftransform sum
  "Finds the sum of numeric elements."
  []
  (assert (nil? downstream))
  {:identity      (constantly 0)
   :reducer       +
   :post-reducer  identity
   :combiner      +
   :post-combiner identity})

(deftransform mean
  "Finds the arithmetic mean of numeric inputs."
  []
  (assert (nil? downstream))
  {:identity      (constantly [0 0])
   :reducer       (fn reducer [[s c] x]
                    [(+ s x) (inc c)])
   :post-reducer  identity
   :combiner      (fn combiner [x y] (core/map + x y))
   :post-combiner (fn post-combiner [x]
                    (double (/ (first x) (core/max 1 (last x)))))})

(deftransform variance
  "Unbiased variance estimation. Given numeric inputs, returns their
  variance."
  []
  (assert (nil? downstream))
  {:identity (constantly [0 0 0])
   :reducer (fn count-mean-sq [[count mean sum-of-squares] x]
              (let [count' (inc count)
                    mean'  (+ mean (/ (- x mean) count'))]
                [count'
                 mean'
                 (+ sum-of-squares (* (- x mean') (- x mean)))]))
   :post-reducer identity
   :combiner (fn partcmsq [[c m sq] [c2 m2 sq2]]
               (let [count (+ c c2)]
                 (if (zero? count)
                   [c m sq]
                   [count
                    (/ (+ (* c m) (* c2 m2)) count)
                    (+ sq sq2 (/ (* (- m2 m) (- m2 m) c c2) count))])))
   :post-combiner (fn vardiv [x] (double (/ (last x) (core/max 1 (dec (first x))))))})

(defn standard-deviation
  "Estimates the standard deviation of numeric inputs."
  [& [f]]
  (->> f (variance) (t/post-combine sqrt)))

(deftransform covariance
  "Given two functions of an input (fx input) and (fy input), each of which
  returns a number, estimates the unbiased covariance of those functions over
  inputs.

  Ignores any inputs where (fx input) or (fy input) are nil. If no inputs have
  both x and y, returns nil."
  [fx fy]
  (assert (nil? downstream))
  {:identity (constantly [0 0 0 0])
   :reducer (fn count-mean2-sq [[count meanx meany sum-of-squares
                                 :as current-state] elt]
              (let [x (fx elt)
                    y (fy elt)]
                (if (or (nil? x) (nil? y))
                  current-state
                  (let [count' (inc count)
                        meanx'  (+ meanx (/ (- x meanx) count'))
                        meany'  (+ meany (/ (- y meany) count'))]
                    [count'
                     meanx'
                     meany'
                     (+ sum-of-squares (* (- x meanx') (- y meany)))]))))
   :post-reducer identity
   :combiner (fn partcm2sq [[c mx my sq :as current-state] [c2 mx2 my2 sq2]]
               (let [count (+ c c2)]
                 (if (zero? count)
                   current-state
                   [count
                    (/ (+ (* c mx) (* c2 mx2)) count)
                    (/ (+ (* c my) (* c2 my2)) count)
                    (+ sq sq2 (/ (* (- mx2 mx) (- my2 my) c c2) count))])))
   :post-combiner (fn vardiv [[c _ _ sq]]
                    (when (pos? c) ; Return nil if no inputs
                      (double (/ sq c))))})

(defn fuse-matrix
  "Given:

  1. A function like `covariance` that takes two functions of an input and
     yields a fold, and

  2. A map of key names to functions that extract values for
     those keys from an input,

  pairwise-matrix computes that fold over each *pair* of keys, returning a map
  of name pairs to the result of that pairwise fold over the inputs. You can
  think of this like an N^2 version of `fuse`."
  [fold keymap & [downstream]]
  (->> downstream
       ; For this transform, map inputs to a temporary map of keys->values;
       ; we'll be doing O(keys) lookups on each key, so having a flat map cuts
       ; down on having to re-run expensive extractor functions.
       (t/map (fn project [input]
                (->> keymap
                     (core/reduce-kv (fn extract [m k extractor]
                                       (assoc! m k (extractor input)))
                                     (transient {}))
                     persistent!)))

       ; And pass those maps into a fused covariance transform
       (t/fuse (->> (combo/combinations (core/keys keymap) 2)
                    ; Turn pairs into [pair, covariance-fold]
                    (core/map (fn [[k1 k2]]
                                [[k1 k2] (fold #(get % k1) #(get % k2))]))
                    (core/into {})))

       ; And return both halves of the resulting triangular matrix
       (t/post-combine complete-triangular-matrix)))

(defn covariance-matrix
  "Given a map of key names to functions that extract values for those keys
  from an input, computes the covariance for each of the n^2 key pairs,
  returning a map of name pairs to the their covariance. For example:

  (t/covariance-matrix {:name-length #(.length (:name %))
                        :age         :age
                        :num-cats    (comp count :cats)})"
  [& args]
  (apply fuse-matrix covariance args))(defn fuse-matrix
  "Given:

  1. A function like `covariance` that takes two functions of an input and
     yields a fold, and

  2. A map of key names to functions that extract values for
     those keys from an input,

  pairwise-matrix computes that fold over each *pair* of keys, returning a map
  of name pairs to the result of that pairwise fold over the inputs. You can
  think of this like an N^2 version of `fuse`."
  [fold keymap & [downstream]]
  (->> downstream
       ; For this transform, map inputs to a temporary map of keys->values;
       ; we'll be doing O(keys) lookups on each key, so having a flat map cuts
       ; down on having to re-run expensive extractor functions.
       (t/map (fn project [input]
              (->> keymap
                   (core/reduce-kv (fn extract [m k extractor]
                                     (assoc! m k (extractor input)))
                                   (transient {}))
                   persistent!)))

       ; And pass those maps into a fused covariance transform
       (t/fuse (->> (combo/combinations (core/keys keymap) 2)
                  ; Turn pairs into [pair, covariance-fold]
                  (core/map (fn [[k1 k2]]
                              [[k1 k2] (fold #(get % k1) #(get % k2))]))
                  (core/into {})))

       ; And return both halves of the resulting triangular matrix
       (t/post-combine complete-triangular-matrix)))

(defn covariance-matrix
  "Given a map of key names to functions that extract values for those keys
  from an input, computes the covariance for each of the n^2 key pairs,
  returning a map of name pairs to the their covariance. For example:

  (t/covariance-matrix {:name-length #(.length (:name %))
                        :age         :age
                        :num-cats    (comp count :cats)})"
  [& args]
  (apply fuse-matrix covariance args))

(deftransform correlation+count
  "Given two functions: (fx input) and (fy input), each of which returns a
  number, estimates the unbiased linear correlation coefficient between fx and
  fy over inputs. Ignores any records where fx or fy are nil. If there are no
  records with values for fx and fy, the correlation is nil. See
  http://mathworld.wolfram.com/CorrelationCoefficient.html.

  This function returns a map of correlation and count, like

  {:correlation 0.34 :count 142}

  which is useful for significance testing."
  [fx fy]
  {:identity (constantly [0 0 0 0 0 0])
   :reducer (->> (fn count-m2-sq3 [[count meanx meany ssx ssy ssxy :as acc] elt]
                   (let [x (fx elt)
                         y (fy elt)]
                     (if-not (and x y)
                       acc
                       (let [count' (inc count)
                             meanx'  (+ meanx (/ (- x meanx) count'))
                             meany'  (+ meany (/ (- y meany) count'))]
                         [count'
                          meanx'
                          meany'
                          (+ ssx (* (- x meanx') (- x meanx)))
                          (+ ssy (* (- y meany') (- y meany)))
                          (+ ssxy (* (- x meanx') (- y meany)))])))))
   :post-reducer identity
   :combiner (fn partcm2sq3 [[c  mx  my  ssx  ssy ssxy]
                             [c2 mx2 my2 ssx2 ssy2 ssxy2]]
               (let [count (+ c c2)]
                 (if (zero? count)
                   [c mx my ssx ssy ssxy]
                   [count
                    (/ (+ (* c mx) (* c2 mx2)) count)
                    (/ (+ (* c my) (* c2 my2)) count)
                    (+ ssx ssx2 (/ (* (- mx2 mx) (- mx2 mx) c c2) count))
                    (+ ssy ssy2 (/ (* (- my2 my) (- my2 my) c c2) count))
                    (+ ssxy ssxy2 (/ (* (- mx2 mx) (- my2 my) c c2) count))])))
   :post-combiner (fn corrdiv [[c mx my ssx ssy ssxy]]
                    (let [div (sqrt (* ssx ssy))]
                      (when-not (zero? div)
                        {:count c
                         :correlation (/ ssxy div)})))})

(defn correlation+count-matrix
  "Given a map of key names to functions that extract values for those keys
  from an input, computes the correlations for each of the n^2 key
  pairs, returning a map of name pairs to the their correlations and counts.
  See correlation+count. For example:

  (t/correlation-matrix {:name-length #(.length (:name %))
                        :age         :age
                        :num-cats    (comp count :cats)})

  will, when executed, returns a map like

  {[:name-length :age]      {:count 150 :correlation 0.56}
   [:name-length :num-cats] {:count 150 :correlation 0.95}
   ...}"
  [& args]
  (apply fuse-matrix correlation+count args))

(defn correlation
  "Like correlation+count, but only returns the correlation."
  [& args]
  (->> args
       (apply correlation+count)
       (t/post-combine :correlation)))

(defn correlation-matrix
  "Like correlation+count-matrix, but returns just correlations coefficients
  instead of maps of :correlation and :count."
  [& args]
  (apply fuse-matrix correlation args))

(deftransform digest
  "You've got a set of numeric inputs and want to know their quantiles
  distribution, histogram, etc. This fold takes numeric inputs and
  produces a statistical estimate of their distribution.

  `digest` takes a function that returns a `tesser.quantiles/Digest`. The fold
  returns an instance of that digest.

  For example, to compute an HDRHistogram over both positive and negative
  doubles (or longs, rationals, etc):

  Compute a digest using e.g.

    (def digest (->> (m/digest q/hdr-histogram)
                     (t/tesser [[1 1 1 1 1 1 2 2 2 3 3 4 5]])))
    ; => #<DoubleHistogram ...>

  To specify options for the digest, just use partial or (fn [] ...)

    (m/digest (partial q/hdr-histogram {:significant-value-digits 4
                                        :highest-to-lowest-value-ratio 1e6}))

  DoubleHistogram, like many quantile estimators, only works over positive
  values. To cover positives and negatives together, use
  `tesser.quantiles/dual`:

    (m/digest #(q/dual q/hdr-histogram {:significant-value-digits 2}))

  Once you've computed a digest, you can find a particular quantile using
  `tesser.quantiles/quantile`

    (q/quantile digest 0)   ; => 1.0
    (q/quantile digest 0.5) ; => 1.0
    (q/quantile digest 4/5) ; => 2.0009765625
    (q/quantile digest 1)   ; => 3.0009765625

  The total number of points in the sample:

    (q/point-count digest) ; => 5

  Minima and maxima:

    (q/min digest) ; => 1.0
    (q/max digest) ; => 3.0009765625

  Or find the distribution of values less than or equal to each point, with
  resolution given by the internal granularity of the digest:

    (q/distribution digest)
    ; => ([1.0 3] [2.0009765625 1] [3.0009765625 1])

    (q/cumulative-distribution digest)
    ; => ([1.0 3] [2.0009765625 4] [3.0009765625 5])

  You don't have to return the whole digest; any of these derivative
  operations can be merged directly into the fold via
  `tesser.core/post-combine`.

    (->> (m/digest q/hdr-histogram)
         (t/post-combine #(q/quantile % 1/2))
         (t/tesser [[1 2 2 3 3 3 3 3 3 3 3]]))
    ; => 3.0009765625

  I want to emphasize that depending on the size of your data, its
  distribution, and the number of digests you want to compute, you may need
  different digest algorithms and widely varying tuning parameters. Until we
  have a better grasp of the space/error tradeoffs here, I won't choose
  defaults for you."
  [digest-generator]
  (assert (nil? downstream))
  {:identity      digest-generator
   :reducer       q/add-point!
   :post-reducer  identity
   :combiner      q/merge-digest!
   :post-combiner identity})
