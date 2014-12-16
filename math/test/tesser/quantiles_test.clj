(ns tesser.quantiles-test
  (:require [clojure.test :refer :all]
            [clojure.math.numeric-tower :refer :all]
            [clojure.pprint :refer [pprint]]
            [clojure.test.check :as tc]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]]
            [tesser.math-test :refer [correlation approx= =ish bigger-ints]]
            [tesser.quantiles :as q]))

; For larger values we start failing the distribution analysis due to
; floating-point offset errors. Ughhh.
(def test-opts {:num-tests 1000
                :par 48})

(defn fill!
  "Applies all points to a given digest."
  [digest points]
  (doseq [p points] (q/add-point! digest p)))

(defn round-trip
  "Returns a copy of a digest by serializing and deserializing it to a
  bytebuffer."
  [digest]
  (->> digest q/write-buf (q/read-buf! digest)))

(defn check-count
  "Verifies that a quantile estimator knows how many points it received."
  [digest points]
  (testing "count"
    (is (= (count points) (q/point-count digest)))))

; Just a wrapper for the quantile comparison tuples
(defrecord QC [quantile actual estimate])

(defn quantile-comparison
  "Given a digest and a set of points, returns a sorted sequence of maps of
  {:quantile 0, :actual 0, :estimate 0.24}, where quantile ranges from 0 to 1,
  :actual is the real value for that quantile, drawn from `points`, and
  :estimate is the estimated value for that quantile from the digest."
  [digest points]
  (let [n (count points)]
    (->> points
         sort
         ; inc i because indices start at 1; see
         ; http://en.wikipedia.org/wiki/Quantile#Quantiles_of_a_population
         (map-indexed (fn [i x]
                        (let [q (/ (inc i) n)]
                          (QC. q x (q/quantile digest q))))))))

(defn check-quantiles-exact
  "If every quantile comparison is almost the same, we're trivially done. We do
  this check because flat distributions like [3 3 3 3 3 3] lead to undefined
  correlations (because there's no extent to the domain."
  [quantile-comparison]
  (testing "exact"
    (every? #(=ish (:actual %) (:estimate %)) quantile-comparison)))

(defn check-quantiles-correlation
  "Compute the correlation between the true and estimated quantiles."
  [quantile-comparison]
  (testing "correlation"
    (let [correlation (correlation :actual :estimate quantile-comparison)]
      (or (is (< 0.99 correlation))
          (pprint "Bad correlation")
          (pprint quantile-comparison)
          (prn :correlation correlation)))))

(defn check-quantiles
  "Evaluates how well the digest estimates quantiles."
  [digest points]
  (testing "quantiles"
    (case (count points)
      ; For the empty set, we don't care
      0 true

      ; A single point should be the same as every quantile.
      1 (and (is (=ish (first points)
                       (q/quantile digest 0)
                       (q/quantile digest 0.5)
                       (q/quantile digest 1))))

      ; Statistical checks
      (let [qc (quantile-comparison digest points)]
        ; Try an exact match, then fall back to statistical methods.
        (or (check-quantiles-exact qc)
            (and (check-quantiles-correlation qc)))))))

(defn equiv-distributions
  "Floating point is a problem.

  test.check is really good at finding inputs like

  :points (-1.05 -0.89 -0.45 0.73 0.73 1.98)
  :digest ([-1.05003 1] [-0.88999 1] [-0.44999 1] [0.72998 2] [1.98001 1])
  :actual [[-1.05003 0] [-0.88999 2] [-0.44999 1] [0.72998 0] [1.98001 3]]

  Note that 0.73 has been represented in the digest as 0.72998, which pushed
  both 0.73's up into the next bucket: 1.98001.

  So, to verify distribution equivalence, we check:

  1. Every point is identical.
  2. If we ever see a count that is not equal, we can make it up by stealing
     from the next bucket.

  dist1 is the digest, dist2 is the actual."
  [dist1 dist2]
  (and (testing "points"
         (is (= (map first dist1) (map first dist2))))
       (testing "counts"
         (let [c1    (map second dist1)
               c2    (map second dist2)]
           (loop [[p1 & c1' :as c1] (map second dist1)
                  [p2 & c2' :as c2] (map second dist2)]
             (if (nil? p1)
               ; Done
               true

               (if (= p1 p2)
                 ; Good.
                 (recur c1' c2')

                 (let [p1' (first c1')
                       p2' (first c2')
                       delta (- p1 p2)]
;                   (prn :p1 p1 :p2 p2 :delta delta)
                   (if-not (and p1' p2')
                     ; We're at the end, too bad.
                     false

                     ; Can we find (= p1 p2) by rebalancing p2 and p2',
                     ; keeping both positive?
                     (let [p2-new  (+ p2 delta)
                           p2'-new (- p2' delta)]
;                       (prn :p2-new p2-new :p2'-new p2'-new)
                       (if (or (neg? p2-new) (neg? p2'-new))
                         ; Can't rebalance enough
                         false

                         ; Try again with the new balance.
                         (recur c1 (cons p2-new (cons p2'-new (next c2')))))))))))))))


(defn check-distribution
  "Verifies that the distribution over the digest matches the points."
  [digest points]
  (testing "distribution"
    (if (empty? points)
      (is (= [] (q/distribution digest)))

      (let [digest-dist (q/distribution digest)
            cutoffs     (map first digest-dist)
            points      (sort points)
            real-dist   (loop [[cutoff & cutoffs' :as cutoffs]  cutoffs
                               [point & points'   :as points]   points
                               dist                             []
                               n                                0]
                          (cond ; Done!
                                (nil? point)
                                (conj dist [cutoff n])

                                (or (empty? cutoffs') (<= point cutoff))
                                (recur cutoffs points' dist (inc n))

                                true
                                (recur cutoffs' points
                                       (conj dist [cutoff n])
                                       0)))]
        (and (is (=ish (first points) (first cutoffs)))
             (is (=ish (last points)  (last cutoffs)))
             (or (is (equiv-distributions digest-dist real-dist))
                 (prn :points points)
                 (prn :digest digest-dist)
                 (prn :actual real-dist)))))))

(defn check-digest
  "Check that a quantile estimator handles a given set of inputs OK."
  [digest points]
  (fill! digest points)
    (and (check-count digest points)
         (check-quantiles digest points)
         (check-distribution digest points)))

(defn runs
  "Quickcheck likes to emit uniformly distributed vectors, but we need
  pathological distributions to test quantile estimators. This generator takes
  an underlying generator and emits a vector containing *runs* of those
  values, which translate to spikes in the probability density."
  [gen]
  (assert (gen/generator? gen) "Arg to runs must be a generator")
  (gen/bind (gen/vector gen/pos-int)
            (fn [run-lengths]
              (gen/bind (gen/vector gen (count run-lengths))
                        (fn [values]
                          (gen/return
                            (mapcat repeat run-lengths values)))))))

(defn smaller
  "Make numbers smaller"
  [x]
  (double (/ x 100)))

(defspec hdr-histogram-spec
  test-opts
  (prop/for-all [points (gen/vector (gen/fmap smaller bigger-ints))]
                ;(prn :points points)
                (check-digest (q/dual
                                q/hdr-histogram
                                {:highest-to-lowest-value-ratio 1e6
                                 :significant-value-digits      4}) points)))

(defspec hdr-histogram-runs-spec
  (assoc test-opts :num-tests 20)
  (prop/for-all [points (runs (gen/fmap smaller bigger-ints))]
                ;(prn :points points)
                (check-digest (q/dual
                                q/hdr-histogram
                                {:highest-to-lowest-value-ratio 1e6
                                 :significant-value-digits      4}) points)))
