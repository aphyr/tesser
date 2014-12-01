(ns tesser.quantiles-test
  (:require [clojure.test :refer :all]
            [clojure.math.numeric-tower :refer :all]
            [clojure.pprint :refer [pprint]]
            [clojure.test.check :as tc]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]]
            [tesser.math-test :refer [correlation =ish bigger-ints]]
            [tesser.quantiles :as q]))

(def test-count 1e2)

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

(defn check-distribution
  "Verifies that the cumulative distribution over the digest matches the
  points."
  [digest points]
  (testing "cumulative distribution"
    (if (empty? points)
      (is (= [] (q/cumulative-distribution digest)))

      (let [digest-dist (q/cumulative-distribution digest)
            cutoffs     (map first digest-dist)
            real-dist   (loop [[cutoff & cutoffs' :as cutoffs] cutoffs
                               [point  & points'  :as points]  (sort points)
                               dist                               []
                               total                              0]
                          (cond ; Stretch the cutoff to the next point
                                (nil? cutoff)
                                (recur [point] points dist total)

                                ; Done!
                                (nil? point)
                                (conj dist [cutoff total])

                                (<= point cutoff)
                                (recur cutoffs points' dist (inc total))

                                true
                                (recur cutoffs' points
                                       (conj dist [cutoff total])
                                       total)))]
        (or (is (= digest-dist real-dist))
            (prn :points points)
            (prn :digest digest-dist)
            (prn :actual real-dist))))))

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
  1e4
  (prop/for-all [points (gen/vector gen/int)]
                (check-digest (q/dual
                                q/hdr-histogram
                                {:highest-to-lowest-value-ratio 1e2
                                 :significant-value-digits      3}) points)))

(comment
(defspec hdr-histogram-spec
  1e4
  (prop/for-all [points (gen/vector (gen/fmap smaller bigger-ints))]
                (check-digest (q/dual
                                q/hdr-histogram
                                {:highest-to-lowest-value-ratio 1e8
                                 :significant-value-digits      3}) points)))

(defspec hdr-histogram-runs-spec
  1e2
  (prop/for-all [points (runs (gen/fmap smaller bigger-ints))]
                (check-digest (q/dual
                                q/hdr-histogram
                                {:highest-to-lowest-value-ratio 1e8
                                 :significant-value-digits      3}) points))))
