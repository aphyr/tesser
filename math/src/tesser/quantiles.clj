(ns tesser.quantiles
  "Supports various streaming quantile sketches"
  (:refer-clojure :exclude [min max])
  (:import (org.HdrHistogram EncodableHistogram
                             DoubleHistogram
                             DoubleHistogramIterationValue)
           (java.nio ByteBuffer)
           (java.util.zip Deflater))
  (:require [tesser.utils :refer :all]
            [clojure.core.reducers :as r]
            [clojure.math.numeric-tower :refer []]
            [clojure.math.combinatorics :as combo]
            [clojure.set  :as set]
            [clojure.core :as core]))

(defprotocol Digest
  (add-point! [digest x]
              "Add a value to the given digest, mutating the digest. Returns
              digest.")
  (merge-digest! [digest other]
                 "Merges the second digest into the first, mutating the
                 first. Returns the first digest.")
  (point-count [digest]
               "How many points went into this digest?"))

(defprotocol Quantile
  (min       [digest] "The minimum point in the digest. For empty digests,
                      nil.")
  (max       [digest] "The maximum point in the digest. For empty digests,
                      nil.")
  (quantile  [digest q]  "Returns a point near the given quantile."))

(defprotocol CumulativeDistribution
  (cumulative-distribution [digest]
                "A non-normalized discrete cumulative distribution function for
                a digest, represented as an ascending-order sequence of `[point
                total]` pairs, where `total` is the number of points in the
                digest which are less than or equal to `point`. `point` ranges
                from min to max, inclusive.

                The cumulative distribution for an empty digest is an empty
                seq."))

(defn distribution
  "A discrete distribution function for a digest, represented as an
  ascending-order sequence of `[point count]` pairs, where `count` is the
  number of points less than or equal to `point`, and greater than the previous
  point. Point ranges from min to max inclusive."
  [digest]
  (let [cd (cumulative-distribution digest)]
    (->> digest
         cumulative-distribution
         (cons [nil 0])
         successive-pairs
         (map (fn [[[x c] [x' c']]]
                [x' (- c' c)])))))

(defn ^"[B" byte-buffer->bytes
  "Convert a byte buffer to a byte array."
  [^ByteBuffer buffer]
  (let [array (byte-array (.remaining buffer))]
    (.get buffer array)
    array))

(defn ^ByteBuffer bytes->byte-buffer
  "Convert a bytebuffer to a byte array."
  [^bytes bs]
  (ByteBuffer/wrap bs))

; TODO: dynamically detect dependencies and load these extensions
; I'd include em all by default but the jar gets *massive*
;(extend-type AVLTreeDigest
;  Quantile
;  (add-point!       [digest x] (.add digest x))
;  (merge-digest!    [digest ^AVLTreeDigest d] (.add digest d))
;  (point-count      [digest]   (.size digest))
;  (quantile         [digest q] (.quantile digest q))

;  Buffers
;  (buf-capacity     [digest]   (.smallByteSize digest))
;  (write-buf!       [digest b] (.asSmallBytes digest b)
;  (read-buf!        [digest b] (AVLTreeDigest/fromBytes b))))

(extend-type DoubleHistogram
  Digest
  (add-point!       [digest x] (.recordValue digest x) digest)
  (merge-digest!    [digest ^DoubleHistogram d] (.add digest d) digest)
  (point-count      [digest]   (.getTotalCount digest))

  Quantile
  (min      [digest]   (when-not (zero? (point-count digest))
                         (.getMinValue digest)))
  (max      [digest]   (when-not (zero? (point-count digest))
                         (.getMaxValue digest)))
  (quantile [digest q] (.getValueAtPercentile digest (* q 100)))

  CumulativeDistribution
  (cumulative-distribution [digest]
    (->> (.recordedValues digest)
         (mapv (fn [^DoubleHistogramIterationValue i]
                 [(.getValueIteratedTo i)
                  (.getTotalCountToThisValue i)])))))

(defn hdr-histogram
  "Constructs a new HDRHistogram for doubles.
  Default options:

      {:highest-to-lowest-value-ratio 1e8
       :significant-value-digits      3}"
  ([] (hdr-histogram {}))
  ([opts]
   (DoubleHistogram. (long (or (:highest-to-lowest-value-ratio opts) 1e8))
                     (int (or (:significant-value-digits      opts) 3)))))

; A histogram that covers both negative and positive numbers by routing to
; two distinct histograms. 0 is considered positive here.
(defrecord DualHistogram [neg pos]
  Digest
  (add-point! [this x]
    (if (neg? x)
      (add-point! neg (- x))
      (add-point! pos x))
    this)

  (merge-digest! [this other]
    (assert (instance? DualHistogram other))
    (merge-digest! neg (:neg other))
    (merge-digest! pos (:pos other))
    this)

  (point-count [this]
    (+ (point-count neg)
       (point-count pos)))

  Quantile
  (min [this] (if-let [x (max neg)]
                (- x)
                (min pos)))

  (max [this] (or (max pos)
                  (when-let [x (min neg)]
                    (- x))))

  (quantile [this q]
    (let [n (point-count neg)
          p (point-count pos)
          N (+ n p)]
;      (println "finding quantile" q ": neg" n "pos" p "total" N)

      (cond ; No negatives
            (zero? n)
            (quantile pos q)

            ; No positives
            (zero? p)
            (let [neg-q (- 1 (clojure.core/max 0 (- q (/ N))))]
;              (println "quantile" q "mapped to all-neg quantile" neg-q
;                       "with value" (- (quantile neg neg-q)))
              (- (quantile neg neg-q)))

            ; Falls in negative range
            (<= q (/ (+ n 1/2) N))
            (let [neg-q (- 1 (* (/ N n) (- q (/ N))))]
;              (println "quantile" q "mapped to neg quantile" neg-q "with value"
;                       (- (quantile neg neg-q)))
              (- (quantile neg neg-q)))

            ; Falls in positive range
            true
            (let [pos-q (/ (- (* N q) n) p)]
;              (println "quantile" q "mapped to pos quantile"
;                       pos-q "with value" (quantile pos pos-q))
              (quantile pos pos-q)))))

  CumulativeDistribution
  (cumulative-distribution [this]
    (cond
      (zero? (point-count neg))
      (cumulative-distribution pos)

      true
      (let [neg-dist    (cumulative-distribution neg)
            pos-offset  (point-count neg)]
        ; Ugh, inefficient
        (concat (map vector
                     ; Points
                     (->> neg-dist
                          reverse
                          (map first)
                          (map -))
                     ; Totals
                     (->> neg-dist
                          (map second)
                          (cons 0)
                          differences
                          reverse
                          cumulative-sums))
                ; Positive distribution
                (->> (cumulative-distribution pos)
                     (map (fn [[point total]]
                            [point (+ total pos-offset)]))))))))

(defn dual
  "HDRHistogram can't deal with negative values. This function takes a function
  to construct a quantile estimator and returns a quantile estimator that uses
  *two* of the underlying estimator, one for positive numbers, and one for
  negative numbers.

      (dual hdr-histogram {:significant-value-digits 4}))"
  ([h] (dual h {}))
  ([h opts]
   (DualHistogram. (h opts) (h opts))))
