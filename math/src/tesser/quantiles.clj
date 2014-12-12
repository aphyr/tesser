(ns tesser.quantiles
  "Supports various streaming quantile sketches"
  (:refer-clojure :exclude [min max])
  (:import (org.HdrHistogram EncodableHistogram
                             DoubleHistogram
                             HistogramIterationValue)
           (java.nio ByteBuffer)
           (java.util.zip Deflater))
  (:require [tesser.utils :refer :all]
            [clojure.core.reducers :as r]
            [clojure.math.numeric-tower :refer :all]
            [clojure.math.combinatorics :as combo]
            [clojure.set  :as set]
            [clojure.core :as core]))

(defprotocol Digest
  (add-point! [digest x]
              "Add a value to the given digest, mutating the digest.")
  (merge-digest! [digest other]
                 "Merges the second digest into the first, mutating the
                 first.")
  (point-count [digest]
               "How many points went into this digest?"))

(defprotocol Quantile
  (min       [digest] "The minimum point in the digest.")
  (max       [digest] "The maximum point in the digest.")
  (quantile  [digest q]  "Returns a point near the given quantile."))

(defprotocol CumulativeDistribution
  (cumulative-distribution [digest]
                "STILL BROKEN, SO SORRY!

                A non-normalized discrete cumulative distribution function for
                a digest, represented as an ascending-order sequence of `[point
                total]` pairs, where `total` is the number of points in the
                digest which are less than or equal to `point`. `point` ranges
                from min to max, inclusive.

                The cumulative distribution for an empty digest is an empty
                seq."))

(defprotocol Buffers
  (buf-capacity [x] "How many bytes are we gonna need, tops?")
  (write-buf! [x ^ByteBuffer b] "Write x to a ByteBuffer. Mutates b by writing
                                bytes to the current position, leaving the
                                position advanced.")
  (read-buf!  [x ^ByteBuffer b] "Read a new instance of x from byte buffer b.
                                Mutates b by advancing the position."))

(defn ^"[B" byte-buffer->bytes
  "Convert a byte buffer to a byte array."
  [^ByteBuffer buffer]
  (let [array (byte-array (.remaining buffer))]
    (.get buffer array)
    array))

(defn write-buf
  "Write the given thing to a fresh ByteBuffer and returns that buffer, flipped
  so it's ready for reading."
  ([x]
   (let [buf (ByteBuffer/allocate (buf-capacity x))]
     (write-buf! x buf)
     (.flip buf)
     buf)))

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
  (add-point!       [digest x] (.recordValue digest x))
  (merge-digest!    [digest ^DoubleHistogram d] (.add digest d))
  (point-count      [digest]   (.getTotalCount digest))

  Quantile
  (min      [digest]   (.getMinValue digest))
  (max      [digest]   (.getMaxValue digest))
  (quantile [digest q] (.getValueAtPercentile digest (* q 100)))

  CumulativeDistribution
  (cumulative-distribution [digest]
    ; multiply by conversion factor to correct for
    ; https://github.com/HdrHistogram/HdrHistogram/issues/35
    (->> digest
         ;.allValues
         .recordedValues
         .iterator
         iterator-seq
         (map (fn [^HistogramIterationValue i]
                [(.getDoubleValueIteratedTo i)
                 (.getTotalCountToThisValue i)]))))

  Buffers
  (buf-capacity     [digest] (.getNeededByteBufferCapacity digest))
  (write-buf!       [digest b]
    (.encodeIntoCompressedByteBuffer digest b Deflater/DEFAULT_COMPRESSION))
  (read-buf         [_ b]
    (DoubleHistogram/decodeFromCompressedByteBuffer b 0)))

(defn hdr-histogram
  "Constructs a new HDRHistogram for doubles. Default options:

  {:highest-to-lowest-value-ratio 1e13
   :significant-value-digits      4}"
  ([] (hdr-histogram {}))
  ([opts]
   (DoubleHistogram. (or (:highest-to-lowest-value-ratio opts) 1e8)
                     (or (:significant-value-digits      opts) 3))))

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

      ;     (zero? (point-count pos))
      ;     (cumulative-distribution neg)

      true
      (let [neg-dist    (cumulative-distribution neg)
            pos-offset  (point-count neg)
            neg-offset  (+ pos-offset (second (first neg-dist)))]
        (prn :pos-offset pos-offset)
        (prn :neg-offset neg-offset)
        (prn :neg-dist neg-dist)
        (concat (loop [[[point' total'] & remaining] neg-dist
                       total 0
                       dist  ()]
                  (if (seq remaining)
                    
                    
                (->> (cumulative-distribution pos)
                     (map (fn [[point total]]
                            [point (+ total pos-offset)])))))))

  Buffers
  (buf-capacity [this]
    (+ (buf-capacity neg) (buf-capacity pos)))

  (write-buf! [digest b]
    (let [offset (.position b)]
      ; Leave space for size headers
      (.position b (+ offset 8))
      ; Write negatives
      (write-buf! neg b)
      (let [neg-end (.position b)]
        ; Write positives
        (write-buf! pos b)
        (let [pos-end (.position b)]
          ; Write size headers
          (.putInt b offset (- neg-end offset 8))
          (.putInt b offset (- pos-end neg-end))))))

  (read-buf! [_ b]
    (let [neg-size (.getInt b)
          pos-size (.getInt b)]
      ; Use negative and positive as hints as to how to interpret b.
      (DualHistogram. (read-buf! neg b)
                      (read-buf! pos b)))))

(defn dual
  "HDRHistogram can't deal with negative values. This function takes a function
  to construct a quantile estimator and returns a quantile estimator that uses
  *two* of the underlying estimator, one for positive numbers, and one for
  negative numbers.

  (dual hdr-histogram {:significant-value-digits 4}))"
  ([h] (dual h {}))
  ([h opts]
   (DualHistogram. (h opts) (h opts))))
