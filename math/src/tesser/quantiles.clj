(ns tesser.quantiles
  "Supports various streaming quantile sketches"
  (:import (com.clearspring.analytics.stream.quantile QDigest)
           (org.HdrHistogram EncodableHistogram
                             DoubleHistogram)
           (com.tdunning.math.stats AVLTreeDigest)
           (java.nio ByteBuffer)
           (java.util.zip Deflater))
  (:require [tesser.utils :refer :all]
            [clojure.core.reducers :as r]
            [clojure.math.numeric-tower :refer :all]
            [clojure.math.combinatorics :as combo]
            [clojure.set  :as set]
            [clojure.core :as core]))

(defn ^"[B" byte-buffer->bytes
  "Convert a byte buffer to a byte array."
  [^ByteBuffer buffer]
  (let [array (byte-array (.remaining buffer))]
    (.get buffer array)
    array))

(defprotocol Buffers
  (buf-capacity [x] "How many bytes are we gonna need, tops?")
  (write-buf! [x ^ByteBuffer b] "Write x to a ByteBuffer. Mutates b by writing
                                bytes to the current position, leaving the
                                position advanced.")
  (read-buf!  [x ^ByteBuffer b] "Read a new instance of x from byte buffer b.
                                Mutates b by advancing the position."))

(defn write-buf
  "Write the given thing to a fresh ByteBuffer and returns that buffer, flipped
  so it's ready for reading."
  ([x]
   (let [buf (ByteBuffer/allocate (buf-capacity x))]
     (write-buf! x buf)
     (.flip buf)
     buf)))

(defprotocol Quantile
  (add-point!     [digest x] "Add a value to the given digest, mutating the
                         digest.")
  (merge-digest!  [digest other] "Merges the second digest into the first,
                                 mutating the first.")
  (point-count    [digest] "How many points went into this digest?")
  (quantile       [digest q]  "Returns a point near the given quantile."))

(extend-type AVLTreeDigest
  Quantile
  (add-point!       [digest x] (.add digest x))
  (merge-digest!    [digest ^AVLTreeDigest d] (.add digest d))
  (point-count      [digest]   (.size digest))
  (quantile         [digest q] (.quantile digest q))

  Buffers
  (buf-capacity     [digest]   (.smallByteSize digest))
  (write-buf!       [digest b] (.asSmallBytes digest b)
  (read-buf!        [digest b] (AVLTreeDigest/fromBytes b))))

(extend-type DoubleHistogram
  Quantile
  (add-point!       [digest x] (.recordValue digest x))
  (merge-digest!    [digest ^DoubleHistogram d] (.add digest d))
  (point-count      [digest]   (.getTotalCount digest))
  (quantile         [digest q] (.getValueAtPercentile digest (* q 100)))

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
  Quantile
  (add-point! [this x]
    (if (neg? x)
      (prn :storing x "in negatives")
      (prn :storing x "in positives"))
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

  (quantile [this q]
    (let [neg-count (point-count neg)
          pos-count (point-count pos)
          n         (+ neg-count pos-count)
          neg-frac  (/ neg-count n)
          pos-frac  (/ pos-count n)]
      (println "finding quantile" q ": neg" neg-count "pos" pos-count "total"
               n)
      (cond (zero? n)
            ; Do whatever the empty underlying Quantile impl does
            (quantile pos q)

            ; In negatives
            (and (pos? neg-count) (<= q neg-frac))
            (let [neg-q (- 1 (max 0 (- q (/ n))))]
              (println "quantile" q "mapped to neg quantile" neg-q "with value"
                       (- (quantile neg neg-q)))
              (- (quantile neg neg-q)))

            ; In positives
            true
            (do (println "quantile" q "mapped to pos quantile"
                         (- 1 (/ (- 1 q) pos-frac)) "with value"
                         (quantile pos (- 1 (/ (- 1 q) pos-frac))))
                (quantile pos (- 1 (/ (- 1 q) pos-frac)))))))
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
