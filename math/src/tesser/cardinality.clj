(ns tesser.cardinality
  "Cardinality estimate digest using HyperLogLog+.
  For examples, check tesser.math/digest."
  (:import [com.clearspring.analytics.stream.cardinality
            HyperLogLogPlus HyperLogLogPlus$Builder])
  (:require [tesser.quantiles :as q]))

(defn hll
  "Construct a new HLL cardinality estimator based on the improved HyperLogLog+
  algorithm which features sparse sets and bias correction.
  Optionally accepts an options map with keys:

  :p - Precision for the Normal set representation.
  :sp - Precision for the Sparse set representation.

  For the sparse representation:
  :p must be a value between 4 and :sp
  :sp must be less than 32.

  For old behaviour:
  :sp must be set to 0.

  Default:
  :p 16, :sp 0"
  (^HyperLogLogPlus [] (hll {:p 16 :sp 0}))
  (^HyperLogLogPlus
   [{:keys [p sp]}]
   {:pre [(or (zero? sp)                ;old behaviour
              (and (<= 4 p sp)
                   (< sp 32)))]}
   (HyperLogLogPlus. ^int p ^int sp)))


(extend-type HyperLogLogPlus
  q/Digest
  (add-point! [hll x] (doto hll (.offer x)))
  (merge-digest! [hll ^HyperLogLogPlus other] (doto hll (.addAll other)))
  (point-count [hll] (.cardinality hll)))


(defn to-byte-array
  "Convert the HLL estimator to a ByteArray"
  ^bytes [^HyperLogLogPlus hll]
  (.getBytes hll))


(defn from-byte-array
  "Convert a ByteArray into an HLL estimator instance."
  ^HyperLogLogPlus [^bytes bs]
  (HyperLogLogPlus$Builder/build bs))
