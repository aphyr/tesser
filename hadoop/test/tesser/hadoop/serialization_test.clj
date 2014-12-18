(ns tesser.hadoop.serialization-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [clojure.test.check :as tc]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]]
            [tesser [core :as t]
                    [math :as m]
                    [quantiles :as q]]
            [tesser.hadoop.serialization :as s])
  (:import (org.HdrHistogram DoubleHistogram)
           (java.nio ByteBuffer)
           (java.util.zip Deflater)))

(def test-opts {:num-tests 1000
                :par 48})

(defspec digest-serialization-spec
  test-opts
  (prop/for-all [xs (gen/vector gen/pos-int)]
                (let [digest (q/dual q/hdr-histogram)]
                  ; Fill digest
                  (doseq [x xs]
                    (q/add-point! digest x))

                  ; Serialize and deserialize
                  (let [digest' (-> digest
                                    s/write-byte-array
                                    s/read-byte-array)]
                    (is (= digest digest'))))))
