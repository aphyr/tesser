(ns tesser.utils-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]]
            [multiset.core :refer [multiset]]
            [tesser.utils :refer :all]))

(def test-opts {:num-tests 1000
                :par 256})

(defspec differences-spec
  test-opts
  (prop/for-all [coll (gen/such-that not-empty (gen/vector gen/int))]
                (is (= coll
                       (->> coll
                            differences
                            (cumulative-sums (first coll)))))))

(defspec partition-fast-spec
  test-opts
  (prop/for-all [coll (gen/one-of [(gen/list gen/int)
                                   (gen/vector gen/int)
                                   (gen/map gen/int gen/int)
                                   (gen/fmap long-array (gen/vector gen/int))])
                 n    (gen/choose 1 50)]
                (or (is (= (->> coll
                                (partition-all-fast n)
                                (map (partial into [])))
                           (partition-all n coll)))
                    (prn :n n :coll (seq coll)))))
