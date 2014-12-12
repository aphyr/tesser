(ns tesser.utils-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]]
            [multiset.core :refer [multiset]]
            [tesser.utils :refer :all]))

(def test-count 1e3)

(defspec differences-spec
  test-count
  (prop/for-all [coll (gen/such-that not-empty (gen/vector gen/int))]
                (is (= coll
                       (->> coll
                            differences
                            (cumulative-sums (first coll)))))))
