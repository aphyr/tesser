(ns tesser.simple-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]]
            [multiset.core :refer [multiset]]
            [tesser.simple :as s]
            [clojure.core.reducers :as r]
            [clojure.set :as set]))

(def test-opts {:num-tests 1000
                :par 256})

(def flat-ints (gen/one-of [(gen/list gen/int)
                            (gen/vector gen/int)
                            (gen/fmap long-array (gen/vector gen/int))]))

(defspec fold-spec
  test-opts
  (let [reducer (r/monoid conj hash-set)]
    (prop/for-all [xs flat-ints]
                  (and (is (= (r/fold + xs)
                              (s/fold + xs)))
                       (is (= (r/fold set/union reducer xs)
                              (s/fold set/union reducer xs)))))))

(defspec reduce-spec
  test-opts
  (prop/for-all [xs flat-ints]
                (and (is (= (reduce   + 0 xs)
                            (s/reduce + 0 xs))))))
