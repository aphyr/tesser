(ns tesser.utils-test
  (:require [tesser.utils :refer :all]
            [clojure.test :refer :all]
            [clojure.core.typed :as T]))

(deftest typecheck
  (is (T/check-ns 'tesser.utils)))
