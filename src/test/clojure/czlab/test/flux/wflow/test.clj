;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
;; Copyright © 2013-2022, Kenneth Leung. All rights reserved.

(ns czlab.test.flux.wflow.test

  (:require [clojure.test :as ct]
            [czlab.flux.core
             :as w :refer [defwflow wkflow runner]]
            [czlab.basal.proc :as p]
            [czlab.basal.util :as u]
            [czlab.basal.core
             :as c :refer [ensure?? ensure-thrown??]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- mksvr
  []
  (doto (p/scheduler<> "test" {:threads 4}) c/activate))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defwflow
  ^:private
  testWFlowSplitAndExpire
  (w/split-join<> [:type :and :wait-secs 2]
                  (w/script<> (c/fn_2 (c/do->nil (u/pause 1000)
                                                (c/setv ____2 :x 5))) "f-1000")
                  (w/script<> (c/fn_2 (c/do->nil (u/pause 4500)
                                                (c/setv ____2 :y 5))) "f-4500")
                  :expiry
                  (c/fn_2 (c/do->nil (c/setv ____2 :z 100)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defwflow
  ^:private
  testWFlowSplitAnd
  (w/split-join<> [:type :and :wait-secs 5]
                  #(c/do->nil (u/pause 1000)
                             (c/setv %2 :x 5))
                  #(c/do->nil (u/pause 1500)
                             (c/setv %2 :y 5)))
  #(c/let->nil
     [x (c/getv %2 :x)
      y (c/getv %2 :y)]
     (c/setv %2 :z (+ x y))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defwflow
  ^:private
  testWFlowSplitOr
  (w/split-join<> [:type :or]
                  #(c/do->nil (u/pause 1000)
                             (c/setv %2 :a 10))
                  #(c/do->nil (u/pause 3500)
                             (c/setv %2 :b 5)))
  #(c/do->nil (assert (and (c/has? %2 :a)
                          (not (c/has? %2 :b))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defwflow
  ^:private
  testWFlowIftrue
  (w/decision<> #(c/do->true %)
                #(c/do->nil (c/setv %2 :a 10))
                #(c/do->nil (c/setv %2 :a 5))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defwflow
  ^:private
  testWFlowIffalse
  (w/decision<> #(c/do->false %)
                #(c/do->nil (c/setv %2 :a 5))
                (w/script<> #(c/do->nil
                               (c/setv %2 :a 10)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defwflow
  ^:private
  testWFlowSwitchfound
  (w/choice<> #(do % "z")
              "y" (w/script<> #(c/do->nil %1 %2))
              "z" #(c/do->nil (c/setv %2 :z 10))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defwflow
  ^:private
  testWFlowSwitchdefault
  (w/choice<> #(do % "z")
              "x" #(c/do->nil %1 %2)
              "y" #(c/do->nil %1 %2)
              :default
              (w/script<> #(c/do->nil
                             (c/setv %2 :z 10)) "dft")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defwflow
  ^:private
  _testWFlowFor
  (w/for<> #(do % 0)
           #(do % 10)
           (w/script<> #(c/do->nil
                            (->> (inc (c/getv %2 :z))
                                 (c/setv %2 :z))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defwflow
  ^:private
  _testWFlowWhile
  (w/while<> #(< (c/getv %1 :cnt) 10)
             (w/script<> #(c/do->nil
                            (->> (inc (c/getv %2 :cnt))
                                 (c/setv %2 :cnt))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defwflow
  ^:private
  _testWFlowDelay
  (w/postpone<> 2)
  #(c/do->nil (->> (u/system-time)
                  (c/setv %2 :time))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/deftest test-core

  (ensure?? "split;expire"
            (= 100 (let [job (w/job<> (mksvr))]
                     (w/exec testWFlowSplitAndExpire job)
                     (u/pause 8000)
                     (c/finz (runner job))
                     (c/getv job :z))))

  (ensure?? "split;and" (== 10 (let [job (w/job<> (mksvr))]
                                 (w/exec testWFlowSplitAnd job)
                                 (u/pause 3500)
                                 (c/finz (runner job))
                                 (c/getv job :z))))

  (ensure?? "split;or" (== 10 (let [job (w/job<> (mksvr))]
                                (w/exec testWFlowSplitOr job)
                                (u/pause 2000)
                                (c/finz (runner job))
                                (c/getv job :a))))

  (ensure?? "switch;default" (== 10 (let [job (w/job<> (mksvr))]
                                      (w/exec testWFlowSwitchdefault job)
                                      (u/pause 2500)
                                      (c/finz (runner job))
                                      (c/getv job :z))))

  (ensure?? "switch;found" (== 10 (let [job (w/job<> (mksvr))]
                                    (w/exec testWFlowSwitchfound job)
                                    (u/pause 2500)
                                    (c/finz (runner job))
                                    (c/getv job :z))))

  (ensure?? "if;false" (== 10 (let [job (w/job<> (mksvr))]
                                (w/exec testWFlowIffalse job)
                                (u/pause 1500)
                                (c/finz (runner job))
                                (c/getv job :a))))

  (ensure?? "if;true" (== 10 (let [job (w/job<> (mksvr))]
                               (w/exec testWFlowIftrue job)
                               (u/pause 1500)
                               (c/finz (runner job))
                               (c/getv job :a))))

  (ensure?? "loop;while" (== 10 (let [job (w/job<> (mksvr) {:cnt 0})]
                                  (w/exec _testWFlowWhile job)
                                  (u/pause 2500)
                                  (c/finz (runner job))
                                  (c/getv job :cnt))))

  (ensure?? "loop;for" (== 10 (let [job (w/job<> (mksvr))]
                                (c/setv job :z 0)
                                (w/exec _testWFlowFor job)
                                (u/pause 2500)
                                (c/finz (runner job))
                                (c/getv job :z))))

  (ensure?? "delay" (let [now (u/system-time)
                          job (w/job<> (mksvr) {:time -1})]
                      (w/exec _testWFlowDelay job)
                      (u/pause 2500)
                      (c/finz (runner job))
                      (let [x (- (c/getv job :time) now)]
                        (and (> x 2000) (< x 3000)))))

  (ensure?? "test-end" (== 1 1)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(ct/deftest
  ^:test-core flux-test-core
  (ct/is (c/clj-test?? test-core)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;EOF


