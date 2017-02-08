;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns czlab.test.flux.wflow.test

  (:require [czlab.flux.wflow.core :refer :all]
            [czlab.basal.logging :as log])

  (:use [czlab.basal.scheduler]
        [czlab.basal.process]
        [czlab.basal.core]
        [clojure.test])

  (:import [czlab.jasal Activable Schedulable CU]
           [czlab.flux.wflow
            Step
            Job
            StepError
            BoolExpr
            RangeExpr
            ChoiceExpr]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mksvr
  ""
  ^Schedulable
  []
  (doto (scheduler<> "test") (.activate nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(comment
(let [svr (mksvr)
      ws
      (workStream<>
        (script<> #(do->nil
                     %1 %2
                     (println "dddddddddddddddd")
                     ))
        (script<> #(do->nil
                     %1 %2
                     (println "ffffffffffffffff")
                     )))
      job (job<> svr ws)]
  (.execWith ws job)
  (pause 3000)
  (println "dispose")
  (.dispose svr)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSplit->And->Expire
  "should return 100"
  []
  (let [ws
        (workStream<>
          (fork<>
            {:join :and
             :waitSecs 2}
            (script<> #(do->nil
                         (pause 1000)
                         (.setv ^Job %2 :x 5)))
            (script<> #(do->nil
                         (pause 4500)
                         (.setv ^Job %2 :y 5))))
          (script<> #(do->nil
                      (->> (+ (.getv ^Job %2 :x)
                              (.getv ^Job %2 :y))
                           (.setv ^Job %2 :z ))))
          :catch
          (fn [^StepError e]
            (let [^Step s (.lastStep e)
                  j (.job s)]
              (.setv j :z 100))))
        svr (mksvr)
        job (job<> svr ws)]
    (.execWith ws job)
    (pause 3000)
    (.dispose svr)
    (.getv job :z)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSplit->And
  "should return 10"
  []
  (let [ws
        (workStream<>
          (fork<> {:join :and}
                  (script<> #(do->nil
                               (pause 1000)
                               (.setv ^Job %2 :x 5)))
                  (script<> #(do->nil
                               (pause 1500)
                               (.setv ^Job %2 :y 5))))
          (script<> #(do->nil
                       (->> (+ (.getv ^Job %2 :x)
                               (.getv ^Job %2 :y))
                            (.setv ^Job %2 :z )))))
        svr (mksvr)
        job (job<> svr ws)]
    (.execWith ws job)
    (pause 3000)
    (.dispose svr)
    (.getv job :z)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSplit->Or
  "should return 10"
  []

  (let [ws
        (workStream<>
          (fork<> {:join :or}
                  (script<> #(do->nil
                               (pause 1000)
                               (.setv ^Job %2 :a 10)))
                  (script<> #(do->nil
                               (pause 5000)
                               (.setv ^Job %2 :b 5))))
          (script<> #(do->nil
                       (assert (.contains ^Job %2 :a)))))
        svr (mksvr)
        job (job<> svr ws)]
    (.execWith ws job)
    (pause 2500)
    (.dispose svr)
    (.getv job :a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowIf->true
  "should return 10"
  []
  (let [ws
        (workStream<>
          (ternary<>
            (reify BoolExpr (ptest [_ j] true))
            (script<> #(do->nil
                         (.setv ^Job %2 :a 10)))
            (script<> #(do->nil
                         (.setv ^Job %2 :a 5)))))
        svr (mksvr)
        job (job<> svr ws)]
    (.execWith ws job)
    (pause 1500)
    (.dispose svr)
    (.getv job :a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowIf->false
  "should return 10"
  []
  (let [ws
        (workStream<>
          (ternary<>
            (reify BoolExpr (ptest [_ j] false))
            (script<> #(do->nil
                         (.setv ^Job %2 :a 5)))
            (script<> #(do->nil
                         (.setv ^Job %2 :a 10)))))
        svr (mksvr)
        job (job<> svr ws)]
    (.execWith ws job)
    (pause 1500)
    (.dispose svr)
    (.getv job :a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSwitch->found
  ""
  []

  (let [ws
        (workStream<>
          (choice<>
            (reify ChoiceExpr (choose [_ j] "z"))
            nil
            "y" (script<> #(do->nil %1 %2 ))
            "z" (script<> #(do->nil
                             (.setv ^Job %2 :z 10)))))
        svr (mksvr)
        job (job<> svr ws)]
    (.execWith ws job)
    (pause 2500)
    (.dispose svr)
    (.getv job :z)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSwitch->default
  ""
  []

  (let [ws
        (workStream<>
          (choice<>
            (reify ChoiceExpr (choose [_ j] "z"))
            (script<> #(do->nil
                         (.setv ^Job %2 :z 10)), "dft")
            "x" (script<> #(do->nil %1 %2 ))
            "y" (script<> #(do->nil %1 %2 ))))
        svr (mksvr)
        job (job<> svr ws)]
    (.execWith ws job)
    (pause 2500)
    (.dispose svr)
    (.getv job :z)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowFor
  "should return 10"
  []

  (let [ws
        (workStream<>
          (floop<>
            (reify RangeExpr
              (lower [_ j] 0)
              (upper [_ j] 10))
            (script<> #(do->nil
                         (->>
                           (inc (.getv ^Job %2 :z))
                           (.setv ^Job %2 :z ))))))
        svr (mksvr)
        job (job<> svr ws)]
    (.setv job :z 0)
    (.execWith ws job)
    (pause 2500)
    (.dispose svr)
    (.getv job :z)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowWhile
  "should return 10"
  []

  (let [ws
        (workStream<>
          (wloop<>
            (reify BoolExpr
              (ptest [_ j]
                (< (.getv ^Job j :cnt) 10)))
            (script<> #(do->nil
                         (->>
                           (inc (.getv ^Job %2 :cnt))
                           (.setv ^Job %2 :cnt))))))
        svr (mksvr)
        job (job<> svr ws)]
    (.setv job :cnt 0)
    (.execWith ws job)
    (pause 2500)
    (.dispose svr)
    (.getv job :cnt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowDelay
  "should return "
  []

  (let [now (System/currentTimeMillis)
        ws
        (workStream<>
          (postpone<> 2)
          (script<> #(do->nil
                       (->> (System/currentTimeMillis)
                            (.setv ^Job %2 :time)))))
        svr (mksvr)
        job (job<> svr ws)]
    (.setv job :time -1)
    (.execWith ws job)
    (pause 2500)
    (.dispose svr)
    (- (.getv job :time) now)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(deftest czlabtestfluxwflow-test

  (testing
    "related to: split"
    (is (== 100 (testWFlowSplit->And->Expire)))
    (is (== 10 (testWFlowSplit->And)))
    (is (== 10 (testWFlowSplit->Or))))

  (testing
    "related to: switch"
    (is (== 10 (testWFlowSwitch->default)))
    (is (== 10 (testWFlowSwitch->found))))

  (testing
    "related to: if"
    (is (== 10 (testWFlowIf->false)))
    (is (== 10 (testWFlowIf->true))))

  (testing
    "related to: loop"
    (is (== 10 (testWFlowWhile)))
    (is (== 10 (testWFlowFor))))

  (testing
    "related to: delay"
    (is (let [x (testWFlowDelay)]
          (and (> x 2000)
               (< x 3000)))))

  (is (string? "That's all folks!")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;EOF


