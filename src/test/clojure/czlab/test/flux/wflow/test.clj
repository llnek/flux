;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns czlab.test.flux.wflow.test

  (:require [czlab.basal.scheduler :as r]
            [czlab.basal.process :as p]
            [czlab.basal.core :as c]
            [czlab.flux.wflow :as w])

  (:use [clojure.test])

  (:import [czlab.jasal Schedulable CU]
           [czlab.basal Stateful]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mksvr "" ^Schedulable [] (doto (r/scheduler<> "test") .activate))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSplit->And->Expire
  "should return 100"
  []
  (let [ws
        (w/workstream<>
          (w/fork<>
            {:join :and
             :waitSecs 2}
            #(c/do->nil (c/pause 1000)
                        (c/alter-atomic % assoc :x 5))
            #(c/do->nil (c/pause 4500)
                        (c/alter-atomic % assoc :y 5)))
          #(c/do->nil
             (->> (+ (:x @%) (:y @%))
                  (c/alter-atomic % assoc :z )))
          :catch
          (fn [{:keys [cog error]}]
            (let [j (w/gjob cog)]
              (c/alter-atomic j assoc :z 100))))
        svr (mksvr)
        job (w/job<> svr ws)]
    (w/exec-with ws job)
    (c/pause 2500)
    (.dispose svr)
    (:z @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSplit->And
  "should return 10"
  []
  (let [ws
        (w/workstream<>
          (w/fork<> :and
                  #(c/do->nil (c/pause 1000)
                              (c/alter-atomic % assoc :x 5))
                  #(c/do->nil (c/pause 1500)
                              (c/alter-atomic % assoc :y 5)))
          #(c/do->nil
             (->> (+ (:x @%) (:y @%))
                  (c/alter-atomic % assoc :z))))
        svr (mksvr)
        job (w/job<> svr ws)]
    (w/exec-with ws job)
    (c/pause 3500)
    (.dispose svr)
    (:z @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSplit->Or
  "should return 10"
  []
  (let [ws
        (w/workstream<>
          (w/fork<> :or
                  #(c/do->nil (c/pause 1000)
                            (c/alter-atomic % assoc :a 10))
                  #(c/do->nil (c/pause 3500)
                            (c/alter-atomic % assoc :b 5)))
          #(c/do->nil (assert (contains? @% :a))))
        svr (mksvr)
        job (w/job<> svr ws)]
    (w/exec-with ws job)
    (c/pause 2000)
    (.dispose svr)
    (:a @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowIf->true
  "should return 10"
  []
  (let [ws
        (w/workstream<>
          (w/decision<>
            #(do % true)
            #(c/do->nil (c/alter-atomic % assoc :a 10))
            #(c/do->nil (c/alter-atomic % assoc :a 5))))
        svr (mksvr)
        job (w/job<> svr ws)]
    (w/exec-with ws job)
    (c/pause 1500)
    (.dispose svr)
    (:a @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowIf->false
  "should return 10"
  []
  (let [ws
        (w/workstream<>
          (w/decision<>
            #(do % false)
            #(c/do->nil (c/alter-atomic % assoc :a 5))
            (w/script<> #(c/do->nil
                         (c/alter-atomic %2 assoc :a 10)))))
        svr (mksvr)
        job (w/job<> svr ws)]
    (w/exec-with ws job)
    (c/pause 1500)
    (.dispose svr)
    (:a @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSwitch->found "" []
  (let [ws
        (w/workstream<>
          (w/choice<>
            #(do % "z")
            nil
            "y" (w/script<> #(c/do->nil %1 %2 ))
            "z" #(c/do->nil (c/alter-atomic % assoc :z 10))))
        svr (mksvr)
        job (w/job<> svr ws)]
    (w/exec-with ws job)
    (c/pause 2500)
    (.dispose svr)
    (:z @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSwitch->default "" []
  (let [ws
        (w/workstream<>
          (w/choice<>
            #(do % "z")
            (w/script<> #(c/do->nil
                         (c/alter-atomic % assoc :z 10)), "dft")
            "x" #(c/do->nil %1 %2 )
            "y" #(c/do->nil %1 %2 )))
        svr (mksvr)
        job (w/job<> svr ws)]
    (w/exec-with ws job)
    (c/pause 2500)
    (.dispose svr)
    (:z @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowFor
  "should return 10"
  []
  (let [ws
        (w/workstream<>
          (w/floop<>
            #(do % 0)
            #(do % 10)
            (w/script<> #(c/do->nil
                           (->>
                             (inc (:z @%))
                             (c/alter-atomic % assoc :z))))))
        svr (mksvr)
        job (w/job<> svr ws)]
    (c/alter-atomic job assoc :z 0)
    (w/exec-with ws job)
    (c/pause 2500)
    (.dispose svr)
    (:z @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowWhile
  "should return 10"
  []
  (let [ws
        (w/workstream<>
          (w/wloop<>
            #(< (:cnt @%) 10)
            (w/script<> #(c/do->nil
                           (->>
                             (inc (:cnt @%2))
                             (c/alter-atomic %2 assoc :cnt))))))
        svr (mksvr)
        job (w/job<> svr ws)]
    (c/alter-atomic job assoc :cnt 0)
    (w/exec-with ws job)
    (c/pause 2500)
    (.dispose svr)
    (:cnt @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowDelay
  "should return "
  []
  (let [now (System/currentTimeMillis)
        ws
        (w/workstream<>
          (w/postpone<> 2)
          #(c/do->nil (->> (System/currentTimeMillis)
                           (c/alter-atomic % assoc :time))))
        svr (mksvr)
        job (w/job<> svr ws)]
    (c/alter-atomic job assoc :time -1)
    (w/exec-with ws job)
    (c/pause 2500)
    (.dispose svr)
    (- (:time @job) now)))

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


