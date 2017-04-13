;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns czlab.test.flux.wflow.test

  (:require [czlab.basal.logging :as log])

  (:use [czlab.basal.scheduler]
        [czlab.basal.process]
        [czlab.basal.core]
        [czlab.flux.wflow]
        [clojure.test])

  (:import [czlab.jasal Schedulable CU]
           [czlab.basal Stateful]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mksvr "" ^Schedulable [] (doto (scheduler<> "test") .activate))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSplit->And->Expire
  "should return 100"
  []
  (let [ws
        (workstream<>
          (fork<>
            {:join :and
             :waitSecs 2}
            #(do->nil (pause 1000)
                      (alterStateful % assoc :x 5))
            #(do->nil (pause 4500)
                      (alterStateful % assoc :y 5)))
          #(do->nil
             (->> (+ (:x @%) (:y @%))
                  (alterStateful % assoc :z )))
          :catch
          (fn [{:keys [cog error]}]
            (let [j (gjob cog)]
              (alterStateful j assoc :z 100))))
        svr (mksvr)
        job (job<> svr ws)]
    (.execWith ws job)
    (pause 2500)
    (.dispose svr)
    (:z @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSplit->And
  "should return 10"
  []
  (let [ws
        (workstream<>
          (fork<> :and
                  #(do->nil (pause 1000)
                            (alterStateful % assoc :x 5))
                  #(do->nil (pause 1500)
                            (alterStateful % assoc :y 5)))
          #(do->nil
             (->> (+ (:x @%) (:y @%))
                  (alterStateful % assoc :z))))
        svr (mksvr)
        job (job<> svr ws)]
    (.execWith ws job)
    (pause 3500)
    (.dispose svr)
    (:z @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSplit->Or
  "should return 10"
  []
  (let [ws
        (workstream<>
          (fork<> :or
                  #(do->nil (pause 1000)
                            (alterStateful % assoc :a 10))
                  #(do->nil (pause 3500)
                            (alterStateful % assoc :b 5)))
          #(do->nil (assert (contains? @% :a))))
        svr (mksvr)
        job (job<> svr ws)]
    (.execWith ws job)
    (pause 2000)
    (.dispose svr)
    (:a @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowIf->true
  "should return 10"
  []
  (let [ws
        (workstream<>
          (decision<>
            #(do % true)
            #(do->nil (alterStateful % assoc :a 10))
            #(do->nil (alterStateful % assoc :a 5))))
        svr (mksvr)
        job (job<> svr ws)]
    (.execWith ws job)
    (pause 1500)
    (.dispose svr)
    (:a @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowIf->false
  "should return 10"
  []
  (let [ws
        (workstream<>
          (decision<>
            #(do % false)
            #(do->nil (alterStateful % assoc :a 5))
            (script<> #(do->nil
                         (alterStateful %2 assoc :a 10)))))
        svr (mksvr)
        job (job<> svr ws)]
    (.execWith ws job)
    (pause 1500)
    (.dispose svr)
    (:a @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSwitch->found "" []
  (let [ws
        (workstream<>
          (choice<>
            #(do % "z")
            nil
            "y" (script<> #(do->nil %1 %2 ))
            "z" #(do->nil (alterStateful % assoc :z 10))))
        svr (mksvr)
        job (job<> svr ws)]
    (.execWith ws job)
    (pause 2500)
    (.dispose svr)
    (:z @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSwitch->default "" []
  (let [ws
        (workstream<>
          (choice<>
            #(do % "z")
            (script<> #(do->nil
                         (alterStateful % assoc :z 10)), "dft")
            "x" #(do->nil %1 %2 )
            "y" #(do->nil %1 %2 )))
        svr (mksvr)
        job (job<> svr ws)]
    (.execWith ws job)
    (pause 2500)
    (.dispose svr)
    (:z @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowFor
  "should return 10"
  []
  (let [ws
        (workstream<>
          (floop<>
            #(do % 0)
            #(do % 10)
            (script<> #(do->nil
                         (->>
                           (inc (:z @%))
                           (alterStateful % assoc :z))))))
        svr (mksvr)
        job (job<> svr ws)]
    (alterStateful job assoc :z 0)
    (.execWith ws job)
    (pause 2500)
    (.dispose svr)
    (:z @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowWhile
  "should return 10"
  []
  (let [ws
        (workstream<>
          (wloop<>
            #(< (:cnt @%) 10)
            (script<> #(do->nil
                         (->>
                           (inc (:cnt @%2))
                           (alterStateful %2 assoc :cnt))))))
        svr (mksvr)
        job (job<> svr ws)]
    (alterStateful job assoc :cnt 0)
    (.execWith ws job)
    (pause 2500)
    (.dispose svr)
    (:cnt @job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowDelay
  "should return "
  []
  (let [now (System/currentTimeMillis)
        ws
        (workstream<>
          (postpone<> 2)
          #(do->nil (->> (System/currentTimeMillis)
                         (alterStateful % assoc :time))))
        svr (mksvr)
        job (job<> svr ws)]
    (alterStateful job assoc :time -1)
    (.execWith ws job)
    (pause 2500)
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


