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
;; Copyright (c) 2013-2016, Kenneth Leung. All rights reserved.

(ns czlabtest.wflow.test

  (:require
    [czlab.wflow.core :refer :all]
    [czlab.xlib.logging :as log])

  (:use [czlab.xlib.scheduler]
        [czlab.xlib.process]
        [czlab.xlib.core]
        [clojure.test])

  (:import
    [czlab.server ServerLike]
    [czlab.wflow
     Step
     Job
     StepError
     BoolExpr
     RangeExpr
     ChoiceExpr]
    [czlab.xlib Activable Schedulable CU]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mksvr
  ""
  ^ServerLike
  []
  (let [_c (mkScheduler "test")]
    (.activate _c {})
    (reify ServerLike (core [_] _c))))

(comment
(let [svr (mksvr)
      ws
      (workStream->
        (script #(do->nil
                   %1 %2
                   (println "dddddddddddddddd")
                   ))
        (script #(do->nil
                   %1 %2
                   (println "ffffffffffffffff")
                   ))
        )
      job (createJob svr ws)
      end (nihilStep job)]
  (.run (.core svr)
          (.create (.startWith ws) end))
  (safeWait 3000)
  (println "dispose")
  (.dispose (.core svr))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSplit->And->Expire
  "should return 100"
  []
  (let [ws
        (workStream->
          (fork
            {:join :and
             :waitSecs 2}
            (script #(do->nil
                       (safeWait 1000)
                       (.setv ^Job %2 :x 5)))
            (script #(do->nil
                       (safeWait 4500)
                       (.setv ^Job %2 :y 5))))
          (script #(do->nil
                    (->> (+ (.getv ^Job %2 :x)
                            (.getv ^Job %2 :y))
                         (.setv ^Job %2 :z ))))
          {:error
           (fn [^StepError e]
             (let [^Step s (.lastStep e)
                   j (.job s)]
               (.setv j :z 100)))})
        svr (mksvr)
        job (createJob svr ws)
        end (nihilStep job)]
    (.run (.core svr)
          (.create (.startWith ws) end))
    (safeWait 3000)
    (.dispose (.core svr))
    (.getv job :z)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSplit->And
  "should return 10"
  []
  (let [ws
        (workStream->
          (fork {:join :and}
                (script #(do->nil
                          (safeWait 1000)
                          (.setv ^Job %2 :x 5)))
                (script #(do->nil
                          (safeWait 1500)
                          (.setv ^Job %2 :y 5))))
          (script #(do->nil
                    (->> (+ (.getv ^Job %2 :x)
                            (.getv ^Job %2 :y))
                         (.setv ^Job %2 :z )))))
        svr (mksvr)
        job (createJob svr ws)
        end (nihilStep job)]
    (.run (.core svr)
          (.create (.startWith ws) end))
    (safeWait 2500)
    (.dispose (.core svr))
    (.getv job :z)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSplit->Or
  "should return 10"
  []

  (let [ws
        (workStream->
          (fork {:join :or}
                (script #(do->nil
                          (safeWait 1000)
                          (.setv ^Job %2 :a 10)))
                (script #(do->nil
                          (safeWait 5000)
                          (.setv ^Job %2 :b 5))))
          (script #(do->nil
                    (assert (.contains ^Job %2 :a)))))
        svr (mksvr)
        job (createJob svr ws)
        end (.createEx (nihil) job)]
    (.run (.core svr)
          (.create (.startWith ws) end))
    (safeWait 2500)
    (.dispose (.core svr))
    (.getv job :a)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowIf->true
  "should return 10"
  []
  (let [ws
        (workStream->
          (ternary
            (reify BoolExpr (ptest [_ j] true))
            (script #(do->nil
                      (.setv ^Job %2 :a 10)))
            (script #(do->nil
                      (.setv ^Job %2 :a 5)))))
        svr (mksvr)
        job (createJob svr ws)
        end (.createEx (nihil) job)]
    (.run (.core svr)
          (.create (.startWith ws) end))
    (safeWait 1500)
    (.dispose (.core svr))
    (.getv job :a)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowIf->false
  "should return 10"
  []
  (let [ws
        (workStream->
          (ternary
            (reify BoolExpr (ptest [_ j] false))
            (script #(do->nil
                      (.setv ^Job %2 :a 5)))
            (script #(do->nil
                      (.setv ^Job %2 :a 10)))))
        svr (mksvr)
        job (createJob svr ws)
        end (.createEx (nihil) job)]
    (.run (.core svr)
          (.create (.startWith ws) end))
    (safeWait 1500)
    (.dispose (.core svr))
    (.getv job :a)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSwitch->found
  ""
  []

  (let [ws
        (workStream->
          (choice
            (reify ChoiceExpr (choose [_ j] "z"))
            nil
            "y" (script #(do->nil %1 %2 ))
            "z" (script #(do->nil
                          (.setv ^Job %2 :z 10)))))
        svr (mksvr)
        job (createJob svr ws)
        end (.createEx (nihil) job)]
    (.run (.core svr)
          (.create (.startWith ws) end))
    (safeWait 2500)
    (.dispose (.core svr))
    (.getv job :z)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSwitch->default
  ""
  []

  (let [ws
        (workStream->
          (choice
            (reify ChoiceExpr (choose [_ j] "z"))
            (script #(do->nil
                          (.setv ^Job %2 :z 10)), "dft")
            "x" (script #(do->nil %1 %2 ))
            "y" (script #(do->nil %1 %2 ))))
        svr (mksvr)
        job (createJob svr ws)
        end (.createEx (nihil) job)]
    (.run (.core svr)
          (.create (.startWith ws) end))
    (safeWait 2500)
    (.dispose (.core svr))
    (.getv job :z)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowFor
  "should return 10"
  []

  (let [ws
        (workStream->
          (floop
            (reify RangeExpr
              (lower [_ j] 0)
              (upper [_ j] 10))
            (script #(do->nil
                      (->>
                        (inc (.getv ^Job %2 :z))
                        (.setv ^Job %2 :z ))))))
        svr (mksvr)
        job (createJob svr ws)
        end (.createEx (nihil) job)]
    (.setv job :z 0)
    (.run (.core svr)
          (.create (.startWith ws) end))
    (safeWait 2500)
    (.dispose (.core svr))
    (.getv job :z)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowWhile
  "should return 10"
  []

  (let [ws
        (workStream->
          (wloop
            (reify BoolExpr
              (ptest [_ j]
                (< (.getv ^Job j :cnt) 10)))
            (script #(do->nil
                      (->>
                        (inc (.getv ^Job %2 :cnt))
                        (.setv ^Job %2 :cnt))))))
        svr (mksvr)
        job (createJob svr ws)
        end (.createEx (nihil) job)]
    (.setv job :cnt 0)
    (.run (.core svr)
          (.create (.startWith ws) end))
    (safeWait 2500)
    (.dispose (.core svr))
    (.getv job :cnt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(deftest czlabtestwflow-test

  (is (== 100 (testWFlowSplit->And->Expire)))
  (is (== 10 (testWFlowSplit->And)))
  (is (== 10 (testWFlowSplit->Or)))

  (is (== 10 (testWFlowSwitch->default)))
  (is (== 10 (testWFlowSwitch->found)))

  (is (== 10 (testWFlowIf->false)))
  (is (== 10 (testWFlowIf->true)))

  (is (== 10 (testWFlowWhile)))
  (is (== 10 (testWFlowFor)))

)

;;(clojure.test/run-tests 'czlabtest.wflow.test)

