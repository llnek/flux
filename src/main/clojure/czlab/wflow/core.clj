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

(ns ^{:doc "A Minimal worflow framework."
      :author "Kenneth Leung"}

  czlab.wflow

  (:require
    [czlab.xlib.str :refer [stror hgl?]]
    [czlab.xlib.logging :as log]
    [czlab.xlib.core
     :refer [inst?
             cast?]]
    [clojure.java.io :as io]
    [clojure.string :as cs])

  (:import
    [java.util.concurrent.atomic AtomicInteger]
    [czlab.server ServerLike ServiceHandler]
    [czlab.wflow
     Innards
     Switch
     Delay
     Job
     Group
     If
     While
     PTask
     Split
     AndJoin
     OrJoin
     Join
     Nihil
     NulJoin
     TaskDef
     Step
     StepError
     BoolExpr
     ChoiceExpr
     CounterExpr]
    [czlab.xlib
     Initable
     Named
     CU
     Schedulable]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

(declare nihil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro reinit!

  ""
  {:private true}
  [^Initable a ^Step s]

  `(.init ~a ~s))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro rerun!

  ""
  [s]

  `(let [^Step s# ~s]
    (-> (.job s#)
        (.container )
        (.core )
        (.reschedule s#))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro gjob

  ""
  ^Job
  [s j]

  `(let [n# (.next ^Step ~s)]
    (or (some-> n# (.job n#)) ~j)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stepRunAfter

  ""
  [^Step this]

  (let [cpu (.core (.container (.job this)))
        np (.next this)]
    (cond
      (inst? Delay (.proto this))
      (->> (:delay (.attrs this))
           (* 1000 )
           (.postpone cpu np))

      (inst? Nihil (.proto this))
      nil

      :else
      (.run cpu this))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stepRun

  ""
  [^Step this]

  (with-local-vars [err nil
                    rc nil svc nil]
    (let [j (.job this)
          par (.container j)
          d (.proto this)]
      (.dequeue (.core par) this)
      (try
        (log/debug "%s :handle()" (.getName ^Named d))
        (var-set rc (.handle this j))
      (catch Throwable e#
        (when-some [^ServiceHandler
                    svc (cast? ServiceHandler par)]
          (let [ret (->> (StepError. this e#)
                         (.handleError svc))]
            (var-set err (cast? TaskDef ret))))
        (when (nil? @err) (var-set err (nihil)))
        (var-set rc (.reify ^TaskDef
                            @err
                            (.create (nihil) j))))))
    (if (nil? @rc)
      (log/debug "step: rc==null => skip")
      (stepRunAfter @rc))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti stepize

  ""
  {:private true
   :tag Step}

  (fn [a b & xs] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Nihil
  [^TaskDef actDef ^Step nxtStep & [_job]]

  (let [pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m])

      Step

      (job [this] (gjob this _job))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (handle [this j] this)
      (setNext [_ n] )
      (proto [_] actDef)
      (attrs [_] nil)
      (id [_] pid)
      (next [this] this))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn nihil

  "Create a special Terminator Task"
  ^Nihil
  []

  (reify

    Initable

    (init [_ m] )

    Nihil

    (create [this c] (stepize this c))
    (getName [_] "nihil")
    (create [this j] (stepize this nil j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Delay
  [^TaskDef actDef ^Step nxtStep & [_job]]

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (setNext [_ n] (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (handle [this j]
        (reinit! actDef this)
        this)
      (attrs [_] (:vars @info))
      (id [_] pid)
      (job [this] (gjob this _job))
      (proto [_] actDef)
      (next [_] (:next @info)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn postpone

  "Create a Delay Task"
  ^Delay
  [delaySecs]
  {:pre [(>= delaySecs 0)]}

  (reify

    Initable

    (init [_ s]
      (->> {:delay delaySecs}
           (.init ^Initable s)))

    Delay

    (create [this c] (stepize this c))
    (getName [_] "delay")
    (create [this j] (stepize this j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  PTask
  [^TaskDef actDef ^Step nxtStep & [_job]]

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (setNext [_ n] (swap! info assoc :next n))
      (job [this] (gjob this _job))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (attrs [_] (:vars @info))
      (id [_] pid)
      (proto [_] actDef)
      (next [_] (:next @info))
      (handle [this j]
        (let [a (-> (get-in @info [:vars :work])
                    (apply this j []))
              rc (.next this)]
          (reinit! actDef this)
          (if
            (inst? TaskDef a)
            (.create ^TaskDef a rc)
            rc))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn ptask

  "Create a Programmable Task"
  ^PTask
  [workFunc &[nm]]
  {:pre [(fn? workFunc)]}

  (reify

    Initable

    (init [_ s]
      (->> {:work workFunc}
           (.init ^Initable s)))

    PTask

    (create [this c] (stepize this c))
    (getName [_] (stror nm "ptask"))
    (create [this j] (stepize this j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Switch
  [^TaskDef actDef ^Step nxtStep & [_job]]

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (setNext [_ n] (swap! info assoc :next n))
      (job [this] (gjob this _job))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (next [_] (:next @info))
      (attrs [_] (:vars @info))
      (id [_] pid)
      (proto [_] actDef)
      (handle [_ j]
        (let [cs (get-in @info [:vars :choices])
              dft (get-in @info [:vars :dft])
              e (get-in @info [:vars :expr])
              m (.choice ^ChoiceExpr e ^Job j)
              a (if (some? m)
                  (some #(if (= m (first %1))
                           (last %1) nil)
                        cs)
                  nil)]
          (reinit! actDef this)
          (or a dft))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn choice

  "Create a Switch Task"
  ^Switch
  [^ChoiceExpr cexpr ^TaskDef dft & choices]

  (let [cpairs (partition 2 choices)]
    (reify

      Initable

      (init [_ s]
        (->> {:expr cexpr
              :dft dft
              :choices cpairs}
             (.init ^Step s)))

      Switch

      (create [this c] (stepize this c))
      (getName [_] "switch")
      (create [this j] (stepize this j)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  NulJoin
  [^TaskDef actDef ^Step nxtStep & [_job]]

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable
      (init [_ m] )

      Step

      (setNext [_ n] (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (attrs [_] (:vars @info))
      (next [_] (:next @info))
      (id [_] pid)
      (proto [_] actDef)
      (job [this] (gjob this _job))
      (handle [_ j] nil))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn nuljoin

  "Create a do-nothing Join Task"
  ^NulJoin
  []

  (reify

    Initable

    (init [_ s] )

    NulJoin

    (create [this c] (stepize this c))
    (getName [_] "nuljoin")
    (create [this c] (stepize this j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  AndJoin
  [^TaskDef actDef ^Step nxtStep & [_job]]

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (setNext [_ n] (swap! info assoc :next n))
      (job [this] (gjob this _job))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (attrs [_] (:vars @info))
      (next [_] (:next @info))
      (proto [_] actDef)
      (id [_] pid)
      (handle [this j]
        (let [b (get-in @info [:vars :branches])
              y (get-in @info [:vars :body])
              c (get-in @info [:vars :cnt])
              nv (.incrementAndGet ^AtomicInteger c)]
          (if (== nv b)
            (do
              (reinit! actDef this)
              (or y (.next this)))
            nil))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn andjoin

  "Create a And Join Task"
  ^AndJoin
  [^long branches & [^TaskDef body]]

  (reify

    Initable

    (init [_ s]
      (let [x (.next ^Step s)
            b (if (some? body) (.reify body x) nil)]
        (->> {:cnt (AtomicInteger. 0)
              :branches branches
              :body b}
           (.init ^Step s))))

    AndJoin

    (create [this c] (stepize this c))
    (getName [_] "andjoin")
    (create [this j] (stepize this j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  OrJoin
  [^TaskDef actDef ^Step nxtStep & [_job]]

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (setNext [_ n] (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (job [this] (gjob this _job))
      (attrs [_] (:vars @info))
      (next [_] (:next @info))
      (proto [_] actDef)
      (id [_] pid)
      (handle [this j]
        (let [b (get-in @info [:vars :branches])
              y (get-in @info [:vars :body])
              c (get-in @info [:vars :cnt])
              nv (.incrementAndGet ^AtomicInteger c)
              nx (.next ^Step this)]
          (cond
            (== 0 b)
            (do
              (reinit! actDef this)
              (or y nx))

            (== 1 nv)
            (do
              (when (== 1 b) (reinit! actDef this))
              (or y nx))

            (>= nv b)
            (do->nil
              (reinit! actDef this))

            :else this))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn orjoin

  "Create a Or Join Task"
  ^OrJoin
  [^long branches & [^TaskDef body]]

  (reify

    Initable

    (init [_ s]
      (let [x (.next ^Step s)
            b (if (some? body) (.reify body x) nil)]
        (->> {:cnt (AtomicInteger. 0)
              :branches branches
              :body b}
             (.init ^Initable s))))

    OrJoin

    (create [this c] (stepize this c))
    (getName [_] "orjoin")
    (create [this j] (stepize this j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  If
  [^TaskDef actDef ^Step nxtStep & [_job]]

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (setNext [_ n] (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (next [_] (:next @info))
      (attrs [_] (:vars @info))
      (proto [_] actDef)
      (job [this] (gjob this _job))
      (handle [this j]
        (let [p (get-in @info [:vars :test])
              t (get-in @info [:vars :then])
              e (get-in @info [:vars :else])
              b (.ptest ^BoolExpr p ^Job j)]
          (reinit! actDef this)
          (if b t e))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn ternary

  "Create a If Task"
  ^If
  [^BoolExpr bexpr ^TaskDef then & [^TaskDef else]]

  (reify

    Initable

    (init [this s]
      (let [nx (.next ^Step s)
            e (if (some? else) (.reify else nx) nil)
            t (.reify then nx)]
        (->> {:test bexpr
              :then t
              :else e}
             (.init ^Initable s))))

    If

    (create [this c] (stepize this c))
    (getName [_] "if")
    (create [this c] (stepize this j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  While
  [^TaskDef actDef ^Step nxtStep & [_job]]

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (setNext [_ n] (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (next [_] (:next @info))
      (attrs [_] (:vars @info))
      (proto [_] actDef)
      (id [_] pid)
      (job [this] (gjob this _job))
      (handle [this j]
        (with-local-vars [rc this]
          (let [p (get-in @info [:vars :bexpr])
                ^Step
                y (get-in @info [:vars :body])
                nx (.next ^Step this)
                b (.ptest ^BoolExpr p ^Job j)]
            (if-not b
              (do
                (reinit! actDef this)
                (var-set rc nx))
              ;;normally n is null, but if it is not
              ;;switch the body to it.
              (when-some [n (.handle y ^Job j)]
                (cond
                  (inst? Delay (.proto n))
                  (do
                    (.setNext n @rc)
                    (var-set rc n))

                  (= n this)
                  nil

                  :else
                  ;; replace body
                  (let [x (.next y)]
                    (.setNext n x)
                    (-> (assoc (:vars @info) :body n)
                        (swap! info assoc :vars ))))))
            @rc))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn wloop

  "Create a While Task"
  ^While
  [^BoolExpr bexpr ^TaskDef body]
  {:pre [(some? body)]}

  (reify

    Initable

    (init [this s]
      (->> {:test bexpr
            :body (.create body ^Step s)}
           (.init ^Initable s)))

    While

    (create [this c] (stepize this c))
    (getName [_] "while")
    (create [this c] (stepize this j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Split
  [^TaskDef actDef ^Step nxtStep & [_job]]

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (setNext [_ n] (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (next [_] (:next @info))
      (attrs [_] (:vars @info))
      (proto [_] actDef)
      (id [_] pid)
      (job [this] (gjob this _job))
      (handle [this j]
        (let [t (get-in @info [:vars :joinStyle])
              ^Innards
              cs (get-in @info [:vars :forks])
              cpu (-> (.container ^Job j)
                      (.core))]
          (while
            (not (.isEmpty cs))
            (.run cpu (.next cs)))
          (reinit! actDef this)
          (if (or (= :and t) (= :or t))
            (.next ^Step this)
            nil))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn fork

  "Create a Split Task"
  ^Split
  [merger ^TaskDef body & branches]

  (let [cnt (count branches)
        join (cond
               (= :and merger) (andjoin cnt body)
               (= :or merger) (orjoin cnt body)
               :else (nuljoin))]
    (reify

      Initable

      (init [this p]
        (let [nx (.next ^Step p)
              s (.reify join nx)]
          (->> {:forks (Innards. s branches)
                :joinStyle merger}
               (.init ^Initable s))))

      Split

      (create [this c] (stepize this c))
      (getName [_] "fork")
      (create [this j] (stepize this j)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Group
  [^TaskDef actDef ^Step nxtStep & [_job]]

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (setNext [_ n] (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (job [this] (gjob this _job))
      (next [_] (:next @info))
      (id [_] pid)
      (attrs [_] (:vars @info))
      (proto [_] actDef)
      (handle [this j]
        (let [^Innards
              cs (get-in @info [:vars :list])
              nx (.next ^Step this)]
          (if-not (.isEmpty cs)
            (let [n (.next cs)
                  d (.proto n)]
              (.handle n ^Job j))
            (do
              (reinit! actDef this)
              nx)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn group

  "Create a Group Task"
  ^Group
  [^TaskDef a & xs]

  (let [cs (concat [a] xs)]
    (reify

      Initable

      (init [_ p]
        (->> {:list (Innards. p cs)}
             (.init ^Initable p)))

      Group

      (create [this c] (stepize this c))
      (getName [_] "group")
      (create [this j] (stepize this j)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro chain->

  ""
  ^Group
  [a & xs]

  `(group ~a ~@xs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;









