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

(ns ^{:doc "A minimal worflow framework."
      :author "Kenneth Leung"}

  czlab.wflow.core

  (:require [czlab.xlib.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as cs])

  (:use [czlab.xlib.core]
        [czlab.xlib.str])

  (:import [java.util.concurrent.atomic AtomicInteger]
           [java.util TimerTask]
           [czlab.server Event ServerLike]
           [czlab.wflow
            Switch
            Delay
            Job
            Group
            For
            If
            While
            Script
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
            RangeExpr
            ChoiceExpr
            WorkStream]
           [czlab.xlib
            Interruptable
            Identifiable
            Catchable
            Initable
            Nameable
            CU
            Schedulable]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rerun!
  ""
  [^Step s]
  (some-> s
          (.job )
          (.server )
          (.core )
          (.reschedule s)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private gcpu
  "Get the core"
  [^Job job] `(.core (.server ~job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private sv!
  "Set vars"
  [info vs] `(swap! ~info assoc :vars ~vs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mv!
  "Merge vars"
  [info m] (->> (merge (:vars @info) m) (sv! info )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fanout
  "Fork off tasks"
  [^Schedulable cpu ^Step nx defs]
  (doseq [t defs]
    (.run cpu
          (.create ^TaskDef t nx))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sa!
  "Set alarm"
  [^Schedulable cpu ^Step step job wsecs]
  (when (spos? wsecs)
    (.alarm cpu step job (* 1000 wsecs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private sn!
  "Set next"
  [info nx]
  `(let [t# ~nx]
     (assert (some? t#))
     (swap! ~info assoc :next t#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private ri!
  "Reset a step"
  [a s]
  `(.init ~(with-meta a {:tag 'Initable}) ~s))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti stepize
  "Create a Step instance"
  {:private true
   :tag Step}
  (fn [a b c] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(declare stepRunAfter stepRun)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- protoStep<>
  "Basic Step"
  ^Step
  [^TaskDef actDef ^Step nxtStep args]

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (str "step#" (seqint2))]
    (reify Initable

      (init [this m]
        (if-some [f (:initFn args)]
          (f this info m)
          (swap! info
                 assoc :vars (or m {}))))

      Step

      (setNext [_ nx] (sn! info nx))
      (job [this]
        (or (:job args)
            (.job (.next this))))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (attrs [_] (:vars @info))
      (id [_] pid)
      (proto [_] actDef)
      (next [_] (:next @info))
      (interrupt [this job]
        (if-some
          [f (:interrupt args)]
          (f this info job)))
      (handle [this job]
        (if-some
          [f (:handle args)]
          (f this info job))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nihil<>
  "Create a special *terminator task*"
  ^Nihil
  []
  (reify Initable

    (init [_ m] )

    Nihil

    (create [this step]
      (.createEx this (.job step)))

    (name [_] "nihil")

    (createEx [this job]
      (doto->> (stepize this nil job)
               (.init this )))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stepRunAfter
  ""
  [^Step this]
  (if (some? this)
    (let [cpu (gcpu (.job this))]
      (if
        (inst? Nihil (.proto this))
        (log/debug "nihil ==> stop or skip")
        (do
          (log/debug
            "next-to-run ==> {%s}"
            (.name ^Nameable (.proto this)))
          (.run cpu this))))
    (log/debug "next-to-run ==> null")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private nihilStep<>
  "" [^Job job] `(.createEx (nihil<>) ~job))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stepRun
  ""
  [^Step this]
  (log/debug "%s :handle()"
             (.name ^Nameable (.proto this)))
  ;;if this step is in a pending queue, remove it
  (-> (gcpu (.job this))
      (.dequeue this))
  (let
    [job (.job this)
     ws (.wflow job)
     rc
     (try
       (.handle this job)
       (catch Throwable e#
         ;;if the error handler returns a new TaskDef
         ;;run it
         (if-some
           [a
            (if-some [c (cast? Catchable ws)]
              (->> (StepError. this e#)
                   (.catche c)
                   (cast? TaskDef))
              (do->nil
                (log/error e# "")))]
           (->> (nihilStep<> job)
                (.create ^TaskDef a )))))]
    (stepRunAfter rc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- onInterrupt
  "A timer has expired - used by (joins)"
  [^Step this ^Job job waitSecs]
  (let
    [err (format "*interruption* %s : %d msecs"
                 "timer expired"
                 (* 1000 waitSecs))
     ws (.wflow  job)
     ;;if the error handling returns a TaskDef
     ;;run it, else just log the error
     rc
     (if-some
       [a
        (if-some [c (cast? Catchable ws)]
          (->> (StepError. this err)
               (.catche c)
               (cast? TaskDef))
          (do->nil (log/error err)))]
       (->> (nihilStep<> job)
            (.create ^TaskDef a )))]
    (stepRunAfter rc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; this is a terminator step, does nothing
(defmethod stepize

  Nihil
  [^TaskDef actDef ^Step nxtStep ^Job _job]

  (assert (nil? nxtStep))
  (assert (some? _job))
  (protoStep<> actDef nxtStep {:job _job}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Delay
  [^TaskDef actDef ^Step nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle
     (fn [^Step this info ^Job job]
       (let
         [nx (.next this)
          cpu (gcpu job)]
         (->> (get-in @info [:vars :delay])
              (* 1000 )
              (.postpone cpu nx ))
         (ri! actDef this)
         nil))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn postpone<>
  "Create a *delay task*"
  ^Delay
  [delaySecs]
  {:pre [(spos? delaySecs)]}
  (reify Initable

    (init [_ step]
      (->> {:delay delaySecs}
           (.init ^Initable step)))

    Delay

    (name [_] "delay")

    (create [this nx]
      (doto->> (stepize this nx nil)
               (.init this )))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Script
  [^TaskDef actDef ^Step nxtStep dummy]

  (protoStep<>
    actDef
    nxtStep
    {:handle
     (fn [^Step this info ^Job job]
       (let
         [a ((get-in @info [:vars :work]) this job)
          nx (.next this)]
         ;;do the work, if a TaskDef is returned
         ;;run it
         (ri! actDef this)
         (if
           (inst? TaskDef a)
           (.create ^TaskDef a nx)
           nx)))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn script<>
  "Create a *scriptable task*"
  {:tag Script}

  ([workFunc] (script<> workFunc nil))

  ([workFunc nm]
   {:pre [(fn? workFunc)]}
   (reify Initable

     (init [_ step]
       (->> {:work workFunc}
            (.init ^Initable step)))

     Script

     (name [_] (stror nm "script"))

     (create [this nx]
       (doto->> (stepize this nx nil)
                (.init this))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Switch
  [^TaskDef actDef ^Step nxtStep dummy]

  (protoStep<>
    actDef
    nxtStep
    {:handle
     (fn [^Step this info ^Job job]
       (let
         [{:keys [^ChoiceExpr cexpr
                  dft
                  choices]}
          (:vars @info)
          m (.choose cexpr job)
          a (if (some? m)
              (some #(if
                       (= m (first %1))
                       (last %1) nil) choices) nil)]
         (ri! actDef this)
         (or a dft)))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn choice<>
  "Create a *switch task*"
  ^Switch
  [^ChoiceExpr cexpr ^TaskDef dft & choices]
  (reify Initable

    (init [_ step]
      (let
        [nx (.next ^Step step)
         cs
         (->>
           (preduce<vec>
             #(let [[k ^TaskDef t] %2]
                (-> (conj! %1 k)
                    (conj! (.create t nx))))
             (partition 2 choices))
           (partition 2))]
        (->> {:dft (some-> dft (.create nx))
              :cexpr cexpr
              :choices cs}
             (.init ^Initable step))))

    Switch

    (name [_] "switch")

    (create [this nx]
      (doto->> (stepize this nx nil)
               (.init this)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  NulJoin
  [^TaskDef actDef ^Step nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle
     (fn [^Step this info ^Job job]
       ;;spawn all children and goto next
       (let
         [bs (get-in @info [:vars :forks])
          nx (nihilStep<> job)
          cpu (gcpu job)]
         (doseq [t (seq bs)]
           (->> (.create ^TaskDef t nx)
                (.run cpu)))
         (.next this)))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nuljoin
  "Create a do-nothing *join task*"
  ^NulJoin
  [branches]

  (reify Initable

    (init [_ step]
      (->> {:forks branches}
           (.init ^Initable step)))

    NulJoin

    (name [_] "nuljoin")

    (create [this nx]
      (doto->> (stepize this nx nil)
               (.init this)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  AndJoin
  [^TaskDef actDef ^Step nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:interrupt
     (fn [^Step this info ^Job job]
       (log/warn "and-join time out")
       (mv! info {:error true})
       (->> (get-in @info [:vars :wait])
            (onInterrupt this job )))
     :handle
     (fn [^Step this info ^Job job]
       (let
         [{:keys [forks alarm
                  error wait cnt]}
          (:vars @info)
          cpu (gcpu job)]
         (cond
           (true? error)
           (do->nil (log/debug "too late"))

           (number? forks)
           (when (== forks
                     (-> ^AtomicInteger cnt
                         (.incrementAndGet )))
             ;;children all returned
             (if (some? alarm)
               (.cancel ^TimerTask alarm))
             (ri! actDef this)
             (.next this))

           :else
           (if-not (empty? forks)
             (do->nil
               (fanout cpu this forks)
               (->>
                 {:alarm
                  (sa! cpu this job wait)
                  :forks (count forks)}
                 (mv! info)))
             (.next this)))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- andjoin
  "Create a *join(and) task*"
  ^AndJoin
  [branches waitSecs]

  (reify Initable

    (init [_ step]
      (->> {:cnt (AtomicInteger. 0)
            :wait waitSecs
            :forks branches}
           (.init ^Initable step)))

    AndJoin

    (name [_] "andjoin")

    (create [this nx]
      (doto->> (stepize this nx nil)
               (.init this)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  OrJoin
  [^TaskDef actDef ^Step nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:interrupt
     (fn [^Step this info ^Job job]
       (log/debug "or-join time out")
       (mv! info {:error true})
       (->> (get-in @info [:vars :wait])
            (onInterrupt this job)))
     :handle
     (fn [^Step this info ^Job job]
       (let
         [{:keys [forks alarm error
                  wait cnt]}
          (:vars @info)
          cpu (gcpu job)
          nx (.next this)]
         (cond
           (true? error)
           nil
           (number? forks)
           (let
             [rc
              (when (some? alarm)
                (.cancel ^TimerTask alarm)
                (mv! info {:alarm nil})
                nx)]
             (if (>= (-> ^AtomicInteger cnt
                         (.incrementAndGet ))
                     forks)
               (ri! actDef this))
             rc)
           :else
           (do
             (fanout cpu this forks)
             (->>
               {:alarm (sa! cpu this job wait)
                :forks (count forks) }
               (mv! info))
             (if (empty? forks) nx nil)))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- orjoin
  "Create a *or join task*"
  ^OrJoin
  [branches waitSecs]

  (reify Initable

    (init [_ step]
      (->> {:cnt (AtomicInteger. 0)
            :wait waitSecs
            :forks branches}
           (.init ^Initable step)))

    OrJoin

    (name [_] "orjoin")

    (create [this nx]
      (doto->> (stepize this nx nil)
               (.init this)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  If
  [^TaskDef actDef ^Step nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle
     (fn [^Step this info ^Job job]
       (let
         [{:keys [test then else]}
          (:vars @info)
          b (.ptest ^BoolExpr test job)]
         (ri! actDef this)
         (if b then else)))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn ternary<>
  "Create a *if task*"
  {:tag If}

  ([^BoolExpr bexpr ^TaskDef then ^TaskDef else]
   (reify Initable

     (init [this step]
       (let
         [nx (.next ^Step step)
          e (some-> else
                    (.create nx))
          t (.create then nx)]
         (->> {:test bexpr
               :then t
               :else e}
              (.init ^Initable step))))

     If

     (name [_] "if")

     (create [this nx]
       (doto->> (stepize this nx nil)
                (.init this)))))

  ([bexpr then] (ternary<> bexpr then nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stepizeLoop

  ""
  [^TaskDef actDef ^Step nxtStep]

  (fn [^Step this info ^Job job]
    (let [{:keys [bexpr ^Step body]}
          (:vars @info)
          nx (.next this)
          b (.ptest ^BoolExpr bexpr job)]
      (if-not b
        (do (ri! actDef this) nx)
        (if-some
          [n (.handle body job)]
          (cond
            (inst? Delay (.proto n))
            (do (.setNext n this) n)

            (identical? n this)
            this

            ;; replace body
            (inst? Step n)
            (do
              (->> (.next body)
                   (.setNext n))
              (->> {:body n}
                   (mv! info))
              this)

            :else this)
          this)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  While
  [^TaskDef actDef ^Step nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle (stepizeLoop actDef nxtStep)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn wloop<>
  "Create a *while task*"
  ^While
  [^BoolExpr bexpr ^TaskDef body]

  (reify Initable

    (init [this step]
      (->> {:bexpr bexpr
            :body (.create body ^Step step)}
           (.init ^Initable step)))

    While

    (name [_] "while")

    (create [this nx]
      (doto->> (stepize this nx nil)
               (.init this)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Split
  [^TaskDef actDef ^Step nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle
     (fn [^Step this info ^Job job]
       (let
         [{:keys [joinStyle wait forks]}
          (:vars @info)
          ^TaskDef t
          (cond
            (= :and joinStyle)
            (andjoin forks wait)
            (= :or joinStyle)
            (orjoin forks wait)
            :else
            (nuljoin forks))]
         (.create t (.next this))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn fork<>
  "Create a *split task*"
  ^Split
  [options & branches]
  {:pre [(map? options)]}

  (let [wsecs (or (:waitSecs options) 0)
        cnt (count branches)
        merger (or (:join options) :nil)]
    (log/debug "forking with [%d] branches" cnt)
    (reify Initable

      (init [_ step]
        (->> {:joinStyle merger
              :wait wsecs
              :forks branches}
             (.init ^Initable step)))

      Split

      (name [_] "fork")

      (create [this nx]
        (doto->> (stepize this nx nil)
                 (.init this))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Group
  [^TaskDef actDef ^Step nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle
     (fn [^Step this info ^Job job]
       (let
         [cs (get-in @info [:vars :list])
          nx (.next this)]
         (if-not (empty? @cs)
           (let [a
                 (-> ^TaskDef (first @cs)
                     (.create ^Step this))
                 r (rest @cs)
                 rc (.handle a job)]
             (reset! cs r)
             rc)
           (do (ri! actDef this) nx))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn group<>

  "Create a *group task*"
  ^Group
  [^TaskDef a & xs]
  {:pre [(some? a)]}

  (reify Initable

    (init [_ step]
      (->> {:list (atom (concat [a] xs))}
           (.init ^Initable step)))

    Group

    (name [_] "group")

    (create [this nx]
      (doto->> (stepize this nx nil)
               (.init this)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rangeExpr
  ""
  ^BoolExpr
  []
  (let [_loop (atom 0)]
    (reify BoolExpr
      (ptest [_ j]
        (let [w (.getv j :lowerRange)
              u (.getv j :upperRange)
              v @_loop]
          (if (< (+ w v) u)
            (do->true
              (.setv j For/RANGE_INDEX v)
              (swap! _loop inc))
            false))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  For
  [^TaskDef actDef ^Step nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle (stepizeLoop actDef nxtStep)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn floop<>
  "Create a *for task*"
  ^For
  [^RangeExpr rexpr ^TaskDef body]

  (reify Initable

    (init [_ step]
      (let [j (.job ^Step step)
            w (.lower rexpr j)
            u (.upper rexpr j)]
        (.setv j :lowerRange w)
        (.setv j :upperRange u)
        (->> {:bexpr (rangeExpr)
              :body (.create body ^Step step)}
             (.init ^Initable step))))

    For

    (name [_] "for")

    (create [this nx]
      (doto->> (stepize this nx nil)
               (.init this)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn job<>
  ""
  {:tag Job}

  ([_server ws] (job<> _server ws nil))
  ([_server] (job<> _server nil nil))
  ([^ServerLike _server ^WorkStream ws ^Event evt]
   (let [jslast (keyword Job/JS_LAST)
         data (muble<>)
         jid (str "job#" (seqint2))]
     (reify Job

       (contains [_ k]
         (if (some? k)
           (.contains data k)))

       (getv [_ k]
         (if (some? k) (.getv data k)))

       (setv [_ k v]
         (if (some? k)
           (.setv data k v)))

       (unsetv [_ k]
         (if (some? k)
           (.unsetv data k)))

       (clear [_]
         (.clear data))

       (server [_] _server)

       (event [_] evt)

       (id [_] jid)

       (setLastResult [_ v]
         (.setv data jslast v))

       (clrLastResult [_]
         (.unsetv data jslast))

       (lastResult [_]
         (.getv data jslast))

       (wflow [_] ws)

       (dbgShow [_ out] )

       (dbgStr [_] (.toEDN data))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wsExec
  ""
  [^WorkStream ws
   ^Job job]
  (.setv job :wflow ws)
  (-> (.core (.server job))
      (.run (-> (.head ws)
                (.create (nihilStep<> job))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wsHead
  ""
  [t0 more]
  (if-not (empty? more) (apply group<> t0 more) t0))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn workStream<>
  "Create a work flow with the
  follwing syntax:
  (workStream<> taskdef [taskdef...] [:catch func])"
  ^WorkStream
  [^TaskDef task0 & args]
  {:pre [(some? task0)]}

  ;;first we look for erro handling which
  ;;must be at the end of the args
  (let
    [[a b] (take-last 2 args)
     [err tasks]
     (if (and (= :catch a)
              (fn? b))
       [b (drop-last 2 args)]
       [nil args])]
    (doseq [t tasks]
      (assert (inst? TaskDef t)))
    (if (fn? err)
      (reify
        WorkStream
        (execWith [this j] (wsExec this j))
        (head [_] (wsHead task0 tasks))
        Catchable
        (catche [_ e] (err e)))
      ;;else
      (reify
        WorkStream
        (execWith [this j] (wsExec this j))
        (head [_] (wsHead task0 tasks))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF





