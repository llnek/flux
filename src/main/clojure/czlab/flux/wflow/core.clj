;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "A minimal worflow framework."
      :author "Kenneth Leung"}

  czlab.flux.wflow.core

  (:require [czlab.basal.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as cs])

  (:use [czlab.basal.core]
        [czlab.basal.str])

  (:import [java.util.concurrent.atomic AtomicInteger]
           [java.util TimerTask]
           [czlab.flux.wflow
            StepError
            Activity
            Step
            Job
            Nihil
            WorkStream]
           [czlab.jasal
            Interruptable
            Identifiable
            Catchable
            Initable
            Nameable
            CU
            Schedulable]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)
(def ^:private js-last :$lastresult)
(def range-index :$rangeindex)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rerun! "" [^Step s] (some-> s .job .scheduler (.reschedule s)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private gcpu
  "Get the core" [job] `(.scheduler ~(with-meta job {:tag 'Job})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private sv! "Set vars" [info vs] `(swap! ~info assoc :vars ~vs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mv! "Merge vars" [info m] (->> (merge (:vars @info) m) (sv! info )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fanout
  "Fork off tasks"
  [^Schedulable cpu ^Step nx defs]
  (doseq [t defs] (. cpu run (. ^Activity t create nx))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sa!
  "Set alarm"
  [^Schedulable cpu ^Step step job wsecs]
  (if (spos? wsecs) (. cpu alarm step job (* 1000 wsecs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private sn!
  "Set next step pointer" [info nx]
  `(let [t# ~nx] (assert (some? t#)) (swap! ~info assoc :next t#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private ri!
  "Reset a step" [a s] `(.init ~(with-meta a {:tag 'Initable}) ~s))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti stepize
  "Create a Step"
  {:private true :tag Step} (fn [a _ _] (.typeid ^Activity a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(declare stepRunAfter stepRun)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- protoStep<>
  "Generic Step"
  ^Step
  [actDef nxtStep args]

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (str "step#" (seqint2))]
    (reify Initable

      (init [_ m]
        (if-fn? [f (:initFn args)]
          (f _ info m)
          (sv! info (or m {}))))

      Step

      (setNext [_ nx] (sn! info nx))
      (job [this]
        (or (:job args)
            (.. this next job)))

      (rerun [this] (rerun! this))
      (run [this] (stepRun this))

      (attrs [_] (:vars @info))
      (next [_] (:next @info))
      (id [_] pid)
      (proto [_] actDef)

      (interrupt [_ job]
        (if-fn?
          [f (:interrupt args)]
          (f _ info job)))

      (handle [_ job]
        (if-fn?
          [f (:handle args)]
          (f _ info job))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nihil<>
  "A special *terminal task*" ^Nihil []

  (reify Initable

    (init [_ m] )

    Nihil

    (name [_] (name (.typeid _)))
    (typeid [_] :nihil)

    (create [_ s]
      (.createEx _ (.job s)))

    (createEx [_ j]
      (doto->> (stepize _ nil j)
               (.init _ )))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stepRunAfter "" [^Step s]
  (if s
    (let [cpu (gcpu (.job s))]
      (if
        (ist? Nihil (.proto s))
        (log/debug "nihil ==> stop or skip")
        (do
          (log/debug "next-to-run ==> %s" (.. s proto name))
          (.run cpu s))))
    (log/debug "next-to-run ==> null")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private nihilStep<> "" [^Job job] `(.createEx (nihil<>) ~job))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stepRun "" [^Step this]

  (log/debug "%s :handle()" (.. this proto name))
  ;;if step is queued, remove it
  (-> (gcpu (.job this))
      (.dequeue this))
  (let
    [job (.job this)
     ws (.wflow job)
     rc
     (try
       (.handle this job)
       (catch Throwable e#
         ;;if error handler returns
         ;;a Activity, run it
         (if-some
           [a
            (if-some [c (cast? Catchable ws)]
              (->> (StepError. this e#)
                   (.catche c)
                   (cast? Activity))
              (do->nil (log/error e# "")))]
           (->> (nihilStep<> job)
                (.create ^Activity a)))))]
    (stepRunAfter rc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- onInterrupt
  "A timer has expired - used by (joins)"
  [^Step this ^Job job waitSecs]

  (let
    [err (format "*interrupt* %s : %d secs"
                 "timer expired" waitSecs)
     ws (.wflow  job)
     ;;if error handling returns a Activity
     ;;run it, else just log the error
     rc
     (if-some
       [a
        (if-some [c (cast? Catchable ws)]
          (->> (StepError. this err)
               (.catche c)
               (cast? Activity))
          (do->nil (log/error err "")))]
       (->> (nihilStep<> job)
            (.create ^Activity a )))]
    (stepRunAfter rc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; this is a terminal, does nothing
(defmethod stepize
  :nihil [actDef nxtStep job]

  (assert (nil? nxtStep))
  (assert (some? job))
  (protoStep<> actDef nxtStep {:job job}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize
  :delay [actDef nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle
     (fn [this info job]
       (do->nil
         (let
           [nx (.next ^Step this)
            cpu (gcpu job)]
           (->> (or (get-in @info
                            [:vars :delay]) 0)
                (* 1000)
                (.postpone cpu nx))
           (ri! actDef this))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn postpone<>
  "Create a *delay task*"
  ^Activity [delaySecs] {:pre [(spos? delaySecs)]}

  (reify Initable

    (init [_ step]
      (. ^Initable step init {:delay delaySecs}))

    Activity

    (name [me] (name (.typeid me)))
    (typeid [_] :delay)

    (create [_ nx]
      (doto->> (stepize _ nx nil)
               (.init _ )))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize
  :script [actDef nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle
     (fn [_ info job]
       (let
         [a ((get-in @info
                     [:vars :work]) _ job)
          nx (.next ^Step _)]
         ;;do the work, if a Activity is returned
         ;;run it
         (ri! actDef _)
         (if
           (ist? Activity a)
           (.create ^Activity a nx)
           nx)))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn script<>
  "Create a *scriptable task*" {:tag Activity}

  ([workFunc] (script<> workFunc nil))

  ([workFunc script-name]
   {:pre [(fn? workFunc)]}
   (reify Initable

     (init [_ step]
       (. ^Initable step init {:work workFunc}))

     Activity

     (name [me] (stror script-name
                       (name (.typeid me))))
     (typeid [_] :script)

     (create [_ nx]
       (doto->> (stepize _ nx nil)
                (.init _))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize
  :switch [actDef nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle
     (fn [_ info job]
       (let
         [{:keys [cexpr dft choices]}
          (:vars @info)
          a (if-some [m (cexpr job)]
              (some #(if
                       (= m (first %1))
                       (last %1)) choices))]
         (ri! actDef _)
         (or a dft)))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn choice<>
  "Create a *switch task*"
  ^Activity [cexpr ^Activity dft & choices] {:pre [(fn? cexpr)]}

  (reify Initable

    (init [_ step]
      (let
        [nx (.next ^Step step)
         cs
         (->>
           (preduce<vec>
             #(let [[k ^Activity t] %2]
                (-> (conj! %1 k)
                    (conj! (.create t nx))))
             (partition 2 choices))
           (partition 2))]
        (->> {:dft (some-> dft (.create nx))
              :cexpr cexpr
              :choices cs}
             (.init ^Initable step))))

    Activity

    (name [me] (name (.typeid me)))
    (typeid [_] :switch)

    (create [_ nx]
      (doto->> (stepize _ nx nil)
               (.init _)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize
  :nuljoin
  [actDef nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle
     (fn [^Step _ info job]
       ;;spawn all children and goto next
       (let
         [bs (get-in @info [:vars :forks])
          nx (nihilStep<> job)
          cpu (gcpu job)]
         (doseq [t (seq bs)]
           (->> (.create ^Activity t nx)
                (.run cpu)))
         (.next _)))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nuljoin
  "Create a do-nothing *join task*" ^Activity [branches]

  (reify Initable

    (init [_ step]
      (. ^Initable step init {:forks branches}))

    Activity

    (name [me] (name (.typeid me)))
    (typeid [_] :nuljoin)

    (create [_ nx]
      (doto->> (stepize _ nx nil)
               (.init _)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize
  :andjoin [actDef nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:interrupt
     (fn [_ info job]
       (log/warn "and-join time out")
       (mv! info {:error true})
       (->> (get-in @info [:vars :wait])
            (onInterrupt _ job)))
     :handle
     (fn [^Step this info job]
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
                         .incrementAndGet ))
             ;;children all returned
             (if alarm
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
  "Create a *join(and) task*" ^Activity [branches waitSecs]

  (reify Initable

    (init [_ step]
      (->> {:cnt (AtomicInteger. 0)
            :wait waitSecs
            :forks branches}
           (.init ^Initable step)))

    Activity

    (name [me] (name (.typeid me)))
    (typeid [_] :andjoin)

    (create [_ nx]
      (doto->> (stepize _ nx nil)
               (.init _)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize
  :orjoin [actDef nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:interrupt
     (fn [_ info job]
       (log/debug "or-join time out")
       (mv! info {:error true})
       (->> (get-in @info [:vars :wait])
            (onInterrupt _ job)))
     :handle
     (fn [^Step this info job]
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
              (when alarm
                (.cancel ^TimerTask alarm)
                (mv! info {:alarm nil})
                nx)]
             (if (>= (-> ^AtomicInteger cnt
                         .incrementAndGet )
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
             (if (empty? forks) nx)))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- orjoin
  "Create a *or join task*"
  ^Activity [branches waitSecs]

  (reify Initable

    (init [_ step]
      (->> {:cnt (AtomicInteger. 0)
            :wait waitSecs
            :forks branches}
           (.init ^Initable step)))

    Activity

    (name [me] (name (.typeid me)))
    (typeid [_] :orjoin)

    (create [_ nx]
      (doto->> (stepize _ nx nil)
               (.init _)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize
  :if [actDef nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle
     (fn [_ info job]
       (let
         [{:keys [bexpr then else]}
          (:vars @info)
          b (bexpr job)]
         (ri! actDef _)
         (if b then else)))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn ternary<>
  "Create a *if task*" {:tag Activity}

  ([bexpr ^Activity then ^Activity else]
   (reify Initable

     (init [this step]
       (let
         [nx (.next ^Step step)
          e (some-> else
                    (.create nx))
          t (.create then nx)]
         (->> {:bexpr bexpr
               :then t
               :else e}
              (.init ^Initable step))))

     Activity

     (name [me] (name (.typeid me)))
     (typeid [_] :if)

     (create [_ nx]
       (doto->> (stepize _ nx nil)
                (.init _)))))

  ([bexpr then] (ternary<> bexpr then nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stepizeLoop "" [actDef nxtStep]

  (fn [^Step this info ^Job job]
    (let [{:keys [bexpr ^Step body]}
          (:vars @info)
          nx (.next this)
          b (bexpr job)]
      (if-not b
        (do (ri! actDef this) nx)
        (if-some
          [n (.handle body job)]
          (cond
            (= :delay (.. n proto typeid))
            (doto n (.setNext this))

            (identical? n this)
            this

            ;; replace body
            (ist? Step n)
            (do
              (->> (.next body)
                   (.setNext n))
              (mv! info {:body n})
              this)

            :else this)
          this)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize
  :while [actDef nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle (stepizeLoop actDef nxtStep)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn wloop<>
  "Create a *while task*"
  ^Activity [bexpr ^Activity body] {:pre [(fn? bexpr)]}

  (let []
    (reify Initable

      (init [_ step]
        (->> {:bexpr bexpr
              :body (.create body ^Step step)}
             (.init ^Initable step)))

      Activity

      (name [me] (name (.typeid me)))
      (typeid [_] :while)

      (create [_ nx]
        (doto->> (stepize _ nx nil)
                 (.init _))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize
  :split [actDef nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle
     (fn [^Step this info job]
       (let
         [{:keys [joinStyle wait forks]}
          (:vars @info)
          t
          (cond
            (= :and joinStyle)
            (andjoin forks wait)
            (= :or joinStyle)
            (orjoin forks wait)
            :else
            (nuljoin forks))]
         (. ^Activity t create (.next this))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn fork<>
  "Create a *split task*"
  ^Activity [options & branches] {:pre [(or (nil? options)
                                           (map? options))]}

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

      Activity

      (name [me] (name (.typeid me)))
      (typeid [_] :split)

      (create [_ nx]
        (doto->> (stepize _ nx nil)
                 (.init _))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize
  :group [actDef nxtStep _]

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
                 (-> ^Activity
                     (first @cs)
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
  ^Activity [^Activity a & xs] {:pre [(some? a)]}

  (reify Initable

    (init [_ step]
      (->> {:list (atom (concat [a] xs))}
           (.init ^Initable step)))

    Activity

    (name [me] (name (.typeid me)))
    (typeid [_] :group)

    (create [_ nx]
      (doto->> (stepize _ nx nil)
               (.init _)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rangeExpr "" [lower upper]
  (let [_loop (atom 0)]
    #(let [v @_loop]
       (if (< (+ lower v) upper)
         (do->true
           (. ^Job % setv range-index v)
           (swap! _loop inc))
         false))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize
  :for [actDef nxtStep _]

  (protoStep<>
    actDef
    nxtStep
    {:handle (stepizeLoop actDef nxtStep)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn floop<>
  "Create a *for task*"
  ^Activity
  [lower upper ^Activity body]
  {:pre [(fn? lower)(fn? upper)]}

  (reify Initable

    (init [_ step]
      (let [j (.job ^Step step)]
        (->> {:bexpr (rangeExpr (lower j)
                                (upper j))
              :body (.create body ^Step step)}
             (.init ^Initable step))))

    Activity

    (name [me] (name (.typeid me)))
    (typeid [_] :for)

    (create [_ nx]
      (doto->> (stepize _ nx nil)
               (.init _)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn job<> "" {:tag Job}

  ([_sch ws] (job<> _sch ws nil))
  ([_sch] (job<> _sch nil nil))
  ([^Schedulable _sch ^WorkStream ws evt]
   (let [data (muble<>)
         jid (str "job#" (seqint2))]
     (reify Job

       (contains [_ k]
         (if k (.contains data k)))

       (getv [_ k]
         (if k (.getv data k)))

       (setv [_ k v]
         (if k (.setv data k v)))

       (unsetv [_ k]
         (if k (.unsetv data k)))

       (clear [_] (.clear data))

       (scheduler [_] _sch)

       (origin [_] evt)

       (id [_] jid)

       (setLastResult [_ v]
         (.setv data js-last v))

       (clrLastResult [_]
         (.unsetv data js-last))

       (lastResult [_]
         (.getv data js-last))

       (wflow [_] ws)

       (dbgShow [_ out] )

       (dbgStr [_] (.toEDN data))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wsExec
  "" [^WorkStream ws ^Job job]

  (.setv job :$wflow ws)
  (-> (.scheduler job)
      (.run (-> (.head ws)
                (.create (nihilStep<> job))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wsHead
  "" [t0 more] (if-not (empty? more) (apply group<> t0 more) t0))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn workstream<>
  "Create a work flow with the
  follwing syntax:
  (workstream<> taskdef [taskdef...] [:catch func])"
  ^WorkStream
  [^Activity task0 & args] {:pre [(some? task0)]}

  ;;first we look for error handling which
  ;;must be at the end of the args
  (let
    [[a b] (take-last 2 args)
     [err tasks]
     (if (and (= :catch a)
              (fn? b))
       [b (drop-last 2 args)]
       [nil args])]
    (doseq [t tasks]
      (assert (ist? Activity t)))
    (if (fn? err)
      (reify
        WorkStream
        (execWith [_ j] (wsExec _ j))
        (head [_] (wsHead task0 tasks))
        Catchable
        (catche [_ e] (err e)))
      (reify
        WorkStream
        (execWith [_ j] (wsExec _ j))
        (head [_] (wsHead task0 tasks))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


