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
        [czlab.basal.meta]
        [czlab.basal.str])

  (:import [java.util.concurrent.atomic AtomicInteger]
           [java.util TimerTask]
           [czlab.flux.wflow
            Activity
            Cog
            Job
            Nihil
            Workstream]
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
(declare script<>)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wrapa "" ^Activity [x]
  (cond
    (ist? Activity x)
    x
    (fn? x)
    (script<> x)
    :else
    (throwBadArg "bad param type: "
                 (if (nil? x) "null" (class x)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wrapc "" ^Cog [x nxt] (-> (wrapa x) (.create nxt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rerun! "" [^Cog c] (some-> c .job .scheduler (.reschedule c)))

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
  [^Schedulable cpu ^Cog nx defs]
  (doseq [t defs] (. cpu run (. ^Activity t create nx))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sa!
  "Set alarm"
  [^Schedulable cpu ^Cog step job wsecs]
  (if (spos? wsecs) (. cpu alarm step job (* 1000 wsecs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private sn!
  "Set next step pointer" [info nx]
  `(let [t# ~nx] (assert (some? t#)) (swap! ~info assoc :next t#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- ri! "Reset a cog" [^Cog c] (-> ^Initable (.proto c) (.init c)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti cogit!
  "Create a Cog"
  {:private true :tag Cog} (fn [a _ _] (.typeid ^Activity a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(declare cogRunAfter cogRun)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- protoCog<>
  "" ^Cog [activity nxtCog args]

  (let [info (atom {:next nxtCog :vars {}})
        pid (str "cog#" (seqint2))]
    (reify Initable

      (init [_ m]
        (if-fn? [f (:initFn args)]
          (f _ info m)
          (sv! info (or m {}))))

      Cog

      (setNext [_ nx] (sn! info nx))
      (job [_]
        (or (:job args)
            (.. _ next job)))

      (rerun [_] (rerun! _))
      (run [_] (cogRun _))

      (attrs [_] (:vars @info))
      (next [_] (:next @info))
      (id [_] pid)
      (proto [_] activity)

      (interrupt [_ job]
        (if-fn?
          [f (:interrupt args)]
          (f _ info job)))

      (handle [_ job]
        (if-fn?
          [f (:action args)]
          (f _ info job))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nihil<>
  "Special *terminal task*" ^Nihil []

  (reify Initable

    (init [_ m] )

    Nihil

    (name [_] (name (.typeid _)))
    (typeid [_] :nihil)

    (create [_ c]
      (.createEx _ (.job c)))

    (createEx [_ j]
      (doto->> (cogit! _ nil j)
               (.init _ )))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cogRunAfter "" [^Cog c]
  (if-some [cpu (some-> c .job gcpu)]
    (if (ist? Nihil (.proto c))
      (log/debug "nihil :> stop/skip")
      (do
        (log/debug "next-cog :> %s"
                   (.. c proto name))
        (.run cpu c)))
    (log/debug "next-cog :> null")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private nihilCog<> "" [job] `(.createEx (nihil<>) ~job))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private err! "" [c e] `(czlab.flux.wflow.Error. ~c ~e))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cogRun "" [^Cog this]
  (log/debug "%s :action()" (.. this proto name))
  (-> this .job gcpu (.dequeue this))
  (let [job (.job this)
        ws (.wflow job)
        rc (try
             (.handle this job)
             (catch Throwable e#
               (if-some [a (if-some [c (cast? Catchable ws)]
                             (->> (err! this e#) (.catche c) (cast? Activity))
                             (do->nil (log/error e# "")))]
                 (->> (nihilCog<> job)
                      (.create ^Activity a)))))]
    (cogRunAfter rc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- onInterrupt
  "A timer has expired - used by (joins)"
  [^Cog this ^Job job waitSecs]
  (let
    [err (format "*interrupt* %s : %d secs"
                 "timer expired" waitSecs)
     ws (.wflow  job)
     rc (if-some [a (if-some [c (cast? Catchable ws)]
                      (->> (err! this err) (.catche c) (cast? Activity))
                      (do->nil (log/error err "")))]
          (->> (nihilCog<> job)
               (.create ^Activity a )))]
    (cogRunAfter rc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; this is a terminal, does nothing
(defmethod cogit!
  :nihil [_ nxtCog job]

  (assert (nil? nxtCog))
  (assert (some? job))
  (protoCog<> _ nxtCog {:job job}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  :delay [activity nxtCog _]

  (->>
    {:action
     (fn [^Cog this info job]
       (do->nil
         (let
           [nx (.next this)
            cpu (gcpu job)]
           (->> (or (get-in @info
                            [:vars :delay]) 0)
                (* 1000)
                (.postpone cpu nx))
           (ri! this))))}
    (protoCog<> activity nxtCog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn postpone<>
  "Create a *delay activity*"
  ^Activity [delaySecs] {:pre [(spos? delaySecs)]}

  (reify Initable

    (init [_ c]
      (. ^Initable c init {:delay delaySecs}))

    Activity

    (name [_] (name (.typeid _)))
    (typeid [_] :delay)

    (create [_ nx]
      (doto->> (cogit! _ nx nil)
               (.init _ )))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  :script [activity nxtCog _]

  (->>
    {:action
     (fn [^Cog c info job]
       (let
         [{:keys [work arity]}
          (get-in @info [:vars])
          a
          (cond
            (contains? arity 2) (work c job)
            (contains? arity 1) (work job)
            :else
            (throwBadArg "Expected %s: on %s"
                         "arity 2 or 1" (class work)))
          nx (.next c)]
         (ri! c)
         (if-some [a' (cast? Activity a)]
                  (.create a' nx)
                  nx)))}
    (protoCog<> activity nxtCog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn script<>
  "Create a *scriptable activity*" {:tag Activity}

  ([workFunc] (script<> workFunc nil))

  ([workFunc script-name]
   {:pre [(fn? workFunc)]}
   (reify Initable

     (init [_ c]
       (let [[s _] (countArity workFunc)]
         (. ^Initable c init {:work workFunc :arity s})))

     Activity

     (name [_] (stror script-name
                       (name (.typeid _))))
     (typeid [_] :script)

     (create [_ nx]
       (doto->> (cogit! _ nx nil)
                (.init _))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  :switch [activity nxtCog _]

  (->>
    {:action
     (fn [_ info job]
       (let [{:keys [cexpr dft choices]}
             (:vars @info)
             a (if-some [m (cexpr job)]
                 (some #(if
                          (= m (first %1))
                          (last %1))
                       (partition 2 choices)))]
         (ri! _)
         (or a dft)))}
    (protoCog<> activity nxtCog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn choice<>
  "Create a *choice activity*"
  ^Activity [cexpr dft & choices]
  {:pre [(fn? cexpr)
         (or (empty? choices)
             (even? (count choices)))]}

  (let [choices (preduce<vec>
                  #(let [[k v] %2]
                     (-> (conj! %1 k)
                         (conj! (wrapa v))))
                  (partition 2 choices))
        dft (some-> dft wrapa)]
    (reify Initable

      (init [_ c]
        (let [nx (.next ^Cog c)]
          (->> {:dft (some-> dft (wrapc nx))
                :cexpr cexpr
                :choices
                (preduce<vec>
                  #(let [[k v] %2]
                     (-> (conj! %1 k)
                         (conj! (wrapc v nx))))
                  (partition 2 choices)) }
               (.init ^Initable c))))

      Activity

      (name [me] (name (.typeid me)))
      (typeid [_] :switch)

      (create [_ nx]
        (doto->> (cogit! _ nx nil)
                 (.init _))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  :nuljoin
  [actDef nxtCog _]

  (protoCog<>
    actDef
    nxtCog
    {:action
     (fn [^Cog _ info job]
       ;;spawn all children and goto next
       (let
         [bs (get-in @info [:vars :forks])
          nx (nihilCog<> job)
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
      (doto->> (cogit! _ nx nil)
               (.init _)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  :andjoin [actDef nxtCog _]

  (protoCog<>
    actDef
    nxtCog
    {:interrupt
     (fn [_ info job]
       (log/warn "and-join time out")
       (mv! info {:error true})
       (->> (get-in @info [:vars :wait])
            (onInterrupt _ job)))
     :action
     (fn [^Cog this info job]
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
             (ri! this)
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
      (doto->> (cogit! _ nx nil)
               (.init _)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  :orjoin [actDef nxtCog _]

  (protoCog<>
    actDef
    nxtCog
    {:interrupt
     (fn [_ info job]
       (log/debug "or-join time out")
       (mv! info {:error true})
       (->> (get-in @info [:vars :wait])
            (onInterrupt _ job)))
     :action
     (fn [^Cog this info job]
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
               (ri! this))
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
      (doto->> (cogit! _ nx nil)
               (.init _)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  :if [actDef nxtCog _]

  (protoCog<>
    actDef
    nxtCog
    {:action
     (fn [_ info job]
       (let
         [{:keys [bexpr then else]}
          (:vars @info)
          b (bexpr job)]
         (ri! _)
         (if b then else)))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn decision<>
  "Create a *if task*" {:tag Activity}

  ([bexpr ^Activity then ^Activity else]
   (reify Initable

     (init [this step]
       (let
         [nx (.next ^Cog step)
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
       (doto->> (cogit! _ nx nil)
                (.init _)))))

  ([bexpr then] (decision<> bexpr then nil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cogit!Loop "" [actDef nxtCog]

  (fn [^Cog this info ^Job job]
    (let [{:keys [bexpr ^Cog body]}
          (:vars @info)
          nx (.next this)
          b (bexpr job)]
      (if-not b
        (do (ri! this) nx)
        (if-some
          [n (.handle body job)]
          (cond
            (= :delay (.. n proto typeid))
            (doto n (.setNext this))

            (identical? n this)
            this

            ;; replace body
            (ist? Cog n)
            (do
              (->> (.next body)
                   (.setNext n))
              (mv! info {:body n})
              this)

            :else this)
          this)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  :while [actDef nxtCog _]

  (protoCog<>
    actDef
    nxtCog
    {:action (cogit!Loop actDef nxtCog)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn wloop<>
  "Create a *while task*"
  ^Activity [bexpr ^Activity body] {:pre [(fn? bexpr)]}

  (let []
    (reify Initable

      (init [_ step]
        (->> {:bexpr bexpr
              :body (.create body ^Cog step)}
             (.init ^Initable step)))

      Activity

      (name [me] (name (.typeid me)))
      (typeid [_] :while)

      (create [_ nx]
        (doto->> (cogit! _ nx nil)
                 (.init _))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  :split [actDef nxtCog _]

  (protoCog<>
    actDef
    nxtCog
    {:action
     (fn [^Cog this info job]
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
  ^Activity [options & branches] {:pre [(some? options)]}

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
        (doto->> (cogit! _ nx nil)
                 (.init _))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  :group [actDef nxtCog _]

  (protoCog<>
    actDef
    nxtCog
    {:action
     (fn [^Cog this info ^Job job]
       (let
         [cs (get-in @info [:vars :list])
          nx (.next this)]
         (if-not (empty? @cs)
           (let [a
                 (-> ^Activity
                     (first @cs)
                     (.create ^Cog this))
                 r (rest @cs)
                 rc (.handle a job)]
             (reset! cs r)
             rc)
           (do (ri! this) nx))))}))

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
      (doto->> (cogit! _ nx nil)
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
(defmethod cogit!
  :for [actDef nxtCog _]

  (protoCog<>
    actDef
    nxtCog
    {:action (cogit!Loop actDef nxtCog)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn floop<>
  "Create a *for task*"
  ^Activity
  [lower upper ^Activity body]
  {:pre [(fn? lower)(fn? upper)]}

  (reify Initable

    (init [_ step]
      (let [j (.job ^Cog step)]
        (->> {:bexpr (rangeExpr (lower j)
                                (upper j))
              :body (.create body ^Cog step)}
             (.init ^Initable step))))

    Activity

    (name [me] (name (.typeid me)))
    (typeid [_] :for)

    (create [_ nx]
      (doto->> (cogit! _ nx nil)
               (.init _)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn job<> "" {:tag Job}

  ([_sch ws] (job<> _sch ws nil))
  ([_sch] (job<> _sch nil nil))
  ([^Schedulable _sch ^Workstream ws evt]
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
  "" [^Workstream ws ^Job job]

  (.setv job :$wflow ws)
  (-> (.scheduler job)
      (.run (-> (.head ws)
                (.create (nihilCog<> job))))))

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
  ^Workstream
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
        Workstream
        (execWith [_ j] (wsExec _ j))
        (head [_] (wsHead task0 tasks))
        Catchable
        (catche [_ e] (err e)))
      (reify
        Workstream
        (execWith [_ j] (wsExec _ j))
        (head [_] (wsHead task0 tasks))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn fnToScript
  "Wrap function into a script"
  ^Activity [func] {:pre [(fn? func)]} (script<> func))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


