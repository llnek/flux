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
           [czlab.basal Stateful]
           [czlab.jasal
            RunnableWithId
            Interruptable
            Catchable
            Initable
            Nameable
            Schedulable]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)
(declare cogRunAfter cogRun script<>)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol ICog ""
  (^czlab.flux.wflow.core.ICog handle [_ arg] "")
  (job [_] "")
  (rerun [_] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol IJob "" (origin [_] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol Activity ""
  (^czlab.flux.wflow.core.ICog createCog [_ nxtCog] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol IWorkstream "" (execWith [_ job] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wrapa ""
  ^czlab.flux.wflow.core.Activity [x]
  (cond
    (satisfies? Activity x) x
    (fn? x) (script<> x)
    :else
    (throwBadArg "bad param type: "
                 (if (nil? x) "null" (class x)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wrapc ""
  ^czlab.flux.wflow.core.ICog
  [x nxt] (-> (wrapa x) (.createCog nxt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rerun!
  "" [^czlab.flux.wflow.core.ICog cog]
  (if-some [j (some-> cog .job)]
    (.reschedule ^Schedulable
                 (::scheduler @j) cog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- gcpu "" ^Schedulable [job] (::scheduler @job))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fanout "Fork off tasks"
  [^Schedulable cpu nx defs]
  (doseq [t defs]
    (.run cpu ^RunnableWithId (wrapc t nx))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sa! "Set alarm"
  [^Schedulable cpu
   ^Interruptable cog job wsecs]
  (if (spos? wsecs)
    (.alarm cpu cog job (* 1000 wsecs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- ri!
  "Reset a cog" [cog]
  (.init ^Initable (:proto @cog)  cog))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti cogit!
  "Create a Cog"
  {:private true
   :tag czlab.flux.wflow.core.ICog} (fn [a _ _] (:typeid @a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Cog

  ICog

  (handle [_ job]
    (if-fn? [f (:action @data)] (f _ job)))

  (job [_]
    (if-some [n (:next @data)]
      (.job ^czlab.flux.wflow.core.ICog n)
      (:job @data)))

  (rerun [_] (rerun! _))

  RunnableWithId

  (run [_] (cogRun _))
  (id [_] (:id @data))

  Interruptable

  (interrupt [me job]
    (if-fn? [f (:interrupt @data)] (f me job)))

  Initable

  (init [me m]
    (if-fn? [f (:initFn @data)]
      (f me m)
      (.update me (or m {})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn protoCog<>
  "" [activity nxtCog args]
  (entity<> Cog
            (merge args
                   {:id (str "cog#" (seqint2))
                    :proto activity
                    :next nxtCog})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- createCogEx [nihil job]
  (doto->> (cogit! nihil nil job)
           (.init ^Initable nihil )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn gjob
  "" [^czlab.flux.wflow.core.ICog cog] (some-> cog .job))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Nihil

  Nameable
  (name [_] (name (:typeid @data)))

  Activity

  (createCog [me cog]
    (createCogEx me (gjob cog)))

  Initable
  (init [_ m] ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nihil<>
  "*terminal activity*"
  ^czlab.flux.wflow.core.Nihil
  [] (entity<> Nihil {:typeid ::nihil}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cogRunAfter
  "" [^czlab.flux.wflow.core.ICog cog]
  (if cog
    (let [{:keys [proto]} @cog
          cpu (gcpu (gjob cog))]
      (if (= ::nihil (:typeid @proto))
        (log/debug "nihil :> stop/skip")
        (do
          (log/debug "next-cog :> %s"
                     (.name ^Nameable proto))
          (.run cpu cog))))
    (log/debug "next-cog :> null")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private nihilCog<>
  "" [job] `(createCogEx (nihil<>) ~job))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private err! "" [c e] `(doto {:cog ~c :error ~e}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cogRun "" [^czlab.flux.wflow.core.ICog this]
  (let
    [{:keys [proto]} @this
     job (gjob this)
     ws (::wflow @job)
     _ (-> job
           gcpu
           (.dequeue this))
     rc (try
          (log/debug "%s :action()"
                     (.name ^Nameable proto))
          (.handle this job)
          (catch Throwable e#
            (if-some
              [a (if-some
                   [c (cast? Catchable ws)]
                   (->> (err! this e#)
                        (.catche c)
                        (cast? czlab.flux.wflow.core.Activity))
                   (do->nil (log/error e# "")))]
              (wrapc a (nihilCog<> job)))))]
    (cogRunAfter rc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- onInterrupt
  "A timer has expired - used by (joins)"
  [^czlab.flux.wflow.core.ICog this job waitSecs]
  (let
    [err (format "*interrupt* %s : %d secs"
                 "timer expired" waitSecs)
     ws (::wflow @job)
     rc (if-some
          [a (if-some
               [c (cast? Catchable ws)]
               (->> (err! this err)
                    (.catche c)
                    (cast? czlab.flux.wflow.core.Activity))
               (do->nil (log/error err "")))]
          (wrapc a (nihilCog<> job)))]
    (cogRunAfter rc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; this is a terminal, does nothing
(defmethod cogit!
  ::nihil [_ nxtCog job]
  (assert (nil? nxtCog))
  (assert (some? job))
  (protoCog<> _ nxtCog {:job job}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::delay [activity nxtCog _]

  (protoCog<>
    activity
    nxtCog
    {:action
     (fn [this job]
       (do->nil
         (let [{:keys [next delay]}
               @this
               cpu (gcpu job)]
           (->> (or delay 0)
                (* 1000)
                (.postpone cpu next))
           (ri! this))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Postpone

  Initable
  (init [_ cog]
    (.init ^Initable
           cog
           {:delay (:delaySecs @data)}))

  Activity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _ )))

  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn postpone<>
  "Create a *delay activity*"
  ^czlab.flux.wflow.core.Activity
  [delaySecs]
  {:pre [(spos? delaySecs)]}

  (entity<> Postpone
            {:delaySecs delaySecs :typeid ::delay}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::script [activity nxtCog _]

  (protoCog<>
    activity
    nxtCog
    {:action
     (fn [cog job]
       (let
         [{:keys [next work arity]}
          @cog
          a
          (cond
            (contains? arity 2) (work c job)
            (contains? arity 1) (work job)
            :else
            (throwBadArg "Expected %s: on %s"
                         "arity 2 or 1" (class work)))]
         (ri! cog)
         (if-some
           [a' (cast? czlab.flux.wflow.core.Activity a)]
           (.createCog a' next)
           next)))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Script

  Initable
  (init [_ cog]
    (let [{:keys [workFunc]} @data
          [s _] (countArity workFunc)]
      (.init ^Initable
             cog {:work workFunc :arity s})))

  Activity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (stror (:scriptname @data)
                   (name (:typeid @data)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn script<>
  "Create a *scriptable activity*"
  {:tag czlab.flux.wflow.core.Activity}

  ([workFunc] (script<> workFunc nil))
  ([workFunc script-name]
   {:pre [(fn? workFunc)]}
   (entity<> Script
             {:workFunc workFunc
              :scriptname script-name :typeid ::script})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::switch [activity nxtCog _]

  (protoCog<>
    activity
    nxtCog
    {:action
     (fn [cog job]
       (let [{:keys [cexpr dft choices]}
             @cog
             a (if-some [m (cexpr job)]
                 (some #(if
                          (= m (first %1))
                          (last %1))
                       (partition 2 choices)))]
         (ri! cog)
         (or a dft)))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Switch
  Initable
  (init [_ cog]
    (let [{:keys [choices cexpr dft]}
          @data
          nx (:next @cog)]
      (->> {:dft (some-> dft (wrapc nx))
            :cexpr cexpr
            :choices
            (preduce<vec>
              #(let [[k v] %2]
                 (-> (conj! %1 k)
                     (conj! (wrapc v nx))))
              (partition 2 choices))}
           (.init ^Initable cog))))

  Activity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn choice<>
  "Create a *choice activity*"
  ^czlab.flux.wflow.core.Activity
  [cexpr dft & choices]
  {:pre [(fn? cexpr)
         (or (empty? choices)
             (even? (count choices)))]}
  (let [choices (preduce<vec>
                  #(let [[k v] %2]
                     (-> (conj! %1 k)
                         (conj! (wrapa v))))
                  (partition 2 choices))
        dft (some-> dft wrapa)]
    (entity<> Switch
              {:choices choices
               :cexpr cexpr
               :dft dft
               :typeid ::switch})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::nuljoin [activity nxtCog _]

  ;;spawn all children and continue
  (protoCog<>
    activity
    nxtCog
    {:action
     (fn [cog job]
       (let [{:keys [forks next]}
             @cog
             nx (nihilCog<> job)
             cpu (gcpu job)]
         (doseq [t forks]
           (.run cpu (wrapc t nx)))
         next))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful NulJoin

  Initable
  (init [_ cog]
    (->> {:forks (mapv #(wrapa %) (:branches @data))}
         (.init ^Initable cog )))

  Activity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nuljoin
  "Create a do-nothing *join task*"
  ^czlab.flux.wflow.core.Activity [branches]

  (entity<> NulJoin
            {:typeid ::nuljoin :branches branches}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::andjoin [activity nxtCog _]

  (protoCog<>
    activity
    nxtCog
    {:interrupt
     (fn [cog job]
       (log/warn "and-join time out")
       (.update ^Stateful
                cog {:error true})
       (->> (:wait @cog)
            (onInterrupt cog job)))
     :action
     (fn [^Stateful this job]
       (let
         [{:keys [forks alarm next
                  error wait cnt]}
          @this
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
             next)
           :else
           (if-not (empty? forks)
             (do->nil
               (fanout cpu this forks)
               (.update this
                        {:alarm (sa! cpu this job wait)
                         :forks (count forks)}))
             next))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful AndJoin

  Initable
  (init [_ cog]
    (let [{:keys [waitSecs branches]}
          @data]
      (->> {:cnt (AtomicInteger. 0)
            :wait waitSecs
            :forks (mapv #(wrapa %) branches)}
           (.init ^Initable cog))))

  Activity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- andjoin
  "Create a *join(and) task*"
  ^czlab.flux.wflow.core.Activity
  [branches waitSecs]

  (entity<> AndJoin
            {:branches branches
             :waitSecs waitSecs :typeid ::andjoin}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::orjoin [activity nxtCog _]

  (protoCog<>
    activity
    nxtCog
    {:interrupt
     (fn [^Stateful cog job]
       (log/debug "or-join time out")
       (.update cog {:error true})
       (->> (:wait @cog)
            (onInterrupt cog job)))
     :action
     (fn [^Stateful this job]
       (let [{:keys [forks alarm next
                     error wait cnt]}
             @this
             cpu (gcpu job)]
         (cond
           (true? error)
           nil
           (number? forks)
           (let [_ (when alarm
                     (.cancel ^TimerTask alarm)
                     (.update this {:alarm nil}))
                 rc next]
             (if (>= (-> ^AtomicInteger cnt
                         .incrementAndGet ) forks)
               (ri! this))
             rc)
           :else
           (if-not (empty? forks)
             (do->nil
               (fanout cpu this forks)
               (.update this
                        {:alarm (sa! cpu this job wait)
                         :forks (count forks)}))
             next))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful OrJoin

  Initable
  (init [_ cog]
    (let [{:keys [waitSecs branches]}
          @data]
      (->> {:cnt (AtomicInteger. 0)
            :wait waitSecs
            :forks (mapv #(wrapa %) branches)}
           (.init ^Initable cog))))

  Activity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- orjoin
  "Create a *or join activity*"
  ^czlab.flux.wflow.core.Activity
  [branches waitSecs]

  (entity<> OrJoin
            {:branches branches
             :waitSecs waitSecs :typeid ::orjoin}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::decision [activity nxtCog _]

  (protoCog<>
    activity
    nxtCog
    {:action
     (fn [cog job]
       (let [{:keys [bexpr next
                     then else]}
             @cog
             b (bexpr job)]
         (ri! cog)
         (if b
           (wrapc then next)
           (wrapc else next))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Decision

  Initable
  (init [this cog]
    (->> (select-keys @data
                      [:bexpr :then :else])
         (.init ^Initable cog)))

  Activity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn decision<>
  "Create a *decision activity*"
  {:tag czlab.flux.wflow.core.Activity}

  ([bexpr then]
   (decision<> bexpr then nil))

  ([bexpr then else]
   (entity<> Decision
             {:typeid ::decision
              :bexpr bexpr :then then :else else})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cogit!Loop "" [_ nxtCog]

  (fn [^Stateful this job]
    (let [{:keys [bexpr body next]}
          @this
          nx (:next @this)]
      (if-not (bexpr job)
        (do (ri! this) next)
        (if-some
          [^Stateful
           n (.handle
               ^czlab.flux.wflow.core.ICog body job)]
          (cond
            (= ::delay (:typeid @(:proto @n)))
            (doto n (.update {:next this}))

            (identical? n this)
            this

            ;;replace body
            (satisfies? ICog n)
            (do
              (.update n {:next (:next @body)})
              (.update this {:body n})
              this)

            :else this)
          this)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::while [activity nxtCog _]

  (protoCog<>
    activity
    nxtCog
    {:action (cogit!Loop activity nxtCog)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful WhileLoop

  Initable
  (init [_ cog]
    (let [{:keys [body bexpr]}
          @data]
      (->> {:bexpr bexpr
            :body (wrapc body cog)}
           (.init ^Initable cog))))

  Activity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn wloop<>
  "Create a *while-loop activity*"
  ^czlab.flux.wflow.core.Activity
  [bexpr body]
  {:pre [(fn? bexpr)]}

  (entity<> WhileLoop
            {:bexpr bexpr :body body :typeid ::while}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::split [activity nxtCog _]

  (protoCog<>
    activity
    nxtCog
    {:action
     (fn [this job]
       (let [{:keys [joinStyle wait forks]}
             @this
             t (cond
                 (= :and joinStyle)
                 (andjoin forks wait)
                 (= :or joinStyle)
                 (orjoin forks wait)
                 :else
                 (nuljoin forks))]
         (wrapc t (:next @this))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Split

  Initable
  (init [_ cog]
    (let [{:keys [branches options]} @data]
      (->> {:forks (mapv #(wrapa %) branches)
            :wait (or (:waitSecs options) 0)
            :joinStyle
            (if (keyword? options)
              options (or (:join options) :nil))}
           (.init ^Initable cog))))

  Activity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn fork<>
  "Create a *split activity*"
  ^czlab.flux.wflow.core.Activity
  [options & branches]
  {:pre [(some? options)]}

  (log/debug "forking with [%d] branches" (count branches))
  (entity<> Split
            {:branches branches
             :options options :typeid ::split}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::group [activity nxtCog _]

  (protoCog<>
    activity
    nxtCog
    {:action
     (fn [this job]
       (let [{:keys [tlist next]} @this]
         (if-not (empty? @tlist)
           (let [a (wrapc (first @tlist) this)
                 r (rest @tlist)
                 rc (.handle
                      ^czlab.flux.wflow.core.Cog a job)]
             (reset! tlist r)
             rc)
           (do (ri! this) next))))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Group

  Initable
  (init [_ cog]
    (->> {:tlist (atom (mapv #(wrapa %)
                             (:acts @data)))}
         (.init ^Initable cog)))

  Activity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn group<>
  "Create a *group activity*"
  ^czlab.flux.wflow.core.Activity
  [a & xs]
  {:pre [(some? a)]}

  (entity<> Group
            {:acts (concat [a] xs) :typeid ::group}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rangeExpr "" [lower upper]
  (let [loopy (atom lower)]
    #(let [v @loopy]
       (if (< v upper)
         (do->true
           (.update ^Stateful
                    % {::rangeindex v})
           (swap! loopy inc))
         false))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::for [activity nxtCog _]

  (protoCog<>
    activity
    nxtCog
    {:action (cogit!Loop activity nxtCog)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful ForLoop

  Initable
  (init [_ cog]
    (let [{:keys [lower upper body]}
          @data
          j (gjob cog)]
      (->> {:bexpr (rangeExpr (lower j)
                              (upper j))
            :body (wrapc body cog)}
           (.init ^Initable cog))))

  Activity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn floop<>
  "Create a *for-loop activity*"
  ^czlab.flux.wflow.core.Activity
  [lower upper body]
  {:pre [(fn? lower)(fn? upper)]}

  (entity<> ForLoop
            {:lower lower
             :upper upper
             :body body :typeid ::for}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defentity Job
  IJob
  (origin [_] (::origin @data)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn job<>
  "Create a job context"
  {:tag czlab.flux.wflow.core.Job}

  ([_sch ws] (job<> _sch ws nil))
  ([_sch] (job<> _sch nil nil))
  ([_sch ws originObj]
   (entity<> Job
             {:id (str "job#" (seqint2))
              ::origin originObj
              ::scheduler _sch
              ::lastresult nil
              ::wflow ws})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wsExec "" [ws job]

  (.update ^Stateful job {::wflow ws})
  (.run ^Schedulable
        (::scheduler @job)
        (wrapc (::head @ws) (nihilCog<> job))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wsHead
  "" [t0 more] (if-not (empty? more) (apply group<> t0 more) t0))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful WorkstreamEx
  IWorkstream
  (execWith [me job] (wsExec me job))
  Catchable
  (catche [_ e] ((::efn @data) e)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Workstream
  IWorkstream
  (execWith [me job] (wsExec me job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn workstream<>
  "Create a work flow with the
  follwing syntax:
  (workstream<> taskdef [taskdef...] [:catch func])"
  ^czlab.flux.wflow.core.IWorkstream
  [task0 & args] {:pre [(some? task0)]}
  ;;first we look for error handling which
  ;;must be at the end of the args
  (let
    [[a b] (take-last 2 args)
     [err tasks]
     (if (and (= :catch a)
              (fn? b))
       [b (drop-last 2 args)]
       [nil args])]
    (if (fn? err)
      (entity<> WorkstreamEx
                {::head (wsHead task0 tasks)
                 ::efn err})
      (entity<> Workstream
                {::head (wsHead task0 tasks)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn wrapScript
  "Wrap function into a script"
  ^czlab.flux.wflow.core.Activity
  [func] {:pre [(fn? func)]} (script<> func))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


