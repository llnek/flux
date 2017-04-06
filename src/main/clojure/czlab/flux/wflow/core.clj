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
           [czlab.jasal
            Interruptable
            Idable
            Catchable
            Initable
            Nameable
            CU
            Schedulable]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)
(def ^:private js-last :$lastresult)
(def range-index :$rangeindex)
(declare cogRunAfter cogRun script<>)

;;(defprotocol Job
  ;;public interface Job extends Gettable , Settable, Idable, Debuggable {
  ;;public void setLastResult(Object v) ;
  ;;public void clrLastResult() ;
  ;;public Object lastResult() ;
  ;;public Schedulable scheduler();
  ;;public void clear();
  ;;public Object origin() ;
  ;;public Workstream wflow();

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol Cog ;; RunnableWithId, Interruptable
  ""
  (^Cog handle [_ arg] "")
  (job [_] "")
  (rerun [_] ""))

  ;;(setNext [_ n] "") :next
  ;;(^Activity :proto [_] "")
  ;;(attrs [_] "")
  ;;(^Cog next();

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol Activity
  "An Activity is the building block of a workflow."
  (^Cog createCog [_ nxtCog] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol Nihil
  "A nothing, nada task."
  (^Cog createCogEx [_ job] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol Workstream
  ""
  (execWith [_ job] ""))
  ;;public Object head();

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wrapa ""
  ^czlab.flux.wflow.core.Activity [x]
  (cond
    (satisfies? czlab.flux.wflow.core.Activity x) x
    (fn? x) (script<> x)
    :else
    (throwBadArg "bad param type: "
                 (if (nil? x) "null" (class x)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wrapc ""
  ^czlab.flux.wflow.xapi.ICog
  [x nxt] (-> (wrapa x) (.createCog nxt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rerun!
  "" [^czlab.flux.wflow.xapi.ICog cog]
  (if-some [s (some-> cog
                      .job
                      .deref
                      :$scheduler)]
    (. ^Schedulable s reschedule cog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private gcpu "" [job] `(:$scheduler @~job))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private sv!
  "Set vars" [info vs] `(swap! ~info assoc :vars ~vs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- mv!
  "Merge vars" [info m]
  (->> (merge (:vars @info) m) (sv! info)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- fanout
  "Fork off tasks"
  [^Schedulable cpu nx defs]
  (doseq [t defs] (. cpu
                     run
                     ^RunnableWithId (wrapc t nx))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- sa!
  "Set alarm"
  [^Schedulable cpu
   ^Interruptable cog job wsecs]
  (if (spos? wsecs)
    (. cpu alarm cog job (* 1000 wsecs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private sn!
  "Set next step pointer" [info nx]
  `(let [t# ~nx]
     (assert (some? t#)) (swap! ~info assoc :next t#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- ri!
  "Reset a cog" [cog]
  (-> ^Initable (:proto @cog) (.init cog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti cogit!
  "Create a Cog"
  {:private true
   :tag czlab.flux.wflow.xapi.ICog} (fn [a _ _] (:typeid @a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Cog

  czlab.flux.wflow.xpi.ICog

  (handle [_ job]
    (if-fn? [f (:action @data)]
      (f _ info job)))

  (job [_]
    (if-some [n (:next @data)]
      (.job ^czlab.flux.wflow.xpi.ICog n)
      (:job @data)))

  (rerun [_] (rerun! _))

  RunnableWithId

  (run [_] (cogRun _))
  (id [_] (:id @data))

  ;;(attrs [_] (:vars @info))
  ;;(next [_] (:next @info))
  ;;(proto [_] activity)

  Interruptable

  (interrupt [_ job]
    (if-fn? [f (:interrupt @data)]
      (f _ data job)))

  Initable

  (init [me m]
    (if-fn? [f (:initFn @data)]
      (f _ data m)
      (-> {:vars (or m {})}
          (.update me)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn protoCog<>
  "" [activity nxtCog args]
  (entity<> Cog
            (merge args
                   {:id (str "cog#" (seqint2))
                    :proto activity
                    :next nxtCog
                    :vars nil})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Nihil

  czlab.flux.wflow.xpi.Activity
  (create [_ cog]
    (.createEx _ (.job cog)))

  Nameable
  (name [_] (name (:typeid @data)))

  Initable
  (init [_ m] )

  czlab.flux.wflow.core.INihil

  (createCogEx [me job]
    (doto->> (cogit! me nil job)
             (.init ^Initable me ))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nihil<>
  "Special *terminal activity*"
  ^czlab.flux.wflow.xpi.INihil
  []
  (entity<> Nihil
            {:typeid ::nihil}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cogRunAfter
  "" [^czlab.flux.wflow.xpi.ICog cog]
  (if-some [cpu (some-> cog .job gcpu)]
    (if (satisfies?
          czlab.flux.wflow.xpi.INihil (:proto @cog))
      (log/debug "nihil :> stop/skip")
      (do
        (log/debug "next-cog :> %s"
                   (.name ^Nameable (:proto @cog)))
        (.run cpu cog)))
    (log/debug "next-cog :> null")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private nihilCog<> "" [job] `(.createEx (nihil<>) ~job))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private err! "" [c e] `(CogError. ~c ~e))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cogRun "" [^czlab.flux.wflow.xpi.ICog this]
  (log/debug "%s :action()"
             (.name ^Nameable (:proto @this)))
  (-> this .job gcpu (.dequeue this))
  (let
    [job (.job this)
     ws (:wflow @job)
     rc (try
          (.handle this job)
          (catch Throwable e#
            (if-some
              [a (if-some
                   [c (cast? Catchable ws)]
                   (->> (err! this e#)
                        (.catche c)
                        (cast? czlab.flux.wflow.xapi.IActivity))
                   (do->nil (log/error e# "")))]
              (wrapc a (nihilCog<> job)))))]
    (cogRunAfter rc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- onInterrupt
  "A timer has expired - used by (joins)"
  [^czlab.flux.wflow.xpi.ICog this job waitSecs]
  (let
    [err (format "*interrupt* %s : %d secs"
                 "timer expired" waitSecs)
     ws (:wflow @job)
     rc (if-some
          [a (if-some
               [c (cast? Catchable ws)]
               (->> (err! this err)
                    (.catche c)
                    (cast? czlab.flux.wflow.xapi.IActivity))
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

  (->>
    {:action
     (fn [this info job]
       (do->nil
         (let [nx (:next @this)
               cpu (gcpu job)]
           (->> (or (get-in info
                            [:vars :delay]) 0)
                (* 1000)
                (.postpone cpu nx))
           (ri! this))))}
    (protoCog<> activity nxtCog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Postpone
  Initable
  (init [_ cog]
    (. ^Initable
       cog
       init {:delay (:delaySecs @data)}))

  czlab.flux.wflow.xpi.IActivity

  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _ )))

  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn postpone<>
  "Create a *delay activity*"
  ^czlab.flux.wflow.xapi.IActivity [delaySecs]
  {:pre [(spos? delaySecs)]}

  (entity<> Postpone
            {:delaySecs delaySecs
             :typeid ::delay}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::script [activity nxtCog _]

  (->>
    {:action
     (fn [cog info job]
       (let
         [{:keys [work arity]}
          (get-in info [:vars])
          a
          (cond
            (contains? arity 2) (work c job)
            (contains? arity 1) (work job)
            :else
            (throwBadArg "Expected %s: on %s"
                         "arity 2 or 1" (class work)))
          nx (:next @cog)]
         (ri! cog)
         (if-some
           [a' (cast? czlab.flux.wflow.xpi.IActivity a)]
           (.create a' nx)
           nx)))}
    (protoCog<> activity nxtCog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Script
  Initable
  (init [_ cog]
    (let [[s _] (countArity
                  (:workFunc @data))]
      (. ^Initable
         cog
         init
         {:work workFunc :arity s})))

  czlab.flux.wflow.xpi.IActivity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil)
             (.init _)))

  Nameable
  (name [_] (stror (:scriptName @data)
                   (name (:typeid @data)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn script<>
  "Create a *scriptable activity*"
  {:tag czlab.flux.wflow.xpi.IActivity}

  ([workFunc] (script<> workFunc nil))
  ([workFunc script-name]
   {:pre [(fn? workFunc)]}
   (entity<> Script
             {:workFunc workFunc
              :typeid ::script})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::switch [activity nxtCog _]

  (->>
    {:action
     (fn [_ info job]
       (let [{:keys [cexpr dft choices]}
             (:vars info)
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

  czlab.flux.wflow.xpi.IActivity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [me] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn choice<>
  "Create a *choice activity*"
  ^czlab.flux.wflow.xapi.IActivity
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
  ::nuljoin
  [activity nxtCog _]

  ;;spawn all children and continue
  (->>
    {:action
     (fn [cog info job]
       (let [nx (nihilCog<> job)
             cpu (gcpu job)]
         (doseq [t (get-in info
                           [:vars :forks])]
           (.run cpu (wrapc t nx)))
         (:next @cog)))}
    (protoCog<> activity nxtCog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful NulJoin
  Initable
  (init [_ cog]
    (->> {:forks (mapv #(wrapa %) branches)}
         (. ^Initable cog init )))

  czlab.flux.wflow.xpi.IActivity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [me] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nuljoin
  "Create a do-nothing *join task*"
  ^czlab.flux.wflow.xapi.IActivity [branches]

  (entity<> NulJoin
            {:typeid ::nuljoin
             :branches branches}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::andjoin [activity nxtCog _]

  (->>
    {:interrupt
     (fn [cog info job]
       (log/warn "and-join time out")
       (. ^Stateful
          cog update {:error true})
       (->> (get-in info
                    [:vars :wait])
            (onInterrupt cog job)))
     :action
     (fn [this info job]
       (let
         [{:keys [forks alarm
                  error wait cnt]}
          (:vars info)
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
             (:next @this))

           :else
           (if-not (empty? forks)
             (do->nil
               (fanout cpu this forks)
               (->>
                 {:alarm
                  (sa! cpu this job wait)
                  :forks (count forks)}
                 (.update this )))
             (:next @this)))))}
    (protoCog<> activity nxtCog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Andjoin
  Initable
  (init [_ cog]
    (->> {:cnt (AtomicInteger. 0)
          :wait (:waitSecs @data)
          :forks (mapv #(wrapa %)
                       (:branches @data))}
         (.init ^Initable cog)))

  czlab.flux.wflow.xpi.IActivity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (name (:typeid @data))))
    ;;(typeid [_] :andjoin)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- andjoin
  "Create a *join(and) task*"
  ^czlab.flux.wflow.xapi.IActivity [branches waitSecs]

  (entity<> AndJoin
            {:branches branches
             :waitSecs waitSecs
             :typeid ::andjoin}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::orjoin [activity nxtCog _]

  (->>
    {:interrupt
     (fn [cog info job]
       (log/debug "or-join time out")
       (.update ^Stateful
                cog {:error true})
       (->> (get-in @cog
                    [:vars :wait])
            (onInterrupt _ job)))
     :action
     (fn [this info job]
       (let [{:keys [forks alarm
                     error wait cnt]}
             (:vars @info)
             cpu (gcpu job)
             nx (:next @this)]
         (cond
           (true? error)
           nil
           (number? forks)
           (let [rc
                 (when alarm
                   (.cancel ^TimerTask alarm)
                   (.update this {:alarm nil})
                   nx)]
             (if (>= (-> ^AtomicInteger cnt
                         .incrementAndGet ) forks)
               (ri! this))
             rc)
           :else
           (if-not (empty? forks)
             (do->nil
               (fanout cpu this forks)
               (->>
                 {:alarm
                  (sa! cpu this job wait)
                  :forks (count forks)}
                 (.update this )))
             (:next @this)))))}
    (protoCog<> activity nxtCog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful OrJoin
  Initable
  (init [_ cog]
    (->> {:cnt (AtomicInteger. 0)
          :wait (:waitSecs @data)
          :forks (mapv #(wrapa %)
                       (:branches @data))}
         (.init ^Initable cog)))

  czlab.flux.wflow.xpi.IActivity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))
  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- orjoin
  "Create a *or join activity*"
  ^czlab.flux.wflow.xapi.IActivity [branches waitSecs]

  (entity<> OrJoin
            {:branches branches
             :waitSecs waitSecs
             :typeid ::orjoin}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::decision [activity nxtCog _]

  (->>
    {:action
     (fn [cog info job]
       (let [{:keys [bexpr
                     then else]}
             (:vars @info)
             nx (:next @cog)
             b (bexpr job)]
         (ri! cog)
         (if b
           (wrapc then nx)
           (wrapc else nx))))}
    (protoCog<> activity nxtCog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Decision
  Initable
  (init [this cog]
    (->> (select-keys @data
                      [:bexpr :then :else])
         (.init ^Initable cog)))

  czlab.flux.wflow.xpi.IActivity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (name (:typeid @data))))
     ;;(typeid [_] :decision)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn decision<>
  "Create a *decision activity*"
  {:tag czlab.flux.wflow.xpi.IActivity}

  ([bexpr then]
   (decision<> bexpr then nil))

  ([bexpr then else]
   (entity<> Decision
             {:bexpr bexpr
              :then then :else else})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cogit!Loop "" [_ nxtCog]

  (fn [this info job]
    (let [{:keys [bexpr body]}
          (:vars @info)
          nx (:next @this)]
      (if-not (bexpr job)
        (do (ri! this) nx)
        (if-some
          [n (.handle body job)]
          (cond
            (= ::delay
               (:typeid @(:proto @n)))
            (doto n
              (.update {:next this}))

            (identical? n this)
            this

            ;;replace body
            (satisfies? czlab.flux.wflow.xpi.ICog n)
            (do
              (->> {:next (:next @body)}
                   (.update n ))
              (.update this {:body n})
              this)

            :else this)
          this)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::while [activity nxtCog _]

  (->> {:action (cogit!Loop activity nxtCog)}
       (protoCog<> activity nxtCog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstate WhileLoop
  Initable
  (init [_ cog]
    (->> {:bexpr bexpr
          :body (wrapc body c)}
         (.init ^Initable cog)))

  czlab.flux.wflow.xpi.IActivity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))
  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn wloop<>
  "Create a *while-loop activity*"
  ^czlab.flux.wflow.xapi.IActivity
  [bexpr body]
  {:pre [(fn? bexpr)]}

  (entity<> WhileLoop
            {:bexpr bexpr
             :body body
             :typeid ::while}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::split [activity nxtCog _]

  (->>
    {:action
     (fn [this info job]
       (let [{:keys [joinStyle wait forks]}
             (:vars @info)
             t (cond
                 (= :and joinStyle)
                 (andjoin forks wait)
                 (= :or joinStyle)
                 (orjoin forks wait)
                 :else
                 (nuljoin forks))]
         (wrapc t (:next @this))))}
    (protoCog<> activity nxtCog)))


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

  czlab.flux.wflow.xpi.IActivity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn fork<>
  "Create a *split activity*"
  ^czlab.flux.wflow.xapi.IActivity
  [options & branches]
  {:pre [(some? options)]}

  (log/debug "forking with [%d] branches" (count branches))
  (entity<> Split
            {:branches branches
             :options options
             :typeid ::split}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::group [activity nxtCog _]

  (->>
    {:action
     (fn [this info job]
       (let [cs (get-in @info
                        [:vars :list])
          nx (:next @this)]
         (if-not (empty? @cs)
           (let [a (wrapc (first @cs) this)
                 r (rest @cs)
                 rc (.handle a job)]
             (reset! cs r)
             rc)
           (do (ri! this) nx))))}
    (protoCog<> activity nxtCog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful Group
  Initable
  (init [_ cog]
    (->> {:list (atom (mapv #(wrapa %)
                            (concat [a] xs)))}
         (.init ^Initable cog)))

  czlab.flux.wflow.xpi.IActivity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn group<>
  "Create a *group activity*"
  ^czlab.flux.wflow.xapi.IActivity
  [a & xs]
  {:pre [(some? a)]}

  (entity<> Group
            {:xs xs
             :a a
             :typeid ::group}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rangeExpr "" [lower upper]
  (let [loopy (atom lower)]
    #(let [v @loopy]
       (if (< v upper)
         (do->true
           (.update ^Job % {range-index  v})
           (swap! loopy inc))
         false))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod cogit!
  ::for [activity nxtCog _]

  (->> {:action (cogit!Loop activity nxtCog)}
       (protoCog<> activity nxtCog)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defstateful ForLoop
  Initable
  (init [_ cog]
    (let [j (.job ^Cog cog)]
      (->> {:bexpr (rangeExpr (lower j)
                              (upper j))
            :body (wrapc body cog)}
           (.init ^Initable cog))))

  czlab.flux.wflow.xpi.IActivity
  (createCog [_ nx]
    (doto->> (cogit! _ nx nil) (.init _)))

  Nameable
  (name [_] (name (:typeid @data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn floop<>
  "Create a *for-loop activity*"
  ^Activity [lower upper body]
  {:pre [(fn? lower)(fn? upper)]}

  (entity<> ForLoop
            {:lower lower
             :upper upper
             :typeid ::for}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn job<>
  "Create a job context" {:tag Job}

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
  (. (.scheduler job)
     run
     (wrapc (.head ws) (nihilCog<> job))))

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
(defn wrapScript
  "Wrap function into a script"
  ^Activity [func] {:pre [(fn? func)]} (script<> func))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


