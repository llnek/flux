;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "A minimal worflow framework."
      :author "Kenneth Leung"}

  czlab.flux.wflow

  (:require [czlab.basal.log :as log]
            [clojure.java.io :as io]
            [clojure.string :as cs]
            [czlab.basal.core :as c]
            [czlab.basal.meta :as m]
            [czlab.basal.str :as s])

  (:import [java.util.concurrent.atomic AtomicInteger]
           [java.util TimerTask]
           [czlab.jasal
            RunnableWithId
            Interruptable
            Settable
            Catchable
            Initable
            Nameable
            Schedulable]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private cact? [a] `(c/cast? czlab.flux.wflow.Activity ~a))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol Workstream
  "Work flow"
  (exec-with [_ job]
             "Execute this workflow trigged by this job"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol Job
  "A job"
  (rootage [_] "Source triggered the creation of this job"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol Cog
  "A step in the workflow"
  (g-handle [_ arg] "Run this step")
  (g-job [_] "Get the job")
  (re-run [_] "Re-run this step"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol Activity
  "A workflow activity"
  (cog-arg [_ nxtCog] "Configure the cog")
  (create-cog [_ nxtCog] "Instantiate this activity"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(declare cogRunAfter cogRun script<>)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn gjob "" [cog] (some-> cog g-job))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wrapa
  "If x is a function, wrapped it
  inside a script activity"
  [x]
  (cond
    (satisfies? Activity x) x
    (fn? x) (script<> x)
    :else
    (c/throwBadArg "bad param type: "
                   (if (nil? x) "null" (class x)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wrapc
  "Create a cog from an Activity"
  [x nxt] (-> (wrapa x) (create-cog nxt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rerun! "Rerun this step" [cog]
  (if-some [job (gjob cog)]
    (.reschedule ^Schedulable (::scheduler @job) cog)))

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
  (if (c/spos? wsecs)
    (.alarm cpu cog job (* 1000 wsecs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- ri!
  "Reset a cog" [cog]
  (.init ^Initable (:proto @cog)  cog))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-mutable CogObj
  Cog
  (g-job [me] (if-some [n (:next @me)] (g-job n) (:job @me)))
  (g-handle [me job] (c/if-fn? [f (:action @me)] (f me job)))
  (re-run [_] (rerun! _))
  RunnableWithId
  (run [_] (cogRun _))
  (id [me] (:id @me))
  Interruptable
  (interrupt [me job]
    (c/if-fn? [f (:interrupt @me)] (f me job)))
  Initable
  (init [me m]
    (c/if-fn? [f (:initFn @me)] (f me m) (c/copy* me m))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private protoCog<> "" [act nxt args]
  `(c/mutable<> CogObj
                (merge ~args
                       {:proto ~act
                        :next ~nxt
                        :id (str "cog#" (c/seqint2)) })))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- createCogEx [nihil job]
  (assert (some? job))
  (c/doto->>
    (protoCog<> nihil nil {:job job})
    (.init ^Initable nihil)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object Nihil
  Activity (create-cog [me cog] (createCogEx me (g-job cog)))
  Nameable (name [me] (name (:typeid me)))
  Initable (init [_ m] ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nihil<> "*terminal*" [] (c/object<> Nihil {:typeid ::nihil}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cogRunAfter "" [cog]
  (if-some [cpu (some-> cog g-job gcpu)]
    (if (= ::nihil (get-in @cog [:proto :typeid]))
      (c/do->nil (log/debug "nihil :> stop/skip"))
      (do
        (log/debug "next-cog :> %s" (:proto @cog))
        (.run cpu cog)))
    (log/debug "next-cog :> null")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private nihilCog<> "" [job] `(createCogEx (nihil<>) ~job))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private err! "" [c e] `(doto {:cog ~c :error ~e}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cogRun "" [cog]
  (let [{:keys [proto]} @cog
        job (gjob cog)
        ws (::wflow @job)]
    (-> job gcpu (.dequeue cog))
    (cogRunAfter
      (try
        (log/debug "%s :action()" proto)
        (g-handle cog job)
        (catch Throwable e#
          (if-some [a (if-some [c (c/cast? Catchable ws)]
                        (cact? (.catche c (err! cog e#)))
                        (c/do->nil (log/error e# "")))]
            (wrapc a (nihilCog<> job))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- onInterrupt
  "A timer has expired - used by (joins)"
  [cog job waitSecs]
  (let [err (format "*interrupt* %s : %d secs"
                    "timer expired" waitSecs)
        ws (::wflow @job)]
    (cogRunAfter
      (if-some [a (if-some [c (c/cast? Catchable ws)]
                    (cact? (.catche c (err! cog err)))
                    (c/do->nil (log/error err "")))]
        (wrapc a (nihilCog<> job))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object Postpone
  Initable
  (init [me cog]
    (.init ^Initable cog {:delay (:delaySecs me)}))
  Activity
  (cog-arg [me nxCog]
    {:action
     (fn [cog job]
       (c/do->nil
         (let [{:keys [next delay]}
               @cog
               cpu (gcpu job)]
           (->> (or delay 0)
                (* 1000) (.postpone cpu next))
           (ri! cog))))})
  (create-cog [me nx]
    (c/doto->> (->> (cog-arg me nx)
                    (protoCog<> me nx)) (.init me )))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn postpone<>
  "Create a *delay activity*"
  [delaySecs]
  {:pre [(c/spos? delaySecs)]}
  (c/object<> Postpone {:delaySecs delaySecs :typeid ::delay}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object Script
  Initable
  (init [me cog]
    (let [{:keys [workFunc]} me
          [s _] (m/countArity workFunc)]
      (.init ^Initable cog {:work workFunc :arity s})))
  Activity
  (cog-arg [me nxCog]
    {:action
     (fn [cog job]
       (let [{:keys [next work arity]}
             @cog
             a
             (cond
               (c/in? arity 2) (work cog job)
               (c/in? arity 1) (work job)
               :else (c/throwBadArg "Expected %s: on %s"
                                    "arity 2 or 1" (class work)))]
         (ri! cog)
         (if-some [a' (cact? a)] (create-cog a' next) next)))})
  (create-cog [me nx]
    (c/doto->> (->> (cog-arg me nx)
                    (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (s/stror (:scriptname me) (name (:typeid me)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn script<>
  "Create a *scriptable activity*"
  ([workFunc] (script<> workFunc nil))
  ([workFunc script-name]
   {:pre [(fn? workFunc)]}
   (c/object<> Script
               {:workFunc workFunc
                :scriptname script-name :typeid ::script})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object Switch
  Initable
  (init [me cog]
    (let [{:keys [choices cexpr dft]}
          me
          nx (:next @cog)]
      (->> {:dft (some-> dft (wrapc nx))
            :cexpr cexpr
            :choices
            (c/preduce<vec>
              #(let [[k v] %2]
                 (-> (conj! %1 k)
                     (conj! (wrapc v nx))))
              (partition 2 choices))}
           (.init ^Initable cog))))
  Activity
  (cog-arg [me nxCog]
    {:action
     (fn [cog job]
       (let [{:keys [cexpr dft choices]}
             @cog
             a (if-some [m (cexpr job)]
                 (some #(if (= m (first %1))
                          (last %1))
                       (partition 2 choices)))]
         (ri! cog)
         (or a dft)))})
  (create-cog [me nx]
    (c/doto->> (->> (cog-arg me nx)
                    (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn choice<>
  "Create a *choice activity*"
  [cexpr dft & choices]
  {:pre [(fn? cexpr)
         (or (empty? choices)
             (even? (count choices)))]}
  (let [choices (c/preduce<vec>
                  #(let [[k v] %2]
                     (-> (conj! %1 k)
                         (conj! (wrapa v))))
                  (partition 2 choices))
        dft (some-> dft wrapa)]
    (c/object<> Switch
                {:choices choices
                 :cexpr cexpr
                 :dft dft
                 :typeid ::switch})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object NulJoin
  Initable
  (init [me cog]
    (->> {:forks (mapv #(wrapa %)
                       (:branches me))}
         (.init ^Initable cog )))
  Activity
  (cog-arg [me nxCog]
  {:action
   (fn [cog job]
     (let [{:keys [forks next]}
           @cog
           nx (nihilCog<> job)
           cpu (gcpu job)]
       ;;spawn all children and continue
       (doseq [t forks]
         (.run cpu (wrapc t nx)))
       next))})
  (create-cog [me nx]
    (c/doto->> (->> (cog-arg me nx)
                    (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nuljoin
  "Create a do-nothing *join task*"
  [branches]
  (c/object<> NulJoin {:typeid ::nuljoin :branches branches}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object AndJoin
  Initable
  (init [me cog]
    (let [{:keys [waitSecs branches]} me]
      (->> {:cnt (AtomicInteger. 0)
            :wait waitSecs
            :forks (mapv #(wrapa %) branches)}
           (.init ^Initable cog))))
  Activity
  (cog-arg [me nxCog]
  {:interrupt
   (fn [cog job]
     (log/warn "and-join time out")
     (c/setf! cog :error true)
     (onInterrupt cog job (:wait @cog)))
   :action
   (fn [cog job]
     (let [{:keys [error wait cnt
                   forks alarm next]}
           @cog
           cpu (gcpu job)]
       (cond
         (true? error)
         (c/do->nil (log/debug "too late"))
         (number? forks)
         (when (== forks
                   (-> ^AtomicInteger
                       cnt .incrementAndGet))
           ;;children all returned
           (some-> ^TimerTask alarm .cancel)
           (ri! cog)
           next)
         :else
         (if-not (empty? forks)
           (c/do->nil
             (fanout cpu cog forks)
             (c/copy* cog
                      {:alarm (sa! cpu cog job wait)
                       :forks (count forks)}))
           next))))})
  (create-cog [me nx]
    (c/doto->> (->> (cog-arg me nx)
                    (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- andjoin
  "Create a *join(and) task*"
  [branches waitSecs]
  (c/object<> AndJoin
              {:branches branches
               :waitSecs waitSecs :typeid ::andjoin}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object OrJoin
  Initable
  (init [me cog]
    (let [{:keys [waitSecs branches]} me]
      (->> {:cnt (AtomicInteger. 0)
            :wait waitSecs
            :forks (mapv #(wrapa %) branches)}
           (.init ^Initable cog))))
  Activity
  (cog-arg [me nxCog]
    {:interrupt
     (fn [cog job]
       (log/debug "or-join time out")
       (c/setf! cog :error true)
       (onInterrupt cog job (:wait @cog)))
     :action
     (fn [cog job]
       (let [{:keys [error wait cnt
                     forks alarm next]}
             @cog
             cpu (gcpu job)]
         (cond
           (true? error)
           nil
           (number? forks)
           (let [rc next]
             (some-> ^TimerTask alarm .cancel)
             (c/unsetf! cog :alarm)
             (if (>= (-> ^AtomicInteger
                         cnt .incrementAndGet) forks)
               (ri! cog))
             rc)
           :else
           (if-not (empty? forks)
             (c/do->nil
               (fanout cpu cog forks)
               (c/copy* cog
                        {:alarm (sa! cpu cog job wait)
                         :forks (count forks)}))
             next))))})
  (create-cog [me nx]
    (c/doto->> (->> (cog-arg me nx)
                    (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- orjoin
  "Create a *or join activity*"
  [branches waitSecs]
  (c/object<> OrJoin
              {:branches branches
               :waitSecs waitSecs :typeid ::orjoin}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object Decision
  Initable
  (init [me cog]
    (->> (select-keys me
                      [:bexpr :then :else])
         (.init ^Initable cog)))
  Activity
  (cog-arg [me nxCog]
    {:action
     (fn [cog job]
       (let [{:keys [bexpr next
                     then else]}
             @cog
             b (bexpr job)]
         (ri! cog)
         (if b
           (wrapc then next)
           (wrapc else next))))})
  (create-cog [me nx]
    (c/doto->> (->> (cog-arg me nx)
                    (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn decision<>
  "Create a *decision activity*"

  ([bexpr then]
   (decision<> bexpr then nil))

  ([bexpr then else]
   (c/object<> Decision
               {:typeid ::decision
                :bexpr bexpr :then then :else else})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cogit!Loop "" [_]
  (fn [cog job]
    (let [{:keys [bexpr
                  body next]} @cog]
      (if-not (bexpr job)
        (do (ri! cog) next)
        (if-some [n (g-handle body job)]
          (cond
            ;;you can add a pause in-between iterations
            (= ::delay (:typeid (:proto @n)))
            (c/setf! n :next cog)
            (c/self? n cog)
            cog
            ;;replace body
            (satisfies? Cog n)
            (do
              (c/setf! n :next (:next @body))
              (c/setf! cog :body n)
              cog)
            :else cog)
          cog)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object WhileLoop
  Initable
  (init [me cog]
    (let [{:keys [body bexpr]} me]
      (->> {:bexpr bexpr
            :body (wrapc body cog)}
           (.init ^Initable cog))))
  Activity
  (cog-arg [me nxCog]
    {:action (cogit!Loop me)})
  (create-cog [me nx]
    (c/doto->> (->> (cog-arg me nx)
                    (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn wloop<>
  "Create a *while-loop activity*"
  [bexpr body]
  {:pre [(fn? bexpr)]}
  (c/object<> WhileLoop
              {:bexpr bexpr :body body :typeid ::while}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object Split
  Initable
  (init [me cog]
    (let [{:keys [branches options]} me]
      (->> {:forks (mapv #(wrapa %) branches)
            :wait (or (:waitSecs options) 0)
            :joinStyle
            (if (keyword? options)
              options (or (:join options) :nil))}
           (.init ^Initable cog))))
  Activity
  (cog-arg [me nxCog]
    {:action
     (fn [cog job]
       (let [{:keys [joinStyle wait forks]}
             @cog
             t (cond
                 (= :and joinStyle)
                 (andjoin forks wait)
                 (= :or joinStyle)
                 (orjoin forks wait)
                 :else
                 (nuljoin forks))]
         (wrapc t (:next @cog))))})
  (create-cog [me nx]
    (c/doto->> (->> (cog-arg me nx)
                    (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn fork<>
  "Create a *split activity*"
  [options & branches]
  {:pre [(some? options)]}
  (log/debug "forking with [%d] branches" (count branches))
  (c/object<> Split
              {:branches branches
               :options options :typeid ::split}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object Group
  Initable
  (init [me cog]
    (->> {:tlist (atom (mapv #(wrapa %)
                             (:acts me)))}
         (.init ^Initable cog)))
  Activity
  (cog-arg [me nxCog]
    {:action
     (fn [cog job]
       (let [{:keys [tlist next]} @cog]
         (if-not (empty? @tlist)
           (let [a (wrapc (first @tlist) cog)
                 r (rest @tlist)
                 rc (g-handle a job)]
             (reset! tlist r)
             rc)
           (do (ri! cog) next))))})
  (create-cog [me nx]
    (c/doto->> (->> (cog-arg me nx)
                    (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn group<>
  "Create a *group activity*"
  [a & xs]
  {:pre [(some? a)]}
  (c/object<> Group {:acts (concat [a] xs) :typeid ::group}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rangeExpr "" [lower upper]
  (let [loopy (atom lower)]
    #(let [job %1 v @loopy]
       (if (< v upper)
         (c/do->true
           (c/alter-atomic job assoc ::rangeindex v)
           (swap! loopy inc))
         false))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object ForLoop
  Initable
  (init [me cog]
    (let [{:keys [lower upper body]}
          me
          j (gjob cog)]
      (->> {:bexpr (rangeExpr (lower j)
                              (upper j))
            :body (wrapc body cog)}
           (.init ^Initable cog))))
  Activity
  (cog-arg [me nxCog]
    {:action (cogit!Loop me)})
  (create-cog [me nx]
    (c/doto->> (->> (cog-arg me nx)
                    (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn floop<>
  "Create a *for-loop activity*"
  [lower upper body]
  {:pre [(fn? lower)(fn? upper)]}
  (c/object<> ForLoop
              {:lower lower
               :upper upper
               :body body :typeid ::for}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-atomic JobObj
  Job
  (rootage [me] (::origin @me)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn job<>
  "Create a job context"
  ([_sch ws] (job<> _sch ws nil))
  ([_sch] (job<> _sch nil nil))
  ([_sch ws originObj]
   (c/atomic<> JobObj
               {:id (str "job#" (c/seqint2))
                ::origin originObj
                ::scheduler _sch
                ::lastresult nil
                ::wflow ws})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wsExec "" [ws job]
  (c/alter-atomic job assoc ::wflow ws)
  (.run ^Schedulable
        (::scheduler @job)
        (wrapc (::head ws) (nihilCog<> job))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wsHead
  "" [t0 more] (if-not (empty? more) (apply group<> t0 more) t0))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object WorkstreamExObj
  Workstream
  (exec-with [me job] (wsExec me job))
  Catchable
  (catche [me e] ((::efn me) e)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(c/decl-object WorkstreamObj
  Workstream
  (exec-with [me job] (wsExec me job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn workstream<>
  "Create a work flow with the
  follwing syntax:
  (workstream<> taskdef [taskdef...] [:catch func])"
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
      (c/object<> WorkstreamExObj
                {::head (wsHead task0 tasks)
                 ::efn err})
      (c/object<> WorkstreamObj {::head (wsHead task0 tasks)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn wrapScript
  "Wrap function into a script"
  [func] {:pre [(fn? func)]} (script<> func))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


