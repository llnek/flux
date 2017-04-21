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

  (:require [czlab.basal.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as cs])

  (:use [czlab.basal.core]
        [czlab.basal.meta]
        [czlab.basal.str])

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
(defmacro ^:private cact? [a] `(cast? czlab.flux.wflow.Activity ~a))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol Workstream "" (exec-with [_ job] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol Job "" (rootage [_] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol Cog ""
  (g-handle [_ arg] "")
  (g-job [_] "")
  (re-run [_] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol Activity
  ""
  (cog-arg [_ nxtCog] "")
  (create-cog [_ nxtCog] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(declare cogRunAfter cogRun script<>)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn gjob "" [cog] (some-> cog g-job))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wrapa "" [x]
  (cond
    (satisfies? Activity x) x
    (fn? x) (script<> x)
    :else
    (throwBadArg "bad param type: "
                 (if (nil? x) "null" (class x)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wrapc "" [x nxt] (-> (wrapa x) (create-cog nxt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rerun! "" [cog]
  (if-some [j (gjob cog)]
    (.reschedule ^Schedulable (::scheduler @j) cog)))

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
(decl-mutable CogObj
  Cog
  (g-job [me] (if-some [n (:next @me)] (g-job n) (:job @me)))
  (g-handle [me job] (if-fn? [f (:action @me)] (f me job)))
  (re-run [_] (rerun! _))
  RunnableWithId
  (run [_] (cogRun _))
  (id [me] (:id @me))
  Interruptable
  (interrupt [me job]
    (if-fn? [f (:interrupt @me)] (f me job)))
  Initable
  (init [me m]
    (if-fn? [f (:initFn @me)] (f me m) (copy* me m))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro ^:private protoCog<> "" [act nxt args]
  `(mutable<> CogObj
              (merge ~args
                     {:proto ~act
                      :next ~nxt
                      :id (str "cog#" (seqint2)) })))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- createCogEx [nihil job]
  (assert (some? job))
  (doto->>
    (protoCog<> nihil nil {:job job})
    (.init ^Initable nihil )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object Nihil
  Activity (create-cog [me cog] (createCogEx me (g-job cog)))
  Nameable (name [me] (name (:typeid me)))
  Initable (init [_ m] ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nihil<> "*terminal*" [] (object<> Nihil {:typeid ::nihil}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- cogRunAfter "" [cog]
  (if-some [cpu (some-> cog g-job gcpu)]
    (if (= ::nihil (get-in @cog [:proto :typeid]))
      (do->nil (log/debug "nihil :> stop/skip"))
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
          (if-some [a (if-some [c (cast? Catchable ws)]
                        (cact? (.catche c (err! cog e#)))
                        (do->nil (log/error e# "")))]
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
      (if-some [a (if-some [c (cast? Catchable ws)]
                    (cact? (.catche c (err! cog err)))
                    (do->nil (log/error err "")))]
        (wrapc a (nihilCog<> job))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object Postpone
  Initable
  (init [me cog]
    (.init ^Initable cog {:delay (:delaySecs me)}))
  Activity
  (cog-arg [me nxCog]
    {:action
     (fn [cog job]
       (do->nil
         (let [{:keys [next delay]}
               @cog
               cpu (gcpu job)]
           (->> (or delay 0)
                (* 1000) (.postpone cpu next))
           (ri! cog))))})
  (create-cog [me nx]
    (doto->> (->> (cog-arg me nx)
                  (protoCog<> me nx)) (.init me )))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn postpone<>
  "Create a *delay activity*"
  [delaySecs]
  {:pre [(spos? delaySecs)]}
  (object<> Postpone {:delaySecs delaySecs :typeid ::delay}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object Script
  Initable
  (init [me cog]
    (let [{:keys [workFunc]} me
          [s _] (countArity workFunc)]
      (.init ^Initable cog {:work workFunc :arity s})))
  Activity
  (cog-arg [me nxCog]
    {:action
     (fn [cog job]
       (let [{:keys [next work arity]}
             @cog
             a
             (cond
               (in? arity 2) (work cog job)
               (in? arity 1) (work job)
               :else (throwBadArg "Expected %s: on %s"
                                  "arity 2 or 1" (class work)))]
         (ri! cog)
         (if-some [a' (cact? a)] (create-cog a' next) next)))})
  (create-cog [me nx]
    (doto->> (->> (cog-arg me nx)
                  (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (stror (:scriptname me) (name (:typeid me)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn script<>
  "Create a *scriptable activity*"
  ([workFunc] (script<> workFunc nil))
  ([workFunc script-name]
   {:pre [(fn? workFunc)]}
   (object<> Script
             {:workFunc workFunc
              :scriptname script-name :typeid ::script})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object Switch
  Initable
  (init [me cog]
    (let [{:keys [choices cexpr dft]}
          me
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
    (doto->> (->> (cog-arg me nx)
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
  (let [choices (preduce<vec>
                  #(let [[k v] %2]
                     (-> (conj! %1 k)
                         (conj! (wrapa v))))
                  (partition 2 choices))
        dft (some-> dft wrapa)]
    (object<> Switch
              {:choices choices
               :cexpr cexpr
               :dft dft
               :typeid ::switch})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object NulJoin
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
    (doto->> (->> (cog-arg me nx)
                  (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nuljoin
  "Create a do-nothing *join task*"
  [branches]
  (object<> NulJoin {:typeid ::nuljoin :branches branches}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object AndJoin
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
     (setf! cog :error true)
     (onInterrupt cog job (:wait @cog)))
   :action
   (fn [cog job]
     (let [{:keys [error wait cnt
                   forks alarm next]}
           @cog
           cpu (gcpu job)]
       (cond
         (true? error)
         (do->nil (log/debug "too late"))
         (number? forks)
         (when (== forks
                   (-> ^AtomicInteger
                       cnt .incrementAndGet ))
           ;;children all returned
           (some-> ^TimerTask alarm .cancel)
           (ri! cog)
           next)
         :else
         (if-not (empty? forks)
           (do->nil
             (fanout cpu cog forks)
             (copy* cog
                    {:alarm (sa! cpu cog job wait)
                     :forks (count forks)}))
           next))))})
  (create-cog [me nx]
    (doto->> (->> (cog-arg me nx)
                  (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- andjoin
  "Create a *join(and) task*"
  [branches waitSecs]
  (object<> AndJoin
            {:branches branches
             :waitSecs waitSecs :typeid ::andjoin}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object OrJoin
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
       (setf! cog :error true)
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
             (unsetf! cog :alarm)
             (if (>= (-> ^AtomicInteger
                         cnt .incrementAndGet) forks)
               (ri! cog))
             rc)
           :else
           (if-not (empty? forks)
             (do->nil
               (fanout cpu cog forks)
               (copy* cog
                      {:alarm (sa! cpu cog job wait)
                       :forks (count forks)}))
             next))))})
  (create-cog [me nx]
    (doto->> (->> (cog-arg me nx)
                  (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- orjoin
  "Create a *or join activity*"
  [branches waitSecs]
  (object<> OrJoin
            {:branches branches
             :waitSecs waitSecs :typeid ::orjoin}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object Decision
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
    (doto->> (->> (cog-arg me nx)
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
   (object<> Decision
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
            (setf! n :next cog)
            (self? n cog)
            cog
            ;;replace body
            (satisfies? Cog n)
            (do
              (setf! n :next (:next @body))
              (setf! cog :body n)
              cog)
            :else cog)
          cog)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object WhileLoop
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
    (doto->> (->> (cog-arg me nx)
                  (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn wloop<>
  "Create a *while-loop activity*"
  [bexpr body]
  {:pre [(fn? bexpr)]}
  (object<> WhileLoop
            {:bexpr bexpr :body body :typeid ::while}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object Split
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
    (doto->> (->> (cog-arg me nx)
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
  (object<> Split
            {:branches branches
             :options options :typeid ::split}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object Group
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
    (doto->> (->> (cog-arg me nx)
                  (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn group<>
  "Create a *group activity*"
  [a & xs]
  {:pre [(some? a)]}
  (object<> Group {:acts (concat [a] xs) :typeid ::group}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rangeExpr "" [lower upper]
  (let [loopy (atom lower)]
    #(let [job %1 v @loopy]
       (if (< v upper)
         (do->true
           (alter-atomic job assoc ::rangeindex v)
           (swap! loopy inc))
         false))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object ForLoop
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
    (doto->> (->> (cog-arg me nx)
                  (protoCog<> me nx)) (.init me)))
  Nameable
  (name [me] (name (:typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn floop<>
  "Create a *for-loop activity*"
  [lower upper body]
  {:pre [(fn? lower)(fn? upper)]}
  (object<> ForLoop
            {:lower lower
             :upper upper
             :body body :typeid ::for}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-atomic JobObj
  Job
  (rootage [me] (::origin @me)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn job<>
  "Create a job context"
  ([_sch ws] (job<> _sch ws nil))
  ([_sch] (job<> _sch nil nil))
  ([_sch ws originObj]
   (atomic<> JobObj
             {:id (str "job#" (seqint2))
              ::origin originObj
              ::scheduler _sch
              ::lastresult nil
              ::wflow ws})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wsExec "" [ws job]
  (alter-atomic job assoc ::wflow ws)
  (.run ^Schedulable
        (::scheduler @job)
        (wrapc (::head ws) (nihilCog<> job))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- wsHead
  "" [t0 more] (if-not (empty? more) (apply group<> t0 more) t0))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object WorkstreamExObj
  Workstream
  (exec-with [me job] (wsExec me job))
  Catchable
  (catche [me e] ((::efn me) e)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(decl-object WorkstreamObj
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
      (object<> WorkstreamExObj
                {::head (wsHead task0 tasks)
                 ::efn err})
      (object<> WorkstreamObj {::head (wsHead task0 tasks)}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn wrapScript
  "Wrap function into a script"
  [func] {:pre [(fn? func)]} (script<> func))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


