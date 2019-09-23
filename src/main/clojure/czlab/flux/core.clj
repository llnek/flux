;; Copyright © 2013-2019, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "A minimal worflow framework."
      :author "Kenneth Leung"}

  czlab.flux.core

  (:require [clojure.java.io :as io]
            [clojure.string :as cs]
            [czlab.basal.log :as l]
            [czlab.basal.util :as u]
            [czlab.basal.proc :as p]
            [czlab.basal.meta :as m]
            [czlab.basal.util :as u]
            [czlab.basal.xpis :as po]
            [czlab.basal.core :as c :refer [n#]])

  (:import [java.util.concurrent.atomic AtomicInteger]
           [java.util.concurrent TimeoutException]
           [java.util TimerTask]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol WorkFlow
  ""
  (exec [_ job]
        "Apply this workflow to the job."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol Job
  ""
  (wkflow [_] "")
  (runner [_] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol Step
  "A step in the workflow."
  (g-job [_] "Get the job."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol Symbol
  "A workflow symbol."
  (step-init [_ step] "Initialize the Step.")
  (step-reify [_ next] "Instantiate this Symbol."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro defwflow
  "Define a workflow." [name & tasks]
  `(def ~name (czlab.flux.core/workflow<> [ ~@tasks ])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/defmacro- csymb??
  "Cast to a Symbol?"
  [a] `(let [x# ~a] (if (c/sas? Symbol x#) x#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/defmacro- cstep??
  "Cast to a Step?"
  [a] `(let [x# ~a] (if (c/sas? Step x#) x#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/defmacro- is-null-join?
  "" [s] `(= (po/typeid ~s) ::null-join))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(c/defmacro- err!
  "" [c e] `(hash-map :step ~c :error ~e))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro script<>

  "Create a *scriptable symbol*."

  ([workFunc]
   `(script<> ~workFunc nil))

  ([workFunc script-name]
   `(czlab.basal.core/object<> czlab.flux.core.Script
                               :work-func ~workFunc
                               :script ~script-name
                               :typeid :czlab.flux.core/script)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(declare step-run-after step-run)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn wrap-symb??
  "If x is a function, wrapped it
  inside a script symbol*."
  [x]
  (cond (c/sas? Symbol x) x
        (fn? x) (script<> x)
        :else (u/throw-BadArg "bad param type: "
                              (if (nil? x) "null" (class x)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- wrapc
  "Create a step from this Symbol"
  [x nxt]
  (-> x wrap-symb?? (step-reify nxt)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- fanout
  "Fork off tasks."
  [job nx defs]
  (let [cpu (runner job)]
    (l/debug "fanout: forking [%d] sub-tasks." (c/n# defs))
    (doseq [t defs] (p/run cpu (wrapc t nx)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- sa!
  "Set alarm."
  [step job wsecs]
  (when (c/spos? wsecs)
    (p/alarm (runner job) (* 1000 wsecs) step job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- rinit!
  "Reset a step." [step]
  (if step (step-init (po/parent step) step)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- proto-step<>

  "Create a generic step object.  We need to know the type of step to
  instantiate, and the step to call after this step is run."
  [proto n args]

  (let [_id (str "step#" (u/seqint2))
        impl (atom (assoc args :next n))]
    (reify Step
      (g-job [_]
        (let [{:keys [job next]} @impl]
          (or job (g-job next))))
      Runnable
      (run [_] (step-run _ (:action @impl)))
      po/Configurable
      (get-conf [_ k] (get @impl k))
      (get-conf [_] @impl)
      (set-conf [_ x] _)
      (set-conf [_ k v] (swap! impl assoc k v) _)
      po/Idable
      (id [_] _id)
      po/Hierarchical
      (parent [_] proto)
      po/Initable
      (init [me m]
        (c/if-fn? [f (:init-fn @impl)]
          (f me m)
          (swap! impl merge m)) me)
      po/Interruptable
      (interrupt [me job]
        (c/if-fn? [f (:timer @impl)] (f me job))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord Terminal []
  Symbol
  (step-init [_ s] s)
  (step-reify [me nx]
    (u/throw-UOE "Cannot reify a terminal."))
  po/Typeable
  (typeid [_] (:typeid _))
  po/Nameable
  (name [me] (name (po/typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- terminal<>
  "*terminal*" [] (c/object<> Terminal :typeid ::terminal))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- terminal-step<>
  "" [job]
  {:pre [(some? job)]}
  (let [t (terminal<>)]
    (step-init t (proto-step<> t nil {:job job}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- step-run-after
  "" [orig arg]
  (let [par' (po/parent orig)
        job (g-job orig)
        cpu (runner job)
        step (if (csymb?? arg)
               (wrapc arg (terminal-step<> job)) arg)]
    (when (cstep?? step)
      (cond (= ::terminal (-> step po/parent po/typeid))
            (l/debug "%s :-> terminal" (po/name par'))
            (po/is-valid? cpu)
            (do (l/debug "%s :-> %s"
                         (po/name par') (po/name (po/parent step)))
                (p/run cpu step))
            :else
            (l/debug "no-cpu, %s skipping %s"
                     (po/name par') (po/name (po/parent step)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- step-run
  "" [step action]
  (let [job (g-job step)
        ws (wkflow job)]
    (step-run-after
      step
      (try (action step job)
           (catch Throwable e#
             (if-not (c/sas? po/Catchable ws)
               (c/do#nil (l/exception e#))
               (u/try!!!
                 (l/exception e#)
                 (po/catche ws (err! step e#)))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- join-timer
  [step job]
  (l/warn "%s: timer expired." (po/name (po/parent step)))
  (let [_ (->> (u/mono-flop<> true) (po/set-conf step :error))
        e (csymb?? (po/get-conf step :expiry))
        ws (wkflow job)
        n (when (and (nil? e)
                     (c/sas? po/Catchable ws))
            (->> (TimeoutException.)
                 (err! step) (po/catche ws)))]
    (some->> (some-> (or e n)
                     (wrapc (terminal-step<> job)))
             (p/run (runner job)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord Postpone []
  Symbol
  (step-init [me step]
    (po/init step {:delay (:delay-secs me)}))
  (step-reify [me nx]
    (->> {:action (fn [cur job]
                    (c/do#nil
                      (let [cpu (runner job)
                            {:keys [next delay]}
                            (po/get-conf cur)]
                        (->> (c/num?? delay 0)
                             (* 1000) (p/postpone cpu next))
                        (rinit! cur))))}
         (proto-step<> me nx) (step-init me)))
  po/Typeable
  (typeid [_] (:typeid _))
  po/Nameable
  (name [me] (name (po/typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro postpone<>

  "Create a *delay symbol*."

  [delay-secs]
  `(czlab.basal.core/object<> czlab.flux.core.Postpone
                              :delay-secs ~delay-secs
                              :typeid :czlab.flux.core/delay))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord Script []
  Symbol
  (step-init [me step]
    (let [{:keys [work-func]} me
          [s _] (m/count-arity work-func)]
      (po/init step {:work work-func :arity s})))
  (step-reify [me nx]
    (->> {:action (fn [cur job]
                    (let [{:keys [next work arity]}
                          (po/get-conf cur)
                          a (cond
                              (c/in? arity 2) (work cur job)
                              (c/in? arity 1) (work job)
                              :else (u/throw-BadArg "Expected %s: on %s"
                                                    "arity 2 or 1" (class work)))]
                      (rinit! cur)
                      (if-some [a' (csymb?? a)]
                        (step-reify a' next) next)))}
         (proto-step<> me nx) (step-init me)))
  po/Typeable
  (typeid [_] (:typeid _))
  po/Nameable
  (name [me] (c/stror (:script me) (name (po/typeid me)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord Switch []
  Symbol
  (step-init [_ step]
    (let [{:keys [choices cexpr default]} _
          {:keys [next]} (po/get-conf step)]
      (po/init step
               {:dft (some-> default wrap-symb?? (wrapc next))
                :cexpr cexpr
                :choices (c/preduce<vec>
                           #(let [[k v] %2]
                              (-> (conj! %1 k)
                                  (conj! (wrapc v next))))
                           (partition 2 choices))})))
  (step-reify [me nx]
    (->> {:action (fn [cur job]
                    (let [{:keys [cexpr dft choices]}
                          (po/get-conf cur)
                          m (cexpr job)]
                      (rinit! cur)
                      (or (if m
                            (some #(if (= m (c/_1 %1)) (c/_E %1))
                                  (partition 2 choices))) dft)))}
         (proto-step<> me nx) (step-init me)))
  po/Typeable
  (typeid [_] (:typeid _))
  po/Nameable
  (name [me] (name (po/typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro choice<>

  "Create a *choice symbol*."

  [cexpr & choices]
  (let [[a b] (take-last 2 choices)
        [dft args]
        (if (and b (= a :default))
          [b (drop-last 2 choices)] [nil choices])]
  `(czlab.basal.core/object<> czlab.flux.core.Switch
                              :choices [~@args]
                              :default ~dft
                              :cexpr ~cexpr
                              :typeid :czlab.flux.core/switch)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord Decision []
  Symbol
  (step-init [me step]
    (po/init step
             (select-keys me
                          [:bexpr :then :else])))
  (step-reify [me nx]
    (->> {:action (fn [cur job]
                    (let [{:keys [bexpr next
                                  then else]}
                          (po/get-conf cur)]
                      (rinit! cur)
                      (if (bexpr job)
                        (wrapc then next)
                        (wrapc else next))))}
         (proto-step<> me nx) (step-init me)))
  po/Typeable
  (typeid [_] (:typeid _))
  po/Nameable
  (name [me] (name (po/typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro decision<>

  "Create a *decision symbol*."

  ([bexpr then]
   `(decision<> ~bexpr ~then nil))

  ([bexpr then else]
   `(czlab.basal.core/object<> czlab.flux.core.Decision
                               :bexpr ~bexpr
                               :then ~then :else ~else
                               :typeid :czlab.flux.core/decision)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord WhileLoop []
  Symbol
  (step-init [_ step]
    (let [{:keys [body bexpr]} _]
      (assert (fn? bexpr))
      (po/init step
               {:bexpr bexpr
                :body (wrapc body step)})))
  (step-reify [me nx]
    (->> {:action (fn [cur job]
                    (let [{:keys [bexpr body]}
                          (po/get-conf cur)]
                      (if-not (bexpr job)
                        (do (rinit! cur) next)
                        (c/do#nil (p/run (runner job) body)))))}
         (proto-step<> me nx) (step-init me)))
  po/Typeable
  (typeid [_] (:typeid _))
  po/Nameable
  (name [me] (name (po/typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro while<>

  "Create a *while-loop symbol*."

  [bexpr body]
  `(czlab.basal.core/object<> WhileLoop
                              :bexpr ~bexpr
                              :body ~body :typeid :czlab.flux.core/while))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord SplitJoin []
  Symbol
  (step-init [me step]
    (let [{:keys [impl wait-secs forks expiry]} me]
      (po/init step
               (->> (if-not (is-null-join? me)
                      {:expiry expiry :impl impl
                       :error nil :wait wait-secs :counter (AtomicInteger. 0)})
                    (merge {:forks (map #(wrap-symb?? %) forks)})))))
  (step-reify [me nx]
    ;-we need a timer so that we don't wait forever
    ;in case some subtasks don't return.
    ;-this step is re-entrant.
    ;-we start off by forking off the set of subtasks,
    ;then wait for them to return.
    ;if time out occurs, this step
    ;is flagged as in error and may proceed differently
    ;depending on the error handling logic.
    ;-each subtask returning will up' the counter,
    ;and-join: wait for all to return,
    ;or-join: only one returning will proceed to next.
    (->>
      (if (is-null-join? me)
        {:action (fn [cur job]
                   (fanout job
                           (terminal-step<> job)
                           (po/get-conf cur :forks))
                   (po/get-conf cur :next))}
        {:timer join-timer
         :action (fn [cur job]
                   (let [{:keys [error wait
                                 impl forks]} (po/get-conf cur)]
                     (cond (some? error) (c/do#nil (l/debug "too late."))
                           (number? forks) (impl cur)
                           (empty? forks) (po/get-conf cur :next)
                           (not-empty forks)
                           (c/do#nil (fanout job cur forks)
                                     (po/set-conf cur :forks (n# forks))
                                     (po/set-conf cur :alarm (sa! cur job wait))))))})
         (proto-step<> me nx) (step-init me)))
  po/Typeable
  (typeid [_] (:typeid _))
  po/Nameable
  (name [me] (name (po/typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- nulljoin

  "Create a do-nothing *join task*."

  [branches]

  (c/object<> SplitJoin :typeid ::null-join :forks branches))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- andjoin

  "Create a *join(and) task*."

  [branches waitSecs expiry]

  (c/object<> SplitJoin
              :typeid ::and-join
              :forks branches
              :expiry expiry
              :wait-secs waitSecs
              :impl #(let [{:keys [counter forks alarm next]}
                           (po/get-conf %)
                           n (-> ^AtomicInteger counter .incrementAndGet)]
                       (l/debug "andjoin: sub-task[%d] returned." n)
                       (when (== n forks)
                         (l/debug "andjoin: sub-tasks completed.")
                         (some-> ^TimerTask alarm .cancel)
                         (po/set-conf % :alarm nil) (rinit! %) next))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- orjoin

  "Create a *or join symbol*."

  [branches waitSecs expiry]

  (c/object<> SplitJoin
              :wait-secs waitSecs
              :typeid ::or-join
              :forks branches
              :expiry expiry
              :impl #(let [{:keys [counter next alarm]} (po/get-conf %)
                           n (-> ^AtomicInteger counter .incrementAndGet)]
                       (l/debug "orjoin: sub-task[%d] returned." n)
                       (some-> ^TimerTask alarm .cancel)
                       (po/set-conf % :alarm nil)
                       (when (== n 1) (rinit! %) next))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord Split []
  Symbol
  (step-init [_ step]
    (let [{:keys [expiry forks options]} _
          {:keys [type wait-secs]} options]
      (po/init step
               {:expiry (some-> expiry wrap-symb??)
                :wait (c/num?? wait-secs 0)
                :join-style type
                :forks (map #(wrap-symb?? %) forks)})))
  (step-reify [me nx]
    (->>
      {:action (fn [cur _]
                 (let [{:keys [join-style wait
                               expiry next forks]}
                       (po/get-conf cur)]
                   (wrapc (cond (= :and join-style)
                                (andjoin forks wait expiry)
                                (= :or join-style)
                                (orjoin forks wait expiry)
                                :else
                                (nulljoin forks)) next)))}
      (proto-step<> me nx) (step-init me)))
  po/Typeable
  (typeid [_] (:typeid _))
  po/Nameable
  (name [me] (name (po/typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro split-join<>

  ""
  [bindings & forks]

  (let [{:keys [type wait-secs] :as M}
        (apply hash-map bindings)
        [a b] (take-last 2 forks)
        [exp args]
        (if (= a :expiry)
          [b (drop-last 2 forks)] [nil forks])]
    (assert (contains?  #{:and :or} type))
    (if wait-secs
      (assert (number? wait-secs)))
    (if (= a :expiry)
      (assert (some? exp)))
    (assert (not-empty args))
    `(czlab.basal.core/object<> czlab.flux.core.Split
                                :expiry ~exp :forks [~@args]
                                :options ~M :typeid :czlab.flux.core/split)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro split<>

  ""
  [f1 & more]

  (let [forks (cons f1 more)]
    `(czlab.basal.core/object<> czlab.flux.core.Split
                                :forks [~@forks] :typeid :czlab.flux.core/split)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord Group []
  Symbol
  (step-init [me step]
    (po/init step
             {:alist (map #(wrap-symb?? %) (:symbols me))}))
  (step-reify [me nx]
    ;iterate through the group, treating it like a
    ;queue, poping off one at a time. Each symbol
    ;pop'ed off will be run and will return back here
    ;for the next iteration to occur.  We can do this
    ;by passing this group-step as the next step to
    ;be performed after the item is done.
    (->>
      {:action (fn [cur job]
                 (let [[a & more] (po/get-conf cur :alist)]
                   (if-some [s (some-> a (wrapc cur))]
                     (do (po/set-conf cur :alist more) s)
                     (do (rinit! cur) (po/get-conf cur :next)))))}
      (proto-step<> me nx) (step-init me)))
  po/Typeable
  (typeid [_] (:typeid _))
  po/Nameable
  (name [me] (name (po/typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro group<>
  "" [a & more]
  `(czlab.basal.core/object<> czlab.flux.core.Group
                              :symbols [~a ~@more] :typeid :czlab.flux.core/group))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- range-expr
  [lower upper]
  (let [loopy (atom lower)]
    (c/fn_1 (let [job ____1 v @loopy]
              (if (< v upper)
                (swap! loopy
                       #(do (po/setv job
                                     :$range-index v) (+ 1 %))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord ForLoop []
  Symbol
  (step-init [me step]
    (let [job (g-job step)
          {:keys [lower upper body]} me
          low (cond (number? lower) lower
                    (fn? lower) (lower job))
          high (cond (number? upper) upper
                     (fn? upper) (upper job))]
      (assert (and (number? low)
                   (number? high)
                   (<= low high))
              "for<>: Bad lower/upper bound.")
      (po/init step
               {:body (wrapc body step)
                :bexpr (range-expr low high)})))
  (step-reify [me nx]
    (->> {:action (fn [cur job]
                    (let [{:keys [bexpr body]}
                          (po/get-conf cur)]
                      (if-not (bexpr job)
                        (do (rinit! cur) next)
                        (c/do#nil (p/run (runner job) body)))))}
         (proto-step<> me nx) (step-init me)))
  po/Typeable
  (typeid [_] (:typeid _))
  po/Nameable
  (name [me] (name (po/typeid me))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmacro for<>
  "" [lower upper body]
  `(czlab.basal.core/object<> czlab.flux.core.ForLoop
                              :lower ~lower :upper ~upper
                              :body ~body :typeid :czlab.flux.core/for))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn job<>
  "Create a job context."
  ([_sch initObj] (job<> _sch initObj nil))
  ([_sch] (job<> _sch nil nil))
  ([_sch initObj originObj]
   (let [impl (atom {:last-result nil})
         data (atom (or initObj {}))
         _id (str "job#" (u/seqint2))]
     (reify
       Job
       (runner [_] _sch)
       (wkflow [_] (:wflow @impl))
       po/Configurable
       (set-conf [_ k v] _)
       (set-conf [me _] me)
       (get-conf [_] nil)
       (get-conf [_ k] (get @impl k))
       po/Hierarchical
       ;where this job came from?
       (parent [_] originObj)
       po/Idable
       (id [_] _id)
       ;for app data
       po/Settable
       (unsetv [_ k]
         (swap! data dissoc k))
       (setv [_ k v]
         (swap! data assoc k v))
       po/Gettable
       (getv [_ k] (get @data k))
       (has? [_ k] (contains? @data k))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- wsexec
  "Apply workflow to this job." [ws job]

  (po/set-conf job :wflow ws)
  (p/run (runner job)
         (wrapc (:head ws) (terminal-step<> job))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord WorkFlowExObj []
  po/Catchable
  (catche [me e] ((:efn me) e))
  WorkFlow
  (exec [me job] (wsexec me job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord WorkFlowObj []
  WorkFlow
  (exec [me job] (wsexec me job)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn workflow<>

  "Creare workflow from a list of symbols."

  [symbols]
  {:pre [(sequential? symbols)]}

  ;;first we look for error handling which,
  ;;if defined, must be at the end of the args.
  (let [[a b] (take-last 2 symbols)
        [err syms]
        (if (and (fn? b)
                 (= :catch a))
          [b (drop-last 2 symbols)] [nil symbols])
        head (c/object<> Group :symbols syms :typeid :group)]
    (if-not (fn? err)
      (c/object<> WorkFlowObj :head head)
      (c/object<> WorkFlowExObj :head head :efn err))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn workflow*

  "Create a work flow with the follwing syntax:
  (workflow<> symbol [symbol...] [:catch func])"

  [symbol0 & args]
  {:pre [(some? symbol0)]}

  (workflow<> (cons symbol0 args)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


