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

  czlab.wflow.core

  (:require
    [czlab.xlib.str :refer [stror hgl?]]
    [czlab.xlib.logging :as log]
    [czlab.xlib.core
     :refer [do->true
             do->nil
             inst?
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
     For
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
     WorkStream
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
(defn- reinit!

  ""
  [^Initable a ^Step s]

  (.init a  s))

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
(defn- gjob

  ""
  ^Job
  [^Step s ^Job j]

  (or (some-> s (.next ) (.job )) j))

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
        (var-set rc (.create ^TaskDef
                            @err
                            (.create ^Nihil (nihil) j))))))
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
  [^TaskDef actDef ^Step nxtStep & [^Job _job]]

  (let [pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m])

      Step

      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (handle [this j] this)
      (setNext [_ n] )
      (job [this] _job)
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

    (create [this c] (stepize this nil (.job c)))
    (getName [_] "nihil")
    (createEx [this j] (stepize this nil j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Delay
  [^TaskDef actDef ^Step nxtStep & xs]
  {:pre [(some? nxtStep)]}

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (job [this] (.job (.next this)))
      (setNext [_ n]
        (assert (some? n))
        (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (handle [this j]
        (reinit! actDef this)
        this)
      (attrs [_] (:vars @info))
      (id [_] pid)
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
    (getName [_] "delay")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  PTask
  [^TaskDef actDef ^Step nxtStep & xs]
  {:pre [(some? nxtStep)]}

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (job [this] (.job (.next this)))
      (setNext [_ n]
        (assert (some? n))
        (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (attrs [_] (:vars @info))
      (id [_] pid)
      (proto [_] actDef)
      (next [_] (:next @info))
      (handle [this j]
        (let [a (-> (get-in @info [:vars :work])
                    (apply this j []))
              nx (.next this)]
          (reinit! actDef this)
          (if
            (inst? TaskDef a)
            (.create ^TaskDef a nx)
            nx))))))

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
    (getName [_] (stror nm "ptask"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Switch
  [^TaskDef actDef ^Step nxtStep & xs]
  {:pre [(some? nxtStep)]}

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (job [this] (.job (.next this)))
      (setNext [_ n]
        (assert (some? n))
        (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (next [_] (:next @info))
      (attrs [_] (:vars @info))
      (id [_] pid)
      (proto [_] actDef)
      (handle [this j]
        (let [cs (get-in @info [:vars :choices])
              dft (get-in @info [:vars :dft])
              e (get-in @info [:vars :cexpr])
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
        (->> {:cexpr cexpr
              :dft dft
              :choices cpairs}
             (.init ^Initable s)))

      Switch

      (create [this c] (stepize this c))
      (getName [_] "switch"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  NulJoin
  [^TaskDef actDef ^Step nxtStep & xs]
  {:pre [(some? nxtStep)]}

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable
      (init [_ m] )

      Step

      (job [this] (.job (.next this)))
      (setNext [_ n]
        (assert (some? n))
        (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (attrs [_] (:vars @info))
      (next [_] (:next @info))
      (id [_] pid)
      (proto [_] actDef)
      (handle [this j] (.next this))))) ;; nil?

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
    (getName [_] "nuljoin")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  AndJoin
  [^TaskDef actDef ^Step nxtStep & xs]
  {:pre [(some? nxtStep)]}

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (job [this] (.job (.next this)))
      (setNext [_ n]
        (assert (some? n))
        (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (attrs [_] (:vars @info))
      (next [_] (:next @info))
      (proto [_] actDef)
      (id [_] pid)
      (handle [this j]
        (let [b (get-in @info [:vars :branches])
              c (get-in @info [:vars :cnt])
              nv (.incrementAndGet ^AtomicInteger c)]
          (if (== nv b)
            (do
              (reinit! actDef this)
              (.next this))
            nil))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn andjoin

  "Create a And Join Task"
  ^AndJoin
  [branches]

  (reify

    Initable

    (init [_ s]
      (->> {:cnt (AtomicInteger. 0)
            :branches branches }
         (.init ^Initable s)))

    AndJoin

    (create [this c] (stepize this c))
    (getName [_] "andjoin")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  OrJoin
  [^TaskDef actDef ^Step nxtStep & xs]
  {:pre [(some? nxtStep)]}

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (job [this] (.job (.next this)))
      (setNext [_ n]
        (assert (some? n))
        (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (attrs [_] (:vars @info))
      (next [_] (:next @info))
      (proto [_] actDef)
      (id [_] pid)
      (handle [this j]
        (let [b (get-in @info [:vars :branches])
              c (get-in @info [:vars :cnt])
              nv (.incrementAndGet ^AtomicInteger c)
              nx (.next ^Step this)]
          (cond
            (== 0 b)
            (do (reinit! actDef this) nx)

            (== 1 nv)
            (do
              (when (== 1 b)
                (reinit! actDef this)) nx)

            (>= nv b)
            (do->nil
              (reinit! actDef this))

            :else this))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn orjoin

  "Create a Or Join Task"
  ^OrJoin
  [branches]

  (reify

    Initable

    (init [_ s]
      (->> {:cnt (AtomicInteger. 0)
            :branches branches}
           (.init ^Initable s)))

    OrJoin

    (create [this c] (stepize this c))
    (getName [_] "orjoin")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  If
  [^TaskDef actDef ^Step nxtStep & xs]
  {:pre [(some? nxtStep)]}

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (job [this] (.job (.next this)))
      (setNext [_ n]
        (assert (some? n))
        (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (next [_] (:next @info))
      (attrs [_] (:vars @info))
      (proto [_] actDef)
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
            e (if (some? else) (.create else nx) nil)
            t (.create then nx)]
        (->> {:test bexpr
              :then t
              :else e}
             (.init ^Initable s))))

    If

    (create [this c] (stepize this c))
    (getName [_] "if")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stepizeLoop

  ""
  [^TaskDef actDef ^Step nxtStep & xs]
  {:pre [(some? nxtStep)]}

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (job [this] (.job (.next this)))
      (setNext [_ n]
        (assert (some? n))
        (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (next [_] (:next @info))
      (attrs [_] (:vars @info))
      (proto [_] actDef)
      (id [_] pid)
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
(defmethod stepize

  While
  [^TaskDef actDef ^Step nxtStep & xs]

  (apply stepizeLoop actDef nxtStep xs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn wloop

  "Create a While Task"
  ^While
  [^BoolExpr bexpr ^TaskDef body]

  (reify

    Initable

    (init [this s]
      (->> {:bexpr bexpr
            :body (.create body ^Step s)}
           (.init ^Initable s)))

    While

    (create [this c] (stepize this c))
    (getName [_] "while")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Split
  [^TaskDef actDef ^Step nxtStep & xs]
  {:pre [(some? nxtStep)]}

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (job [this] (.job (.next this)))
      (setNext [_ n]
        (assert (some? n))
        (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (next [_] (:next @info))
      (attrs [_] (:vars @info))
      (proto [_] actDef)
      (id [_] pid)
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
            (.next this)
            nil))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn fork

  "Create a Split Task"
  ^Split
  [merger ^TaskDef body & branches]

  (let [cnt (count branches)
        bs (into [] branches)
        ^TaskDef
        join (cond
               (= :and merger) (andjoin cnt body)
               (= :or merger) (orjoin cnt body)
               :else (nuljoin))]
    (reify

      Initable

      (init [this p]
        (let [nx (.next ^Step p)
              s (.create join nx)]
          (->> {:forks (Innards. s bs)
                :joinStyle merger}
               (.init ^Initable s))))

      Split

      (create [this c] (stepize this c))
      (getName [_] "fork"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Group
  [^TaskDef actDef ^Step nxtStep & xs]
  {:pre [(some? nxtStep)]}

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m] (swap! info assoc :vars m))

      Step

      (job [this] (.job (.next this)))
      (setNext [_ n]
        (assert (some? n))
        (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (next [_] (:next @info))
      (id [_] pid)
      (attrs [_] (:vars @info))
      (proto [_] actDef)
      (handle [this j]
        (let [^Innards
              cs (get-in @info [:vars :list])
              nx (.next this)]
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

  (let [cs (into [] (concat [a] xs))]
    (reify

      Initable

      (init [_ p]
        (->> {:list (Innards. p cs)}
             (.init ^Initable p)))

      Group

      (create [this c] (stepize this c))
      (getName [_] "group"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- floopExpr

  ""
  ^BoolExpr
  [cnt]

  (let [_loop (atom 0)]
    (reify BoolExpr
      (ptest [_ j]
        (let [v @_loop]
          (if (< v cnt)
            (do->true
              (.setv ^Job j For/FLOOP_INDEX v)
              (swap! _loop inc))
            false))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  For
  [^TaskDef actDef ^Step nxtStep & xs]

  (apply stepizeLoop actDef nxtStep xs))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn floop

  "Create a For Task"
  ^For
  [^CounterExpr cexpr ^TaskDef body]

  (reify

    Initable

    (init [_ p]
      (let [j (.job ^Step p)
            n (.gcount cexpr j)]
        (->> {:bexpr (floop n)
              :body (.create body ^Step p)}
             (.init ^Initable p))))

    For

    (create [this c] (stepize this c))
    (getName [_] "for")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn createJob

  ""
  [^ServerLike server ^WorkStream ws & [^Event evt]]

  (let [jslast (keyword Job/JS_LAST)
        data (atom {})
        jid (CU/nextSeqLong)]
    (reify

      Job

      (getv [_ k]
        (when (some? k) (get @data k)))

      (setv [_ k v]
        (when (some? k)
          (swap! data assoc k v)))

      (unsetv [_ k]
        (when (some? k)
          (swap! data dissoc k)))

      (clear [_]
        (reset! data {}))

      (container [_] server)

      (event [_] evt)

      (id [_] jid)

      (setLastResult [_ v]
        (swap! data assoc jslast v))

      (clrLastResult [_]
        (swap! data dissoc jslast))

      (getLastResult [_] (get @data jslast))

      (wflow [_] ws)

      (dbgShow [_ out] )

      (dbgStr [_] ""))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn workstream->

  ""
  [^TaskDef task0 & tasks]

  {:pre [(some? task0)]}

  (with-local-vars [opts nil]
    (let [g
          (persistent!
            (reduce
              #(cond
                 (map? %2) (var-set opts %1)
                 (fn? %2) (conj! %1 %2)
                 :else %1)
              (transient [])
              tasks))
          err (:error @opts)]
      (reify WorkStream
        (startWith [_]
          (if-not (empty? g)
            (apply group task0 g)
            task0))
        (onError [_ e]
          (when (fn? err) (err e)))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF





