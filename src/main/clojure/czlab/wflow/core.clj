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

  (:require
    [czlab.xlib.str :refer [stror hgl?]]
    [czlab.xlib.logging :as log]
    [czlab.xlib.core
     :refer [do->true
             doto->>
             do->nil
             spos?
             inst?
             cast?]]
    [clojure.java.io :as io]
    [clojure.string :as cs])

  (:import
    [java.util.concurrent.atomic AtomicInteger]
    [java.util TimerTask]
    [czlab.server ServerLike]
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
     Catchable
     Initable
     Nameable
     CU
     Schedulable]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti stepize

  ""
  {:private true
   :tag Step}

  (fn [a b & xs] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn nihil

  "Create a special *terminator task*"
  ^Nihil
  []

  (reify

    Initable

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
(defn- reinit!

  ""
  [^Initable a ^Step s]

  (.init a  s))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rerun!

  ""
  [^Step s]

  (some-> s
          (.job )
          (.container )
          (.core )
          (.reschedule s)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stepRunAfter

  ""
  [^Step this]

  (if (some? this)
    (let [cpu (.core (.container (.job this)))]
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
(defmacro nihilStep

  ""
  {:private true
   :tag Step}
  [^Job job]

  `(.createEx (nihil) ~job))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stepRun

  ""
  [^Step this]

  (log/debug "%s :handle()"
             (.name ^Nameable (.proto this)))
  (-> (.core (.container (.job this)))
      (.dequeue this))
  (let
    [job (.job this)
     ws (.wflow job)
     rc
     (try
       (.handle this job)
       (catch Throwable e#
         (when-some
           [a
            (if (inst? Catchable ws)
              (->> (StepError. this e#)
                   (.catche ^Catchable ws)
                   (cast? TaskDef))
              (do->nil
                (log/error "" e#)))]
           (->> (nihilStep job)
                (.create ^TaskDef a )))))]
    (stepRunAfter rc)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- onInterrupt

  ""
  [^Step this ^Job job waitSecs]

  (let
    [err (format "*interruption* %s : %d msecs"
                 "timer expired"
                 (* 1000 waitSecs))
     ws (.wflow  job)
     rc
     (when-some
       [a
        (if (inst? Catchable ws)
          (->> (StepError. this err)
               (.catche ^Catchable ws )
               (cast? TaskDef))
          (do->nil (log/error err)))]
       (->> (nihilStep job)
            (.create ^TaskDef a )))]
    (stepRunAfter rc)))

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
      (handle [this j] nil)
      (interrupt [_ _])
      (setNext [_ n] )
      (job [this] _job)
      (proto [_] actDef)
      (attrs [_] nil)
      (id [_] pid)
      (next [this] nil))))

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

      (init [_ m]
        (swap! info assoc :vars m))

      Step

      (job [this] (.job (.next this)))
      (setNext [_ nx]
        (assert (some? nx))
        (swap! info assoc :next nx))
      (attrs [_] (:vars @info))
      (next [_] (:next @info))
      (id [_] pid)
      (proto [_] actDef)
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (interrupt [_ _] )
      (handle [this j]
        (let
          [cpu (.core (.container ^Job j))
           nx (.next this)]
          (->> (get-in @info [:vars :delay])
               (* 1000 )
               (.postpone cpu nx ))
          (reinit! actDef this)
          nil)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn postpone

  "Create a Delay Task"
  ^Delay
  [delaySecs]
  {:pre [(some? delaySecs)
         (not (neg? delaySecs))]}

  (reify

    Initable

    (init [_ step]
      (->> {:delay delaySecs}
           (.init ^Initable step)))

    Delay

    (name [_] "delay")

    (create [this nx]
      (doto->> (stepize this nx)
               (.init this )))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Script
  [^TaskDef actDef ^Step nxtStep & xs]
  {:pre [(some? nxtStep)]}

  (let [info (atom {:next nxtStep
                    :vars {}})
        pid (CU/nextSeqLong)]
    (reify

      Initable

      (init [_ m]
        (swap! info assoc :vars m))

      Step

      (job [this] (.job (.next this)))
      (setNext [_ nx]
        (assert (some? nx))
        (swap! info assoc :next nx))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (attrs [_] (:vars @info))
      (id [_] pid)
      (proto [_] actDef)
      (next [_] (:next @info))
      (interrupt [_ _] )
      (handle [this j]
        (let
          [a (-> (get-in @info [:vars :work])
                 (apply this j []))
           nx (.next this)]
          (reinit! actDef this)
          (if
            (inst? TaskDef a)
            (.create ^TaskDef a nx)
            nx))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn script

  "Create a *scriptable task*"
  ^Script
  [workFunc & [nm]]
  {:pre [(fn? workFunc)]}

  (reify

    Initable

    (init [_ step]
      (->> {:work workFunc}
           (.init ^Initable step)))

    Script

    (name [_] (stror nm "script"))

    (create [this nx]
      (doto->> (stepize this nx)
               (.init this)))))

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

      (init [_ m]
        (swap! info assoc :vars m))

      Step

      (job [this] (.job (.next this)))
      (setNext [_ nx]
        (assert (some? nx))
        (swap! info assoc :next nx))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (next [_] (:next @info))
      (attrs [_] (:vars @info))
      (id [_] pid)
      (proto [_] actDef)
      (interrupt [_ _] )
      (handle [this job]
        (let
          [cs (get-in @info [:vars :choices])
           dft (get-in @info [:vars :dft])
           e (get-in @info [:vars :cexpr])
           m (.choose ^ChoiceExpr e ^Job job)
           a (if (some? m)
               (some #(if
                        (= m (first %1))
                        (last %1) nil) cs) nil)]
          (reinit! actDef this)
          (or a dft))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn choice

  "Create a Switch Task"
  ^Switch
  [^ChoiceExpr cexpr ^TaskDef dft & choices]

  (reify

    Initable

    (init [_ step]
      (let
        [nx (.next ^Step step)
         cs
         (->>
           (persistent!
             (reduce
               #(-> (conj! %1 (first %2))
                    (conj! (.create ^TaskDef
                                    (last %2) nx)))
               (transient [])
               (partition 2 choices)))
           (partition 2))]
        (->> {:dft (some-> dft (.create nx))
              :cexpr cexpr
              :choices cs}
             (.init ^Initable step))))

    Switch

    (name [_] "switch")

    (create [this nx]
      (doto->> (stepize this nx)
               (.init this)))))

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
      (setNext [_ nx]
        (assert (some? nx))
        (swap! info assoc :next nx))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (attrs [_] (:vars @info))
      (next [_] (:next @info))
      (id [_] pid)
      (proto [_] actDef)
      (interrupt [_ _] )
      (handle [this job]
        (let
          [cpu (.core (.container ^Job job))
           nx (nihilStep job)
           bs (get-in @info [:vars :forks])]
          (doseq [^TaskDef t (seq bs)]
            (.run cpu
                  (.create t nx)))
          (.next this))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- nuljoin

  "Create a do-nothing Join Task"
  ^NulJoin
  [branches]

  (reify

    Initable

    (init [_ step]
      (->> {:forks branches}
           (.init ^Initable step)))

    NulJoin

    (name [_] "nuljoin")

    (create [this nx]
      (doto->> (stepize this nx)
               (.init this)))))

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

      (init [_ m]
        (swap! info assoc :vars m))

      Step

      (job [this] (.job (.next this)))
      (setNext [_ nx]
        (assert (some? nx))
        (swap! info assoc :next nx))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (attrs [_] (:vars @info))
      (next [_] (:next @info))
      (proto [_] actDef)
      (id [_] pid)
      (interrupt [this j]
        (let [w (get-in @info [:vars :wait])]
          (log/warn "and-join time out")
          (->>
            (merge
              (:vars @info)
              {:error true})
            (swap! info assoc :vars))
          (onInterrupt this j w)))
      (handle [this job]
        (let
          [cpu (.core (.container ^Job job))
           b (get-in @info [:vars :forks])
           z (get-in @info [:vars :alarm])
           e (get-in @info [:vars :error])
           w (get-in @info [:vars :wait])
           ^AtomicInteger
           c (get-in @info [:vars :cnt])]
          (cond
            (true? e)
            (do->nil
              (log/debug "too late"))

            (number? b)
            (if (== b (.incrementAndGet c))
              (do
                (when (some? z)
                  (.cancel ^TimerTask z))
                (reinit! actDef this)
                (.next this))
              nil)

            :else
            (if-not (empty? b)
              (do->nil
                (doseq [^TaskDef t b]
                  (->> (.create t this)
                       (.run cpu)))
                (->>
                  (merge
                    (:vars @info)
                    {:alarm
                     (when (spos? w)
                       (.alarm cpu
                               this
                               job
                               (* 1000 w)))
                     :forks
                     (count b)})
                  (swap! info assoc :vars)))
              (.next this))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- andjoin

  "Create a *join(and) task*"
  ^AndJoin
  [branches waitSecs]

  (reify

    Initable

    (init [_ step]
      (->> {:cnt (AtomicInteger. 0)
            :wait waitSecs
            :forks branches}
           (.init ^Initable step)))

    AndJoin

    (name [_] "andjoin")

    (create [this nx]
      (doto->> (stepize this nx)
               (.init this)))))

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

      (init [_ m]
        (swap! info assoc :vars m))

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
      (interrupt [this j]
        (let [w (get-in @info [:vars :wait])]
          (log/debug "or-join time out")
          (->>
            (merge
              (:vars @info)
              {:error true})
            (swap! info assoc :vars))
          (onInterrupt this j w)))
      (handle [this j]
        (let
          [b (get-in @info [:vars :forks])
           z (get-in @info [:vars :alarm])
           e (get-in @info [:vars :error])
           w (get-in @info [:vars :wait])
           cpu (.core (.container ^Job j))
           ^AtomicInteger
           c (get-in @info [:vars :cnt])]
          (cond
            (true? e)
            nil
            (number? b)
            (let [nv (.incrementAndGet c)
                  nx (.next this)]
              (when (some? z)
                (.cancel ^TimerTask z))
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

                :else nil))
            :else
            (if-not (empty? b)
              (do->nil
                (doseq [t (seq b)]
                  (->> (.create ^TaskDef t this)
                       (.run cpu)))
                (->>
                  (merge
                    (:vars @info)
                    {:alarm (when (spos? w)
                              (.alarm cpu this j (* 1000 w)))
                     :forks (count b)})
                  (swap! info assoc :vars)))
              (.next this))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- orjoin

  "Create a Or Join Task"
  ^OrJoin
  [branches waitSecs]

  (reify

    Initable

    (init [_ s]
      (->> {:cnt (AtomicInteger. 0)
            :wait waitSecs
            :forks branches}
           (.init ^Initable s)))

    OrJoin

    (name [_] "orjoin")

    (create [this c]
      (doto->> (stepize this c)
               (.init this)))))

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

      (init [_ m]
        (swap! info assoc :vars m))

      Step

      (job [this] (.job (.next this)))
      (setNext [_ n]
        (assert (some? n))
        (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (id [_] pid)
      (next [_] (:next @info))
      (attrs [_] (:vars @info))
      (proto [_] actDef)
      (interrupt [_ _])
      (handle [this j]
        (let
          [p (get-in @info [:vars :test])
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
      (let
        [nx (.next ^Step s)
         e (some-> else
                   (.create nx))
         t (.create then nx)]
        (->> {:test bexpr
              :then t
              :else e}
             (.init ^Initable s))))

    If

    (name [_] "if")

    (create [this c]
      (doto->> (stepize this c)
               (.init this)))))

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

      (init [_ m]
        (swap! info assoc :vars m))

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
      (interrupt [_ _])
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

    (name [_] "while")

    (create [this c]
      (doto->> (stepize this c)
               (.init this)))))

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

      (init [_ m]
        (swap! info assoc :vars m))

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
      (interrupt [_ _])
      (handle [this j]
        (let
          [t (get-in @info [:vars :joinStyle])
           ws (get-in @info [:vars :wait])
           cs (get-in @info [:vars :forks])
           nx (.next this)
           ^TaskDef jx
           (cond
             (= :and t) (andjoin cs ws)
             (= :or t) (orjoin cs ws)
             :else (nuljoin cs)) ]
          (.create jx nx))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn fork

  "Create a Split Task"
  ^Split
  [options & branches]

  {:pre [(map? options)]}

  (let [wsecs (or (:waitSecs options) 0)
        cnt (count branches)
        merger (or (:join options) :nul)]
    (log/debug "forking with [%d] branches" cnt)
    (reify

      Initable

      (init [_ p]
        (->> {:joinStyle merger
              :wait wsecs
              :forks branches}
             (.init ^Initable p)))

      Split

      (name [_] "fork")

      (create [this c]
        (doto->> (stepize this c)
                 (.init this))))))

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

      (init [_ m]
        (swap! info assoc :vars m))

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
      (interrupt [_ _])
      (handle [this j]
        (let
          [cs (get-in @info [:vars :list])
           nx (.next this)]
          (if-not (empty? @cs)
            (let [a (.create ^TaskDef (first @cs) ^Step this)
                  r (rest @cs)
                  rc (.handle a ^Job j)]
              (reset! cs r)
              rc)
            (do
              (reinit! actDef this)
              nx)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn group

  "Create a Group Task"
  ^Group
  [^TaskDef a & xs]
  {:pre [(some? a)]}

  (let []
    (reify

      Initable

      (init [_ p]
        (->> {:list (atom (concat [a] xs))}
             (.init ^Initable p)))

      Group

      (name [_] "group")

      (create [this c]
        (doto->> (stepize this c)
                 (.init this))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- rangeExpr

  ""
  ^BoolExpr
  []

  (let [_loop (atom 0)]
    (reify BoolExpr
      (ptest [_ j]
        (let [w (.getv ^Job j :lowerRange)
              u (.getv ^Job j :upperRange)
              v @_loop]
          (if (< (+ w v) u)
            (do->true
              (.setv ^Job j For/RANGE_INDEX v)
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
  [^RangeExpr rexpr ^TaskDef body]

  (reify

    Initable

    (init [_ p]
      (let [j (.job ^Step p)
            w (.lower rexpr j)
            u (.upper rexpr j)]
        (.setv j :lowerRange w)
        (.setv j :upperRange u)
        (->> {:bexpr (rangeExpr)
              :body (.create body ^Step p)}
             (.init ^Initable p))))

    For

    (name [_] "for")

    (create [this c]
      (doto->> (stepize this c)
               (.init this)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn createJob

  ""
  ^Job
  [^ServerLike server ^WorkStream ws & [^Event evt]]

  (let [jslast (keyword Job/JS_LAST)
        data (atom {})
        jid (CU/nextSeqLong)]
    (reify

      Job

      (contains [_ k]
        (when (some? k)
          (contains? @data k)))

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

      (dbgStr [_] (str @data)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn workStream->

  ""
  ^WorkStream
  [^TaskDef task0 & tasks]

  {:pre [(some? task0)]}

  (with-local-vars [opts nil]
    (let [g
          (persistent!
            (reduce
              #(do
                 (assert (or (inst? TaskDef %2)
                             (map? %2)))
                 (cond
                   (map? %2) (do (var-set opts %2) %1)
                   (inst? TaskDef %2) (conj! %1 %2)
                   :else %1))
              (transient [])
              tasks))
          err (:error @opts)]
      (if (fn? err)
        (reify
          WorkStream
          (startWith [_]
            (if-not (empty? g)
              (apply group task0 g)
              task0))
          Catchable
          (catche [_ e] (err e)))
        (reify
          WorkStream
          (startWith [_]
            (if-not (empty? g)
              (apply group task0 g)
              task0)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF





