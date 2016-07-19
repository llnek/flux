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
             doto->>
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

  "Create a special Terminator Task"
  ^Nihil
  []

  (reify

    Initable

    (init [_ m] )

    Nihil

    (create [this c] (stepize this nil (.job c)))
    (name [_] "nihil")
    (createEx [this j]
      (doto->> (stepize this nil j)
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

  ;;(log/debug "entering stepRunAfter()")
  (when (some? this)
    (let [cpu (.core (.container (.job this)))
          nx (.next this)]
      (log/debug "step-to-run-next===> %s"
                 (.name ^Nameable (.proto this)))
      (cond
        (inst? Delay (.proto this))
        (->> (:delay (.attrs this))
             (* 1000 )
             (.postpone cpu nx))

        (inst? Nihil (.proto this))
        nil

        :else
        (.run cpu this)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro nihilStep

  ""
  ^Step
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
              (->> (.catche ^Catchable ws e#)
                   (cast? TaskDef))
              nil)]
           (->> (nihilStep job)
                (.create ^TaskDef a )))))]
    (if (nil? rc)
      (log/debug "step-to-run-next ==null => skip")
      (stepRunAfter rc))))

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
  {:pre [(some? delaySecs)
         (not (neg? delaySecs))]}

  (reify

    Initable

    (init [_ s]
      (->> {:delay delaySecs}
           (.init ^Initable s)))

    Delay

    (name [_] "delay")
    (create [this c]
      (doto->> (stepize this c)
               (.init this)))))

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
(defn script

  "Create a Programmable Task"
  ^Script
  [workFunc &[nm]]
  {:pre [(fn? workFunc)]}

  (reify

    Initable

    (init [_ s]
      (->> {:work workFunc}
           (.init ^Initable s)))

    Script

    (name [_] (stror nm "script"))
    (create [this c]
      (doto->> (stepize this c)
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
              m (.choose ^ChoiceExpr e ^Job j)
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

  (reify

    Initable

    (init [_ s]
      (let
        [nx (.next ^Step s)
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
        (->> {:dft (some-> dft
                           (.create nx))
              :cexpr cexpr
              :choices cs}
             (.init ^Initable s))))

    Switch

    (name [_] "switch")
    (create [this c]
      (doto->> (stepize this c)
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
      (setNext [_ n]
        (assert (some? n))
        (swap! info assoc :next n))
      (rerun [this] (rerun! this))
      (run [this] (stepRun this))
      (attrs [_] (:vars @info))
      (next [_] (:next @info))
      (id [_] pid)
      (proto [_] actDef)
      (handle [this j]
        (let [b (get-in @info [:vars :forks])
              cpu (.core (.container ^Job j))]
          (if-not (empty? b)
            (do->nil
              (doseq [t (seq b)]
                (->> (.create ^TaskDef t this)
                     (.run cpu)))))
          (.next this))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn nuljoin

  "Create a do-nothing Join Task"
  ^NulJoin
  [branches]

  (reify

    Initable

    (init [_ s]
      (->> {:forks branches}
           (.init ^Initable s)))

    NulJoin

    (name [_] "nuljoin")
    (create [this c]
      (doto->> (stepize this c)
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
        (let [b (get-in @info [:vars :forks])
              cpu (.core (.container ^Job j))
              ^AtomicInteger
              c (get-in @info [:vars :cnt])]
          (log/debug "inside AND")
          (if (number? b)
            (let [nv (.incrementAndGet c)]
              (log/debug "WTF!!!!!!!!!!!!! %d %d" b nv)
              (if (== nv (int b))
                (do
                  (reinit! actDef this)
                  (.next this))
                nil))
            (if-not (empty? b)
              (do->nil
                (doseq [t (seq b)]
                  (->> (.create ^TaskDef t this)
                       (.run cpu)))
                (->> (assoc (:vars @info)
                            :forks
                            (count b))
                     (swap! info assoc :vars))
                (log/debug "AND ret nuill"))
              (.next this))))))))

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
            :forks branches}
           (.init ^Initable s)))

    AndJoin

    (name [_] "andjoin")
    (create [this c]
      (doto->> (stepize this c)
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
        (let [b (get-in @info [:vars :forks])
              cpu (.core (.container ^Job j))
              ^AtomicInteger
              c (get-in @info [:vars :cnt])]
          (if (number? b)
            (let [nv (.incrementAndGet c)
                  nx (.next this)]
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
            (if-not (empty? b)
              (do->nil
                (doseq [t (seq b)]
                  (->> (.create ^TaskDef t this)
                       (.run cpu)))
                (->> (assoc (:vars @info)
                            :forks
                            (count b))
                     (swap! info assoc :vars)))
              (.next this))))))))

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

      (init [_ m] (swap! info assoc :vars m))

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
              cs (get-in @info [:vars :forks])
              nx (.next this)
              ^TaskDef jx
              (cond
                (= :and t) (andjoin cs)
                (= :or t) (orjoin cs)
                :else (nuljoin cs)) ]
          (.create jx nx))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn fork

  "Create a Split Task"
  ^Split
  [merger & branches]

  (let [cnt (count branches)]
    (log/debug "forking with [%d] branches" cnt)
    (reify

      Initable

      (init [_ p]
        (->> {:joinStyle merger
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
          (log/debug "innards = %s" (some? cs))
          (log/debug "innardsize = %s" (.size cs))
          (if-not (.isEmpty cs)
            (let [n (.next cs)
                  d (.proto n)]
              (log/debug "p = %s" (.name ^Nameable (.proto n)))
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
  {:pre [(some? a)]}

  (let [cs (into [] (concat [a] xs))]
    (reify

      Initable

      (init [_ p]
        (->> {:list (Innards. p cs)}
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
                 (assert (inst? TaskDef %2))
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





