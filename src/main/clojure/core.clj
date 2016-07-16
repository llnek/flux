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

(ns ^{}

  czlab.wflow)

;;private long _pid = CU.nextSeqLong();
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stepRunAfter

  ""
  [^Step this]

  (let [cpu (.core (.container (.job this)))
        np (.next this)]
    (cond
      (inst? Delay (.isa this))
      (.postpone cpu np (* 1000 (:delay (.attrs this))))

      (inst? Nihil (.isa this))
      nil

      :else
      (.run cpu this))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- stepRun

  ""
  [^Step this]

          ;;^Step rc nil
    ;;Activity err= null,
    ;;ServiceHandler svc = null;
  (with-local-vars [err nil rc nil svc nil]
    (let [j (.job this)
          par (.container j)
          d (.isa this)]
      (.dequeue (.core par) this)
      (try
        (log/debug "%s :handle()" (.getName ^Named d))
        (var-set rc (.handle this j))
      (catch Throwable e#
        (when-some [^ServiceHandler
                    svc (cast? ServiceHandler par)]
          (let [ret (->> (StepError. this "" e#)
                         (.handleError svc))]
            (var-set err (cast? Activity ret))))
        (when (nil? @err)
          (var-set err (nihil)))
        (var-set rc (.reify ^Activity
                            @err
                            (.create (nihil) j))))))
    (if (nil? @rc)
      (log/debug "step: rc==null => skip")
      (stepRunAfter @rc))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmulti stepize

  ""
  {:private true :tag Step}

  (fn [a b & xs] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Nihil
  [^Activity actDef ^Step curStep & [job]]

  (let [^Job theJob (or (some-> curStep
                                (.job curStep)) job)]
    (reify
      Initable
      (init [_ obj])
      Step
      (handle [this j] ^Step this)
      (id [_] (CU/nextSeqLong))
      (run [_] (stepRun this))
      (setNext! [_ n] n)
      (isa [_] actDef)
      (job [_] theJob)
      (attrs [_] nil)
      (next [this] ^Step this))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn nihil

  ""
  ^Nihil
  []

  (reify

    Initable

    (init [_ obj] )

    Nihil
    (create [this c] (stepize this c))
    (create [this j] (stepize this nil j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize
  Delay
  [^Activity actDef ^Step curStep & [job]]
  (let [^Job theJob (or (some-> curStep
                                (.job curStep) job))
        info (atom {})]
    (reify

      Initiable

      (init [_ m] (swap! info m))

      Step

      (handle [_ j] ^Step this)
      (attrs [_] @info)
      (job [_] theJob)
      (isa [_] actDef)
      (next [_] curStep))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn postpone

  ""
  ^Delay
  [delaySecs]

  (reify

    Initable
    (init [this obj]
      (->> {:delay delaySecs}
           (.init ^Initable obj)))

    Delay
    (create [this c] (stepize this c))
    (create [this j] (stepize this j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  PTask
  [^Activity actDef ^Step curStep & [job]]

  (let [^Job theJob (or (some-> curStep
                                (.job curStep)) job)
        info (atom {})]
    (reify

      Initable
      (init [_ m] (swap! info m))

      Step

      (attrs [_] @info)
      (isa [_] actDef)
      (job [_] theJob)
      (next [_] curStep)
      (handle [_ j]
        (let [a ((:work @info) this j)
              rc (.next this)]
          (if
            (inst? Activity a)
            (stepize a rc)
            rc))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn ptask

  ""
  ^PTask
  [workFunc]
  {:pre [(fn? workFunc)]}

  (reify

    Initable

    (init [_ obj]
      (->> {:work workFunc}
           (.init ^Initable obj)))

    PTask

    (create [this c] (stepize this c))
    (create [this j] (stepize this j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Switch
  [^Activity actDef ^Step curStep & [job]]

  (let [^Job theJob (or (some-> curStep
                                (.job curStep)) job)
        info (atom {})]
    (reify

      Initiable

      (init [_ m] (swap! info m))

      Step

      (next [_] curStep)
      (attrs [_] @info)
      (isa [_] actDef)
      (job [_] theJob)
      (handle [_ j]
        (let [^ChoiceExpr e (:expr @info)
              cs (:choices @info)
              dft (:dft @info)
              m (.choice e j)
              a (if (some? m)
                  (some #(if (= m (first %1))
                           (last %1) nil)
                        cs)
                  nil)]
          (if (inst? Activity a) a dft))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn switch?

  ""
  ^Activity
  [^ChoiceExpr cexpr ^Activity dft & choices]

  (let [cpairs (partition 2 choices)]
    (reify

      Initable

      (init [_ obj]
        (->> {:expr cexpr
              :dft dft
              :choices cpairs}
             (.init ^Step obj)))

      Switch

      (create [this c] (stepize this c))
      (create [this j] (stepize this j)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  NulJoin
  [^Activity actDef ^Step curStep & [job]]

  (let [^Job theJob (or (some-> curStep
                                (.job curStep)) job)]
    (reify

      Initiable
      (init [_ m] )

      Step

      (next [_] curStep)
      (attrs [_] @info)
      (isa [_] actDef)
      (job [_] theJob)
      (handle [_ j] nil))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn nuljoin

  ""
  ^NulJoin
  []

  (reify

    Initiable

    (init [_ s] )

    NulJoin

    (create [this c] (stepize this c))
    (create [this c] (stepize this j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  AndJoin
  [^Activity actDef ^Step curStep & [job]]

  (let [^Job theJob
        (or (some-> curStep
                    (.job curStep)) job)
        info (atom {})]
    (reify

      Initable

      (init [_ m] (swap! info m))

      Step

      (next [_] curStep)
      (attrs [_] @info)
      (isa [_] actDef)
      (job [_] theJob)
      (handle [_ j]
        (let [nv (.incrementAndGet (:cnt @info))]
          (if (== nv (:branches @info))
            (or (:body @info) (.next this))
            nil))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn andjoin

  ""
  ^AndJoin
  [^long branches & [^Activity body]]

  (reify

    Initiable

    (init [_ s]
      (let [x (.next ^Step s)
            b (if (some? body) (.reify body x) nil)]
        (->> {:cnt (AtomicInteger. 0)
              :branches branches
              :body b}
           (.init ^Step s))))

    AndJoin

    (create [this c] (stepize this c))
    (create [this j] (stepize this j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  OrJoin
  [^Activity actDef ^Step curStep & [job]]

  (let [^Job theObj
        (or (some-> curStep
                    (.job curStep)) job)
        info (atom {})]
    (reify

      Initable

      (init [_ m] (swap! info m))

      Step

      (next [_] curStep)
      (attrs [_] @info)
      (isa [_] actDef)
      (job [_] theJob)
      (handle [this j]
        (let [nv (.incrementAndGet (:cnt @info))
              rc this
              nx (.next ^Step this)]
          (cond
            (== 0 (:branches @info))
            (do
              (.init ^Initable actDef this)
              (or (:body @info) nx))

            (== 1 nv) ;; only need one
            (do
              (if (== 1 (:branches @info)) (.init actDef this))
              (or (:body @info) nx))

            (>= nv (:branches @info))
            (do->nil
              (.init actDef this))

            :else this))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn orjoin

  ""
  ^OrJoin
  [^long branches & [^Activity body]]

  (reify

    Initable

    (init [_ s]
      (let [x (.next ^Step s)
            b (if (some? body) (.reify body x) nil)]
        (->> {:cnt (AtomicInteger. 0)
              :branches branches
              :body b}
             (.init ^Initable s))))

    OrJoin

    (create [this c] (stepize this c))
    (create [this j] (stepize this j))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  If
  [^Activity actDef ^Step curStep & [job]]

  (let [^Job theJob
        (or (some-> curStep
                    (.job curStep)) job)
        info (atom {})]
    (reify

      Initable
      (init [_ m] (reset! info m))

      Step

      (next [_] curStep)
      (attrs [_] @info)
      (isa [_] actDef)
      (job [_] theJob)
      (handle [_ j]
        (let [b (.ptest (:test @info) j)
              rc (if b (:then @info) (:else @info))]
          (.init actDef this)
          rc)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn ternary

  ""
  ^If
  [^BoolExpr bexpr ^Activity then & [^Activity else]]

  (reify

    Initable

    (init [this s]
      (let [nx (.next ^Step s)
            e (if (some? else) (.reify else nx) nil)
            t (.reify then nx)]
        (->> {:test bexpr
              :then t
              :else e}
             (.init ^Initable s))))

    If

    (create [this c] (stepize this c))
    (create [this c] (stepize this j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  While
  [^Activity actDef ^Step curStep & [job]]

  (let [^Job theJob
        (or (some-> curStep
                    (.job curStep)) job)
        info (atom {})]
    (reify

      Initable

      (init [_ m] (reset! info m))

      Step

      (next [_] curStep)
      (attrs [_] @info)
      (isa [_] actDef)
      (job [_] theJob)
      (handle [this j]
        (let [rc (object-array [this])
              nx (.next ^Step this)
              b (.ptest (:bexpr @info) j)]
          (if-not b
            (do
              (.init actDef this)
              (aset rc 0 nx))
            ;;normally n is null, but if it is not
            ;;switch the body to it.
            (when-some [^Step n (.handle (:body @info) j)]
              (cond
                (inst? Delay (.isa n))
                (do
                  (.setNext n rc)
                  (aset rc 0 n))

                (not (= n this))
                ;;(swap! info assoc :body n)
                (println "dont handle now"))))
          rc)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn while*

  ""
  ^While
  [^BoolExpr bexpr ^Activity body]
  {:pre [(some? body)]}

  (reify

    Initable

    (init [this s]
      (->> {:test bexpr
            :body (.reify body ^Step s)}
           (.init ^Initiable s)))

    While

    (create [this c] (stepize this c))
    (create [this c] (stepize this j))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Split
  [^Activity actDef ^Step curStep & [job]]

  (let [^Job theJob
        (or (some-> curStep
                    (.job curStep)) job)
        info (atom {})]
    (reify

      Initable

      (init [_ m] (reset! info m))

      Step

      (next [_] curStep)
      (attrs [_] @info)
      (isa [_] actDef)
      (job [_] theJob)
      (handle [this j]
        (let [^Innards cs (:forks @info)
              cpu (-> (.container ^Job j)
                      (.core))
              t (:joinStyle @info)
              rc null]
          (while (not (.isEmpty cs))
            (.run cpu (.next cs)))
          (.init actDef this)
          (if (or (= :and t) (= :or t))
            (.next ^Step this)
            nil))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn fork

  ""
  ^Split
  [merger ^Activity body & branches]

  (let [cnt (count branches)
        join (cond
               (= :and merger) (andjoin cnt body)
               (= :or merger) (orjoin cnt body)
               :else (nuljoin body))]
    (reify

      Initable

      (init [this p]
        (let [nx (.next ^Step p)
              s (.reify join nx)]
          (->> {:forks (Innards. s branches)
                :joinStyle merger}
               (.init ^Initable s))))

      Split

      (create [this c] (stepize this c))
      (create [this j] (stepize this j)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod stepize

  Group
  [^Activity actDef ^Step curStep & [job]]

  (let [^Job theJob
        (or (some-> curStep
                    (.job curStep)) job)
        info (atom {})]
    (reify

      Initable

      (init [_ m] (reset! info m))

      Step

      (next [_] curStep)
      (job [_] theJob)
      (attrs [_] @info)
      (isa [_] actDef)
      (handle [this j]
        (let [^Innards cs (:list @info)
              nx (.next ^Step this)
              rc (object-arry [nil])]
          (if-not (.isEmpty cs)
            (let [^Step n (.next cs)
                  d (.isa n)]
              (aset rc 0 (.handle n ^Job j)))
            (do
              (.init actDef this)
              (aset rc 0 nx)))
          (aget rc 0))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn group

  ""
  ^Group
  [^Activity a & xs]

  (let [cs (concat [a] xs)]
    (reify

      Initable

      (init [_ p]
        (->> {:list (Innards. p cs)}
             (.init ^Step p)))

      Group

      (create [this c] (stepize this c))
      (create [this j] (stepize this j)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmacro chain->

  ""
  ^Group
  [a & xs]

  `(group ~a ~@xs))

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









