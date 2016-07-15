(ns )


(defmulti declActivity "" ^Activity (fn [a & xs] a))
(defmulti declStep "" ^Step (fn [a & xs] (class a)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declActivity
  Nihil
  [cz & xs]
  (reify Nihil
    (create [this c] (declStep this c))
    (init [_ s] s)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declStep
  Nihil
  [actDef & xs]
  (reify Step
    (handle [this j] this)
    (isa [_] actDef)
    (attrs [_] nil)
    (init [_ m])
    (next [this] this)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declActivity
  Delay
  [cz & [delayMillis]]
  (reify Delay
    (create [this c] (declStep this c))
    (init [this s]
      (->> {:delayMillis delayMillis}
           (.init ^Step s))
      s)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declStep
  Delay
  [actDef & [curStep]]
  (let [info (atom {})]
    (reify
      (init [_ m] (swap! info m))
      (handle [_ j] this)
      (attrs [_] @info)
      (isa [_] actDef)
      (next [_] curStep))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declActivity
  PTask
  [cz & [work]]
  (reify PTask
    (create [this c] (declStep this c))
    (init [this s]
      (->> {:work work}
           (.init ^Step s))
      s)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declStep
  PTask
  [actDef & [curStep]]
  (let [info (atom {})]
    (reify
      (init [_ m] (swap! info m))
      (attrs [_] @info)
      (isa [_] actDef)
      (next [_] curStep)
      (handle [_ j]
        (let [a ((:work @info) this j)
              rc (.next this)]
          (if
            (inst? Activity a)
            (declStep a rc)
            rc))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declActivity
  Switch
  [cz & xs]
  (let [cexpr (nth xs 0)
        dft (nth xs 1)
        choices (partition 2 (drop 2 xs))]
    (reify Switch
      (create [this c] (declStep this c))
      (init [this s]
        (->> {:expr cexpr
              :dft dft
              :choices choices}
             (.init ^Step s))
        s))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declStep
  Switch
  [actDef & [curStep]]
  (let [info (atom {})]
    (reify
      (init [_ m] (swap! info m))
      (attrs [_] @info)
      (isa [_] actDef)
      (next [_] curStep)
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
          (if (nil? a) dft a))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declActivity
  NullJoin
  [cz & xs]
  (reify NullJoin
    (create [this c] (declStep this c))
    (init [this s] s)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declStep
  NullJoin
  [actDef & [curStep]]
  (let []
    (reify
      (init [_ m] )
      (attrs [_] @info)
      (isa [_] actDef)
      (next [_] curStep)
      (handle [_ j] nil))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declActivity
  And
  [cz & [branches body]]

  (reify And
    (create [this c] (declStep this c))
    (init [this s]
      (let [x (.next s)
            b (if (some? body) (.reify body x))]
        (->> {:branches branches
              :body b}
           (.init ^Step s))
      s))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declStep
  And
  [actDef & [curStep]]
  (let [info (atom {})]
    (reify
      (init [_ m] (swap! info m))
      (attrs [_] @info)
      (isa [_] actDef)
      (next [_] curStep)
      (handle [_ j]
        (let [nv (.incrementAndGet (:cnt @info))]
          (if (== nv (:branches @info))
            (or (:body @info) (.next this));; (done)
            nil))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declActivity
  JoinOr
  [cz & [branches body]]
  (reify JoinOr
    (create [this c] (declStep this c))
    (init [this s]
      (let [x (.next this)
            b (if (some? body) (.reify body x))]
        (->> {:branches branches
              :body b}
             (.init s))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declStep
  JoinOr
  [actDef & [curStep]]
  (let [info (atom {})]
    (reify
      (init [_ m] (swap! info m))
      (attrs [_] @info)
      (isa [_] actDef)
      (next [_] curStep)
      (handle [_ j]
        (let [nv (.incrementAndGet (:cnt @info))
              rc this
              nx (.next this)]
          (cond
            (== 0 (:branches @info))
            (do
              (.init actDef this)
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
