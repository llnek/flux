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
  Join
  [cz & xs]
  (reify Join
    (create [this c] (declStep this c))
    (init [this s] s)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declStep
  Join
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
  JoinAnd
  [cz & [branches body]]

  (reify JoinAnd
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
  JoinAnd
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
(defmethod declActivity
  If
  [cz & [bexpr then else]]
  (reify If
    (create [this c] (declStep this c))
    (init [this s]
      (let [nx (.next s)
            e (if (some? else) (.reify else nx) nil)
            t (.reify then nx)]
        (->> {:test bexpr
              :then t
              :else e}
             (.init s))
        s))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declStep
  If
  [actDef & [curStep]]
  (let [info (atom {})]
    (reify
      (init [_ m] (reset! info m))
      (attrs [_] @info)
      (isa [_] actDef)
      (next [_] curStep)
      (handle [_ j]
        (let [b (.ptest (:test @info) j)
              rc (if b (:then @info) (:else @info))]
          (.init actDef this)
          rc)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declActivity
  While
  [cz & [bexpr body]]
  {:pre [(some? body)]}
  (reify If
    (create [this c] (declStep this c))
    (init [this s]
      (->> {:test bexpr
            :body (.reify body s)}
           (.init this))
      s)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declStep
  While
  [actDef & [curStep]]
  (let [info (atom {})]
    (reify
      (init [_ m] (reset! info m))
      (attrs [_] @info)
      (isa [_] actDef)
      (next [_] curStep)
      (handle [_ j]
        (let [rc (object-array [this])
              nx (.next this)]
          (if-not (.ptest (:bexpr info) j)
            (do
              (.init actDef this)
              (aset rc 0 nx))
            ;;normally n is null, but if it is not
            ;;switch the body to it.
            (when-some [n (.handle (:body @info) j)]
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
(defmethod declActivity
  Split
  [cz & xs]
  (let [merger (nth xs 0)
        body (nth xs 1)
        bs (drop 2 xs)
        join (cond
               (= :and merger) (declActivity AndJoin cnt body)
               (= :or merger) (declActivity OrJoin cnt body)
               :else (declActivity NulJoin body))]
    (reify Split
      (create [this c] (declStep this c))
      (init [this p]
        (let [nx (.next p)
              s (.reify join nx)]
          (->> {:forks (Innards. s (listC))
                :joinStyle merger}
               (.init s))
          s)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declStep
  Split
  [actDef & [curStep]]
  (let [info (atom {})]
    (reify
      (init [_ m] (reset! info m))
      (attrs [_] @info)
      (isa [_] actDef)
      (next [_] curStep)
      (handle [_ j]
        (let [rc null]
          (while (not (.isEmpty (:forks @info)))
            (.run (.core this) (.next (:forks @info))))
          (.init actDef this)
          (if (or (= :and (:joinStyle @info))
                  (= :or (:joinStyle @info)))
            (.next this)
            nil))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declActivity
  Group
  [cz & xs]
  (let []
    (reify Group
      (create [this c] (declStep this c))
      (init [this p]
        (->> {:list (Innards. p (listC))}
             (.init p))
        p))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defmethod declStep
  Group
  [actDef & [curStep]]
  (let [info (atom {})]
    (reify
      (init [_ m] (reset! info m))
      (attrs [_] @info)
      (isa [_] actDef)
      (next [_] curStep)
      (handle [_ j]
        (let [nx (.next this)
              rc (object-arry [nil])]
          (if-not (.isEmpty (:list @info))
            (let [n (.next (:list @info))
                  d (.isa n)]
              (aset rc 0 (.handle n j)))
            (do
              (.init actDef this)
              (aset rc 0 nx)))
          (aget rc 0))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;


