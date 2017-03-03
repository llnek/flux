# flux

[![Build Status](https://travis-ci.org/llnek/flux.svg?branch=master)](https://travis-ci.org/llnek/flux)

`flux` is a simple workflow engine, providing a set of `Activities` for the
user to build workflows.

## Installation

Add the following dependency to your `project.clj` file:

    [io.czlab/flux "1.0.0"]

## Documentation

* [API Docs](https://llnek.github.io/flux/)

## Activities

### Split (Fork)

The `fork` *Activity*  can be used to *split* out a set of *Activities* which
are then scheduled to run asynchronously in parallel.  The workflow can
pause with an `and` *Join* until all children have completed their work
and returned, or with an `or` *Join*, in which case only one child has to
complete and return, or with an `nil` *Join*  which tells the engine to
continue without waiting.  A timeout can be set to prevent infinite wait,
allowing the user to handle such error condition.

```clojure
(ns demo.app
  :require [czlab.flux.wflow.core :as w
            czlab.basal.core :as c
            czlab.basal.scheduler :as s])

  ;global scheduler
  (def cpu (doto (s/scheduler<> "test") (.activate nil)))

  ;fork two tasks, wait until they are done, then continue
  (let
    [ws
     (w/workStream<>
       (w/fork<> {:join :and}
                 (w/script<> #(c/do->nil
                                (c/pause 1000)
                                (.setv %2 :x 5)))
                 (w/script<> #(c/do->nil
                                (c/pause 1500)
                                (.setv %2 :y 7))))
       (w/script<>
         #(c/do->nil
             (assert (= 12
                        (+ (.getv %2 :x)
                           (.getv %2 :y)))))))
     job (w/job<> cpu ws)]
   (.execWith ws job))

```

### Ternary (If)

The `ternary` *Activity* is the standard if-then-else control construct.

```clojure
(ns demo.app
  :require [czlab.flux.wflow.core :as w
            czlab.basal.core :as c
            czlab.basal.scheduler :as s])

  ;global scheduler
  (def cpu (doto (s/scheduler<> "test") (.activate nil)))

  (let [ws
        (workStream<>
          (ternary<>
            (fn [job] (= "ok" (.getv job :state)))
            (script<> #(do->nil
                         (.setv %2 :a 10)))
            (script<> #(do->nil
                         (.setv %2 :a 5)))))
        job (job<> cpu ws)]
    (.execWith ws job))

```

### Switch (Choice)

The `choice` *Activity* is the standard switch control construct.

```clojure
(ns demo.app
  :require [czlab.flux.wflow.core :as w
            czlab.basal.core :as c
            czlab.basal.scheduler :as s])

  ;global scheduler
  (def cpu (doto (s/scheduler<> "test") (.activate nil)))

  (let [ws
        (workStream<>
          (choice<>
            (fn [job] (if (= "ok" (.getv job :reply)) "x" "?"))
            (script<> #(do->nil (.setv %2 :out -1)))
            "x" (script<> #(do->nil (.setv %2 :out 10)))
            "y" (script<> #(do->nil (.setv %2 :out 5)))))
        job (job<> cpu ws)]
    (.execWith ws job))

```

### For

The `floop` *Activity* is the standard for loop control construct.  User
can set/configure the looping range, and access the current loop index.

```clojure
(ns demo.app
  :require [czlab.flux.wflow.core :as w
            czlab.basal.core :as c
            czlab.basal.scheduler :as s])

  ;global scheduler
  (def cpu (doto (s/scheduler<> "test") (.activate nil)))

  (let [ws
        (workStream<>
          (floop<>
            (reify RangeExpr
              (lower [_ job] (if (= "ok" (.getv job :x)) 0 4))
              (upper [_ job] 10))
            (script<> #(do->nil
                         (->>
                           (inc (.getv %2 :total))
                           (.setv %2 :total))))))
        job (job<> cpu ws)]
    (.execWith ws job))

```

### While

The `wloop` *Activity* is the standard while loop control construct.

```clojure
(ns demo.app
  :require [czlab.flux.wflow.core :as w
            czlab.basal.core :as c
            czlab.basal.scheduler :as s])

  ;global scheduler
  (def cpu (doto (s/scheduler<> "test") (.activate nil)))

  (let [ws
        (workStream<>
          (wloop<>
            (fn [job] (< (.getv job :cnt) 10))
            (script<> #(do->nil
                         (->>
                           (inc (.getv %2 :cnt))
                           (.setv %2 :cnt))))))
        job (job<> cpu ws)]
    (.setv job :cnt 0)
    (.execWith ws job))

```

### Postpone (Delay)

The `postpone` *Activity* can be used to pause a workflow, rescheduling it
after the time limit has expired.

```clojure
(ns demo.app
  :require [czlab.flux.wflow.core :as w
            czlab.basal.core :as c
            czlab.basal.scheduler :as s])

  ;global scheduler
  (def cpu (doto (s/scheduler<> "test") (.activate nil)))

  (let [now (System/currentTimeMillis)
        ws
        (workStream<>
          (postpone<> 5)
          (script<> #(do->nil
                       (println "boom! something exploded!"))))
        job (job<> cpu ws)]
    (.execWith ws job))

```



## Contacting me / contributions

Please use the project's [GitHub issues page] for all questions, ideas, etc. **Pull requests welcome**. See the project's [GitHub contributors page] for a list of contributors.

## License

Copyright © 2013-2017 Kenneth Leung

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

<!--- links -->
[1]: http://ant.apache.org/
<!--- links (repos) -->
[CHANGELOG]: https://github.com/llnek/flux/releases
[GitHub issues page]: https://github.com/llnek/flux/issues
[GitHub contributors page]: https://github.com/llnek/flux/graphs/contributors




