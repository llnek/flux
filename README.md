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

### Script (User code block)

The `script` *Activity* is where the user writes code.  It is essentially a
wrapper on a function [2-arg] with arg-1 being the current `step` in the
workflow, and arg-2 being the context of the workflow - the `job`.

User can get/set values to the `job` and perform application specific
work.  In general the return value of the function should be `nil`.
However, if the logic decides that a new `Activity` is to be returned,
then the engine will `run` this new `Activity`, implying that the
user can dynamically alter the current `workstream`.


```clojure
(ns demo.app
  (:require [czlab.flux.wflow.core :as w
             czlab.basal.core :as c
             czlab.basal.scheduler :as s]))

  ;global scheduler
  (def cpu (doto (s/scheduler<> "test") (.activate nil)))

  (let
    [ws
     (w/workStream<>
       (w/group<>
         (w/script<> #(println "job data = " %2 " at step 1"))
         (w/script<> #(println "job data = " %2 ", at step 2"))))
     job (w/job<> cpu ws)]
   (.execWith ws job))

  (let
    [ws
     (w/workStream<>
       (w/group<>
         (w/script<>
           (fn [_ job]
             (println "job data = " job ", at step 1")
             (w/script<>
               (fn [_ _]
                 (c/do->nil
                   (println "i changed the flow!"))))))
         (w/script<> #(println "you won't see me"))))
     job (w/job<> cpu ws)]
   (.execWith ws job))

```

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
  (:require [czlab.flux.wflow.core :as w
             czlab.basal.core :as c
             czlab.basal.scheduler :as s]))

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
  (:require [czlab.flux.wflow.core :as w
             czlab.basal.core :as c
             czlab.basal.scheduler :as s]))

  ;global scheduler
  (def cpu (doto (s/scheduler<> "test") (.activate nil)))

  (let [ws
        (w/workStream<>
          (w/ternary<>
            (fn [job] (= "ok" (.getv job :state)))
            (w/script<> #(c/do->nil
                           (.setv %2 :a 10)))
            (w/script<> #(c/do->nil
                           (.setv %2 :a 5)))))
        job (w/job<> cpu ws)]
    (.execWith ws job))

```

### Choice (Switch)

The `choice` *Activity* is the standard switch control construct.

```clojure
(ns demo.app
  (:require [czlab.flux.wflow.core :as w
             czlab.basal.core :as c
             czlab.basal.scheduler :as s]))

  ;global scheduler
  (def cpu (doto (s/scheduler<> "test") (.activate nil)))

  (let [ws
        (w/workStream<>
          (w/choice<>
            (fn [job] (if (= "ok" (.getv job :reply)) "x" "?"))
            (w/script<> #(c/do->nil (.setv %2 :out -1)))
            "x" (w/script<> #(c/do->nil (.setv %2 :out 10)))
            "y" (w/script<> #(c/do->nil (.setv %2 :out 5)))))
        job (w/job<> cpu ws)]
    (.execWith ws job))

```

### Floop (For)

The `floop` *Activity* is the standard for loop control construct.  User
can set/configure the looping range, and access the current loop index.

```clojure
(ns demo.app
  (:import [czlab.flux.wflow RangeExpr])
  (:require [czlab.flux.wflow.core :as w
             czlab.basal.core :as c
             czlab.basal.scheduler :as s]))

  ;global scheduler
  (def cpu (doto (s/scheduler<> "test") (.activate nil)))

  (let [ws
        (w/workStream<>
          (w/floop<>
            (reify RangeExpr
              (lower [_ job] (if (= "ok" (.getv job :x)) 0 4))
              (upper [_ job] 10))
            (w/script<> #(c/do->nil
                           (->> (inc (.getv %2 :total))
                                (.setv %2 :total))))))
        job (w/job<> cpu ws)]
    (.execWith ws job))

```

### Wloop (While)

The `wloop` *Activity* is the standard while loop control construct.

```clojure
(ns demo.app
  (:require [czlab.flux.wflow.core :as w
             czlab.basal.core :as c
             czlab.basal.scheduler :as s]))

  ;global scheduler
  (def cpu (doto (s/scheduler<> "test") (.activate nil)))

  (let [ws
        (w/workStream<>
           (w/wloop<>
             (fn [job] (< (.getv job :cnt) 10))
             (w/script<> #(c/do->nil
                            (->> (inc (.getv %2 :cnt))
                                 (.setv %2 :cnt))))))
        job (w/job<> cpu ws)]
    (.setv job :cnt 0)
    (.execWith ws job))

```

### Postpone (Delay)

The `postpone` *Activity* can be used to pause a workflow, rescheduling it
after the time limit has expired.

```clojure
(ns demo.app
  (:require [czlab.flux.wflow.core :as w
             czlab.basal.core :as c
             czlab.basal.scheduler :as s]))

  ;global scheduler
  (def cpu (doto (s/scheduler<> "test") (.activate nil)))

  (let [now (System/currentTimeMillis)
        ws
        (w/workStream<>
          (w/postpone<> 5)
          (w/script<> (fn [_ _]
                        (c/do->nil
                          (println "boom! something exploded!")))))
        job (w/job<> cpu ws)]
    (.execWith ws job))

```

### Group

The `group` *Activity* is used to sequence a set of *Activities*.

```clojure
(ns demo.app
  (:require [czlab.flux.wflow.core :as w
             czlab.basal.core :as c
             czlab.basal.scheduler :as s]))

  ;global scheduler
  (def cpu (doto (s/scheduler<> "test") (.activate nil)))

  (let [ws
        (w/workStream<>
          (w/group<>
            (w/script<> (fn [_ _]
                          (c/do->nil
                            (println "this is step 1"))))
            (w/postpone<> 5)
            (w/script<> (fn [_ _]
                          (c/do->nil
                            (println "boom! something exploded after 5 secs!"))))))
        job (w/job<> cpu ws)]
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




