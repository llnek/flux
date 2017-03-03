# flux

[![Build Status](https://travis-ci.org/llnek/flux.svg?branch=master)](https://travis-ci.org/llnek/flux)


## Installation

Add the following dependency to your `project.clj` file:

    [io.czlab/flux "1.0.0"]

## Documentation

* [API Docs](https://llnek.github.io/flux/)

## Usage

```clojure
(ns demo.app
  :require [czlab.flux.wflow.core :as wf])

  ;global scheduler
  (def cpu (doto (scheduler<> "test") (.activate nil)))

  ;fork two tasks, wait until they are done, then continue
  (let
    [ws
     (workStream<>
       (fork<> {:join :and}
               (script<> #(do->nil
                             (pause 1000)
                             (.setv %2 :x 5)))
               (script<> #(do->nil
                             (pause 1500)
                             (.setv %2 :y 7))))
       (script<> 
         #(do->nil
             (assert (= 12 
                        (+ (.getv %2 :x)
                           (.getv %2 :y)))))))
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




