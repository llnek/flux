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

(ns czlabtest.wflow.test

  (:require [czlab.wflow.core :refer :all])

  (:use [czlab.xlib.scheduler]
        [czlab.xlib.process]
        [czlab.xlib.core]
        [clojure.test])

  (:import
            [czlab.xlib CU])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(def ^:private ^Schedulable SCD (mkScheduler "test"))
(.activate SCD {})

(def ^:private ^ServerLike
  SVR (reify ServerLike (core [_] SCD)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defn- testWFlowSplit
  ""
  []
  (let [out (AtomicInteger. 0)
        testValue 10
        f
        (fork :and
              (ptask #(do (.set out 10) %1 %2 nil))
              (ptask #(do (safeWait 1000) %1 %2 nil))
              (ptask #(do (safeWait 1500) %1 %2 nil)))
        ws (workStream f)
        job (createJob SVR ws)
        end (.create (nihil) job)]
    (.run (.core SVR)
          (.create (.startWith ws) end))
    pause(5000);
    assertEquals(testValue, out.get());

    a=PTask.apply( (Step n, Job j) -> {
      out.set(10);
      //System.out.println("All Right! " + System.currentTimeMillis());
      return null;
    });
    b=PTask.apply( (Step n, Job j) -> {
      //System.out.println("Dude! " + System.currentTimeMillis());
      try { Thread.sleep(1000);  } catch (Exception e) {}
      return null;
    });
    c=PTask.apply( (Step n, Job j) -> {
      //System.out.println("Yo! " + System.currentTimeMillis());
      try { Thread.sleep(1500);  } catch (Exception e) {}
      return null;
    });
    a=Split.applyOr(a).includeMany(b,c);
    s.handle(a, null);

    pause(5000);
    assertEquals(testValue, out.get());


    a=PTask.apply( (Step n, Job j) -> {
      out.set(10);
      //System.out.println("****All Right! " + System.currentTimeMillis());
      return null;
    });
    b=PTask.apply( (Step n, Job j) -> {
      //System.out.println("Dude! " + System.currentTimeMillis());
      //try { Thread.sleep(2000);  } catch (Exception e) {}
      return null;
    });
    c=PTask.apply( (Step n, Job j) -> {
     // System.out.println("Yo! " + System.currentTimeMillis());
      //try { Thread.sleep(3000);  } catch (Exception e) {}
      return null;
    });
    a=Split.apply().includeMany(b,c).chain(a);
    s.handle(a, null);

    pause(5000);
    assertEquals(testValue, out.get());
  }

  @Test
  public void testWFlowIf() throws Exception {
    WFlowServer s= new WFlowServer(ServerCore.apply());
    s.start();
    AtomicInteger out= new AtomicInteger(0);
    int testValue=10;
    Activity a;
    Activity t= new PTask( (Step n, Job j)-> {
      out.set(10);
      return null;
    });
    Activity e= new PTask( (Step n, Job j)-> {
      out.set(20);
      return null;
    });
    a= If.apply( (Job j) -> {
      return true;
    }, t,e);
    s.handle(a,  null);
    pause(1500);
    assertEquals(testValue, out.get());

    testValue=20;
    t= new PTask( (Step n, Job j)-> {
      out.set(10);
      return null;
    });
    e= new PTask( (Step n, Job j)-> {
      out.set(20);
      return null;
    });
    a= If.apply( (Job j) -> {
      return false;
    }, t,e);
    s.handle(a,  null);
    pause(1500);
    assertEquals(testValue, out.get());

  }

  @Test
  public void testWFlowSwitch() throws Exception {
    WFlowServer s= new WFlowServer(ServerCore.apply());
    s.start();
    AtomicInteger out= new AtomicInteger(0);
    final int testValue=10;
    Activity a=null;
    a= PTask.apply( (Step cur, Job j) -> {
        out.set(10);
        return null;
    });
    Activity dummy= new PTask( (Step n, Job j)-> {
      return null;
    });
    a=Switch.apply((Job j) -> {
      return "bonjour";
    }).withChoice("hello", dummy)
    .withChoice("goodbye", dummy)
    .withChoice("bonjour", a);
    s.handle(a,null);
    pause(1500);
    assertEquals(testValue, out.get());

    a=Switch.apply((Job j) -> {
      return "bonjour";
    }).withChoice("hello", dummy)
    .withChoice("goodbye", dummy)
    .withDft(a);
    s.handle(a,null);
    pause(1500);
    assertEquals(testValue, out.get());

  }

  @Test
  public void testWFlowFor() throws Exception {
    WFlowServer s= new WFlowServer(ServerCore.apply());
    s.start();
    AtomicInteger out= new AtomicInteger(0);
    final int testValue=10;
    Activity a=null;
    a= PTask.apply( (Step cur, Job j) -> {
        //System.out.println("index = " + j.getv(For.JS_INDEX));
        out.incrementAndGet();
        return null;
    });
    a=For.apply( (Job j) -> { return testValue; }, a);
    s.handle(a,null);
    pause(1500);
    assertEquals(testValue, out.get());
  }

  @Test
  public void testWFlowWhile() throws Exception {
    WFlowServer s= new WFlowServer(ServerCore.apply());
    s.start();
    AtomicInteger out= new AtomicInteger(0);
    final int testValue=10;
    Activity a=null;
    a= PTask.apply( (Step cur, Job j) -> {
        int v= (int) j.getv("count");
        j.setv("count", (v+1));
        out.getAndIncrement();
        System.out.println("count = " + v);
        return null;
    });
    a=While.apply( (Job j) -> {
      Object v= j.getv("count");
      if (v==null) {
        j.setv("count", 0);
      }
      return (int)j.getv("count") < testValue;
    }, a);
    s.handle(a,null);
    pause(1500);
    assertEquals(testValue, out.get());
  }


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(deftest czlabtestwflow-test

  (is (CU/isNichts? CU/NICHTS))

)

;;(clojure.test/run-tests 'czlabtest.wflow.test)

