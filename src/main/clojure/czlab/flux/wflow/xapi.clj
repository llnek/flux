;; Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.

(ns ^{:doc "A minimal worflow framework."
      :author "Kenneth Leung"}

  czlab.flux.wflow.xapi

  (:require [czlab.basal.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as cs])

  (:use [czlab.basal.core]
        [czlab.basal.meta]
        [czlab.basal.str])

  (:import [java.util.concurrent.atomic AtomicInteger]
           [java.util TimerTask]
           [czlab.jasal
            Interruptable
            Idable
            Catchable
            Initable
            Nameable
            CU
            Schedulable]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(set! *warn-on-reflection* true)


;;(defprotocol Job
  ""
  ;;public interface Job extends Gettable , Settable, Idable, Debuggable {
  ;;public void setLastResult(Object v) ;
  ;;public void clrLastResult() ;
  ;;public Object lastResult() ;
  ;;public Schedulable scheduler();
  ;;public void clear();
  ;;public Object origin() ;
  ;;public Workstream wflow();


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol ICog ;; RunnableWithId, Interruptable
  ""
  (^ICog handle [_ arg] "")
  (job [_] "")
  (rerun [_] ""))

  ;;(setNext [_ n] "") :next
  ;;(^Activity :proto [_] "")
  ;;(attrs [_] "")
  ;;(^Cog next();


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol IActivity
  "An Activity is the building block of a workflow."
  (^ICog createCog [_ nxtCog] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol INihil
  "A nothing, nada task."
  ;;public interface Nihil extends Activity {
  (^ICog createCogEx [_ job] ""))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
(defprotocol IWorkstream
  ""
  (execWith [_ job] ""))
  ;;public Object head();


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;EOF


