/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright (c) 2013-2016, Kenneth Leung. All rights reserved. */


package czlab.wflow.dsl;

import static java.lang.invoke.MethodHandles.lookup;
import static org.slf4j.LoggerFactory.getLogger;

import org.slf4j.Logger;

import czlab.wflow.server.ServiceHandler;
import czlab.xlib.CU;
import czlab.xlib.RunnableWithId;
import czlab.xlib.Schedulable;

/**
 * @author Kenneth Leung
 *
 */
@SuppressWarnings("unused")
public abstract class FlowDot implements RunnableWithId {

  public static final Logger TLOG = getLogger(lookup().lookupClass());

  private long _pid = CU.nextSeqLong();

  private FlowDot _nextStep;
  protected Job _job;
  private Activity _defn;

  /**
   * @param c
   * @param a
   */
  protected FlowDot(FlowDot c, Activity a) {
    this( c.job() );
    _nextStep=c;
    _defn=a;
  }

  protected FlowDot(Job j) {
    _job=j;
    _defn= new Nihil();
  }

  public FlowDot next() { return _nextStep; }
  public Activity getDef() { return _defn; }
  public Object id() { return _pid; }

  public abstract FlowDot eval(Job j);

  protected void postRealize() {}

  protected FlowDot realize() {
    getDef().realize(this);
    postRealize();
    return this;
  }

  protected Schedulable core() {
    return _job.container().core();
  }

  public Job job() { return _job; }

  public void setNext(FlowDot n) {
    _nextStep=n;
  }

  public void rerun() {
    core().reschedule(this);
  }

  public void run() {
    Object par = _job.container();
    ServiceHandler svc = null;
    Activity err= null,
             d= getDef();
    FlowDot rc= null;

    core().dequeue(this);

    try {
      if (d.hasName()) {
        TLOG.debug("FlowDot##{} :eval()", d.getName());
      }
      rc= eval( _job );
    } catch (Throwable e) {
      if (par instanceof ServiceHandler) {
        svc= (ServiceHandler)par;
      }
      if (svc != null) {
        Object ret= svc.handleError(new FlowError(this,"",e));
        if (ret instanceof Activity) {
          err= (Activity)ret;
        }
      }
      if (err == null) {
        TLOG.error("",e);
        err= Nihil.apply();
      }
      rc= err.reify( new NihilDot( _job) );
    }

    if (rc==null) {
      TLOG.debug("FlowDot: rc==null => skip");
      // indicate skip, happens with joins
    } else {
      runAfter(rc);
    }
  }

  private void runAfter(FlowDot rc) {
    FlowDot np= rc.next();

    if (rc instanceof DelayDot) {
      core().postpone( np, ((DelayDot) rc).delayMillis() );
    }
    else
    if (rc instanceof NihilDot) {
      //rc.job().clear();  don't do this
      //end
    }
    else {
      core().run(rc);
    }
  }

  public void XXXfinalize() throws Throwable {
    super.finalize();
    TLOG.debug("FlowDot: " + getClass().getName() + " finz'ed");
  }

}

