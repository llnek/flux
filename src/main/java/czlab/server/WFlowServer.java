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


package czlab.server;

import static java.lang.invoke.MethodHandles.lookup;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;

import czlab.xlib.Schedulable;
import czlab.xlib.Activable;
import czlab.xlib.CU;
import czlab.wflow.*;


/**
 *
 * @author Kenneth Leung
 *
 */
public class WFlowServer implements ServerLike, ServiceHandler {

  public static final Logger TLOG=getLogger(lookup().lookupClass());

  private JobCreator _jctor;
  private Schedulable _sch;

  public static void main(String[] args) {
    try {
      WFlowServer s= new WFlowServer(ServerCore.apply()).start();
      Activity a, b, c,d,e,f;
      a= PTask.apply((Step cur, Job job)-> {
          System.out.println("A");
          return null;
      });
      b= PTask.apply((Step cur, Job job) -> {
          System.out.println("B");
          return null;
      });
      c= a.chain(b);
      d= PTask.apply((Step cur, Job job) -> {
          System.out.println("D");
          return null;
      });
      e= PTask.apply((Step cur, Job job) -> {
          System.out.println("E");
          return null;
      });
      f= d.chain(e);

      s.handle(c.chain(f), Collections.EMPTY_MAP);

      //Thread.sleep(1000);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public WFlowServer(final Schedulable s) {
    _jctor= new JobCreator(this);
    _sch= s;
  }

  @Override
  public Schedulable core() {
    return _sch;
  }

  public WFlowServer start(Properties options) {
    if (_sch instanceof Activable) {
      ((Activable)_sch).activate(options);
    }
    return this;
  }

  public WFlowServer start() {
    return start(new Properties());
  }

  public void dispose() {
    _sch.dispose();
  }

  @Override
  public Object handleError(Throwable t) {
    WorkStream ex = null;
    if (t instanceof StepError) {
      StepError fe = (StepError)t;
      Step n=fe.lastStep();
      Object obj = null;
      if (n != null) {
        obj= n.job().wflow();
      }
      if (obj instanceof WorkStream) {
        ex= (WorkStream)obj;
        t= fe.getCause();
      }
    }
    if (ex != null) {
      return ex.onError(t);
    } else {
      return null;
    }
  }

  @Override
  public Object handle(final Object arg, final Object options) throws Exception {
    WorkStream wf= null;
    if (arg instanceof Work) {
      wf=new WorkStream() {
        public Activity onError(Throwable t) { return null; }
        public Activity startWith() {
          return PTask.apply((Work) arg);
        }
      };
    }
    else
    if (arg instanceof WorkStream) {
      wf= (WorkStream) arg;
    }
    else
    if (arg instanceof Activity) {
      wf=new WorkStream() {
        public Activity onError(Throwable t) { return null; }
        public Activity startWith() { return (Activity)arg; }
      };
    }

    if (wf == null) {
      throw new StepError("no valid workflow to handle.");
    }

    Step end= Nihil.apply().reify( _jctor.newJob(wf));
    core().run( wf.startWith().reify(end));
    return null;
  }

}



class JobCreator {

  protected static final String JS_FLATLINE = "____flatline";
  protected static final String JS_LAST = "____lastresult";
  private final WFlowServer _server;

  public JobCreator(WFlowServer s) {
    _server=s;
  }

  public Job newJob(WorkStream wf) {
    return newJob(wf, new NonEvent());
  }

  public Job newJob(WorkStream wf, final Event evt) {
    return new Job() {
      private Map<Object,Object> _m= new HashMap<>();
      private long _id= CU.nextSeqLong();

      @Override
      public Object getv(Object key) {
        return key==null ? null : _m.get(key);
      }

      @Override
      public void setv(Object key, Object p) {
        if (key != null) {
          _m.put(key, p);
        }
      }

      @Override
      public void unsetv(Object key) {
        if (key != null) {
          _m.remove(key);
        }
      }

      @Override
      public void clear() {
    	  	_m.clear();
      }

      @Override
      public ServerLike container() {
        return _server;
      }

      @Override
      public Event event() {
        return evt;
      }

      @Override
      public Object id() {
        return _id;
      }

      @Override
      public void setLastResult(Object v) {
        _m.put(JS_LAST, v);
      }

      @Override
      public void clrLastResult() {
        _m.remove(JS_LAST);
      }

      @Override
      public Object getLastResult() {
        return _m.get(JS_LAST);
      }

      @Override
      public WorkStream wflow() {
        return wf;
      }

      @Override
      public void dbgShow(PrintStream out) {
      }

      @Override
      public String dbgStr() {
        return null;
      }

    };
  }

}

