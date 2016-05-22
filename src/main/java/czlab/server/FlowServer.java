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


package czlab.wflow.server;

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
 * @author kenl
 *
 */
public class FlowServer implements ServerLike, ServiceHandler {

  public static final Logger TLOG=getLogger(lookup().lookupClass());

  protected NulEmitter _mock;
  private JobCreator _jctor;
  private Schedulable _sch;

  public static void main(String[] args) {
    try {
      FlowServer s= new FlowServer(NulCore.apply()).start();
      Activity a, b, c,d,e,f;
      a= PTask.apply((FlowDot cur, Job job)-> {
          System.out.println("A");
          return null;
      });
      b= PTask.apply((FlowDot cur, Job job) -> {
          System.out.println("B");
          return null;
      });
      c= a.chain(b);
      d= PTask.apply((FlowDot cur, Job job) -> {
          System.out.println("D");
          return null;
      });
      e= PTask.apply((FlowDot cur, Job job) -> {
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

  public FlowServer(final Schedulable s) {
    _mock=new NulEmitter(this);
    _jctor= new JobCreator(this);
    _sch= s;
  }

  @Override
  public Schedulable core() {
    return _sch;
  }

  public FlowServer start(Properties options) {
    if (_sch instanceof Activable) {
      ((Activable)_sch).activate(options);
    }
    return this;
  }

  public FlowServer start() {
    return start(new Properties());
  }

  public void dispose() {
    _sch.dispose();
  }

  @Override
  public Object handleError(Throwable t) {
    WorkFlowEx ex = null;
    if (t instanceof FlowError) {
      FlowError fe = (FlowError)t;
      FlowDot n=fe.getLastDot();
      Object obj = null;
      if (n != null) {
        obj= n.job().wflow();
      }
      if (obj instanceof WorkFlowEx) {
        ex= (WorkFlowEx)obj;
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
  public Object handle(Object work, Object options) throws Exception {
    WorkFlow wf= null;
    if (work instanceof WHandler) {
      final WHandler h = (WHandler)work;
      wf=() -> {
          return PTask.apply( (FlowDot cur, Job j) -> {
            return h.run(j);
          });
      };
    }
    else
    if (work instanceof WorkFlow) {
      wf= (WorkFlow) work;
    }
    else
    if (work instanceof Work) {
      wf= () -> {
          return new PTask((Work)work);
      };
    }
    else
    if (work instanceof Activity) {
      wf = () -> {
        return (Activity) work;
      };
    }

    if (wf == null) {
      throw new FlowError("no valid workflow to handle.");
    }

    FlowDot end= Nihil.apply().reify( _jctor.newJob(wf));
    core().run( wf.startWith().reify(end));
    return null;
  }

}



class JobCreator {

  protected static final String JS_FLATLINE = "____flatline";
  protected static final String JS_LAST = "____lastresult";
  private final FlowServer _server;

  public JobCreator(FlowServer s) {
    _server=s;
  }

  public Job newJob(WorkFlow wf) {
    return newJob(wf, new NonEvent(_server._mock));
  }

  public Job newJob(WorkFlow wf, final Event evt) {
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
      public WorkFlow wflow() {
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

