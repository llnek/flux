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


package czlab.wflow;

/**
 * @author kenl
 *
 */
public class PTask extends Activity {

  public static PTask PTaskWrapper(String name, Work w) {
    return new PTask(name, w);
  }

  public static PTask PTaskWrapper(Work w) {
    return new PTask("",w);
  }

  public static PTask apply(String name, Work w) {
    return new PTask(name, w);
  }

  public static PTask apply(Work w) {
    return apply("",w);
  }

  private Work _work;

  public PTask(String name, Work w) {
    super(name);
    _work=w;
  }

  public PTask(Work w) {
    this("",w);
  }

  public FlowDot reifyDot(FlowDot cur) {
    return new PTaskDot(cur, this);
  }

  public void realize(FlowDot n) {
    PTaskDot s= (PTaskDot) n;
    s.withWork(_work);
  }

  public Work work() { return _work; }

}


/**
 *
 * @author kenl
 *
 */
class PTaskDot extends FlowDot {

  public PTaskDot(FlowDot c, PTask a) {
    super(c,a);
  }

  public PTaskDot withWork(Work w) {
    _work=w;
    return this;
  }

  public FlowDot eval(Job j) {
    //TLOG.debug("PTaskDot: {} about to exec work.", this.id )
    Object a= _work.on(this,j);
    FlowDot rc= next();

    if (a instanceof Nihil) {
      rc = new NihilDot( j );
    }
    else
    if (a instanceof Activity) {
      rc = ((Activity) a).reify(rc);
    }
    else {
    }

    return rc;
  }

  private Work _work= null;
}


