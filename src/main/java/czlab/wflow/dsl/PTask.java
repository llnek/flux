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

/**
 * @author Kenneth Leung
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

  public Step createStep(Step cur) {
    return new PTaskStep(cur, this);
  }

  public Step realize(Step me) {
    PTaskStep s= (PTaskStep) me;
    s.withWork(_work);
    return me;
  }

  public Work work() { return _work; }

}


/**
 *
 * @author Kenneth Leung
 *
 */
class PTaskStep extends Step {

  public PTaskStep(Step c, PTask a) {
    super(c,a);
  }

  public PTaskStep withWork(Work w) {
    _work=w;
    return this;
  }

  public Step handle(Job j) {
    //TLOG.debug("PTaskDot: {} about to exec work.", this.id )
    Object a= _work.eval(this,j);
    Step rc= next();

    if (a instanceof Nihil) {
      rc = new NihilStep( j );
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


