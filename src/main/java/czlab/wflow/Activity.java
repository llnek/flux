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

import static java.lang.invoke.MethodHandles.lookup;
import static org.slf4j.LoggerFactory.getLogger;

import org.slf4j.Logger;
import czlab.xlib.Named;


/**
 * An Activity is a definition of work - a task to be done.
 * At runtime, it has to be reified.  This process
 * turns an Activity into a Step in the Workflow.
 *
 * @author kenl
 *
 */
public abstract class Activity implements Named {

  public static final Logger TLOG = getLogger(lookup().lookupClass());
  private String _label;

  protected Activity() { this(""); }

  protected Activity(String n) {
    _label= n==null ? "" : n;
  }

  public boolean hasName() { return _label.length() > 0; }
  public String getName() {
    return _label;
  }

  /**
   * Connect up another activity to make up a chain.
   *
   * @param a the unit of work to follow after this one.
   * @return an *ordered* list of work units.
   */
  public Activity chain( Activity a) {
    return
    new Group(this).chain(a);
  }

  /**
   * Instantiate a *runtime* version of this work unit as it becomes
   * part of the Workflow.
   *
   * @param cur current step.
   * @return a *runtime* version of this Activity.
   */
  public FlowDot reify(FlowDot cur) {
    FlowDot n= reifyDot(cur);
    n.realize();
    return n;
  }

  public FlowDot reify(Job j) throws Exception {
    throw new IllegalAccessException("Should not be called");
  }

  public String toString() {
    return "Activity##(" + getClass().getName() + ")";
  }

  public void XXXfinalize() throws Throwable {
    super.finalize();
    TLOG.debug("Activity: " + getClass().getName() + " finz'ed");
  }

  protected abstract FlowDot reifyDot(FlowDot cur) ;
  protected abstract void realize(FlowDot p);

}


