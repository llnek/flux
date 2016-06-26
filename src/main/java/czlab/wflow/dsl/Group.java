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
 * A logical group - sequence of connected activities.
 *
 * @author Kenneth Leung
 *
 */
class Group extends Composite {

  public static Group apply(Activity a) {
    return new Group(a);
  }

  public Group(String name, Activity a) {
    super(name);
    if (a != null) { add(a); }
  }

  public Group(Activity a) {
    this("",a);
  }

  public Group(String name) {
    this(name, null);
  }

  public Group() {
    this("");
  }

  public Activity chainMany(Activity... acts) {
    for (Activity a: acts) {
      add(a);
    }
    return this;
  }

  public Activity chain(Activity a) {
    add(a);
    return this;
  }

  public FlowDot reifyDot(FlowDot cur) {
    return new GroupDot(cur,this);
  }

}


/**
 *
 * @author Kenneth Leung
 *
 */
class GroupDot extends CompositeDot {

  public GroupDot(FlowDot c, Group a) {
    super(c,a);
  }

  public FlowDot eval(Job j) {
    FlowDot rc= null;

    if ( ! inner().isEmpty()) {
      //TLOG.debug("Group: {} element(s.)",  _inner.size() );
      FlowDot n=inner().next();
      Activity d=n.getDef();
      if (d.hasName()) {
        TLOG.debug("FlowDot##{} :eval().", d.getName());
      }
      rc = n.eval(j);
    } else {
      //TLOG.debug("Group: no more elements.");
      rc= next();
      realize();
    }

    return rc;
  }

}

