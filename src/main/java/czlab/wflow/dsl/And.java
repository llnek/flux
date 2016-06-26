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
 * A "AND" enforces that all bound activities must return before it continues.
 *
 * @author Kenneth Leung
 *
 */
public class And extends Merge {

  public And(String name, Activity body) {
    super(name, body);
  }

  public And(Activity body) {
    this("",body);
  }

  public FlowDot reifyDot(FlowDot cur) {
    return new AndDot(cur, this);
  }

  public void realize(FlowDot n) {
    AndDot s = (AndDot)n;
    FlowDot x=s.next();
    s.withBranches(_branches);
    if (_body != null) {
      s.withBody( _body.reify( x));
    }
  }

}

/**
 *
 * @author Kenneth Leung
 *
 */
class AndDot extends MergeDot {

  public AndDot(FlowDot c, And a) {
    super(c,a);
  }

  public FlowDot eval(Job j) {
    int nv= _cntr.incrementAndGet();
    FlowDot rc= null;

    TLOG.debug("AndDot: size={}, cntr={}, join={}",
        size(),  nv, getDef().getName());

    // all branches have returned, proceed...
    if (nv == size() ) {
      rc= (_body == null) ? next() : _body;
      done();
    }
    return rc;
  }

  private void done() {
    TLOG.debug("AndDot: all branches have returned");
    realize();
  }

}



