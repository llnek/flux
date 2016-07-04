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
    this("", body);
  }

  public Step createStep(Step cur) {
    return new AndStep(cur, this);
  }

  public Step realize(Step me) {
    AndStep s= (AndStep)me;
    Step x= s.next();
    s.withBranches(_branches);
    if (_body != null) {
      s.withBody(_body.reify(x));
    }
    return me;
  }

}

/**
 *
 * @author Kenneth Leung
 *
 */
class AndStep extends MergeStep {

  public AndStep(Step c, And a) {
    super(c, a);
  }

  public Step handle(Job j) {
    int nv= _cntr.incrementAndGet();
    Step rc= null;

    TLOG.debug("AndStep: size={}, cntr={}, join={}",
               size(),
               nv,
               getDef().getName());

    if (nv == size() ) {
      rc= (_body == null) ? next() : _body;
      done();
    }

    return rc;
  }

  private void done() {
    TLOG.debug("AndStep: all branches have returned");
    realize();
  }

}



