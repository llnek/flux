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
public class Or extends Merge {

  public Or(String name, Activity b) {
    super(name,b);
  }

  public Or(Activity b) {
    this("",b);
  }

  public FlowDot reifyDot(FlowDot cur) {
    return new OrDot(cur, this);
  }

  public void realize(FlowDot n) {
    OrDot s= (OrDot) n;
    if (_body != null) {
      s.withBody( _body.reify(s.next() ));
    }
    s.withBranches(_branches);
  }

}


/**
 *
 * @author kenl
 *
 */
class OrDot extends MergeDot {

  public OrDot(FlowDot c, Or a) {
    super(c,a);
  }

  public FlowDot eval(Job j) {
    int nv= _cntr.incrementAndGet();
    FlowDot rc= this;
    FlowDot nx= next();

    if (size() == 0) {
      // 'or' of nothing, nothing to do
      rc= nx;
      realize();
    }
    else
    if (nv==1) {
      // got one in, proceed
      rc= (_body== null) ? nx : _body;
      // there is only one? end it
      if(size() == 1) {
        done();
      }
    }
    else
    if ( nv >= size() ) {
      // don't care about others
      rc=null;
      done();
    }

    return rc;
  }

  private void done() {
    TLOG.debug("OrDot: all branches have returned.");
    realize();
  }

}


