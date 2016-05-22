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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kenl
 *
 */
public abstract class Merge  extends Activity {

  protected Merge(String name, Activity b) {
    super(name);
    _body=b;
  }

  protected Merge(Activity b) {
    this("",b);
  }

  protected Merge withBranches(int n) {
    _branches=n;
    return this;
  }

  protected int _branches=0;
  protected Activity _body;

}


/**
 *
 * @author kenl
 *
 */
abstract class MergeDot extends FlowDot {

  protected AtomicInteger _cntr=new AtomicInteger(0);
  protected FlowDot _body = null;
  private int _branches= 0;

  protected MergeDot(FlowDot c, Merge a) {
    super(c,a);
  }

  public MergeDot withBody(FlowDot body) {
    _body=body;
    return this;
  }

  public MergeDot withBranches(int n) {
    _branches=n;
    return this;
  }

  public int size() { return  _branches; }

  public void postRealize() {
    _cntr.set(0);
  }

}

/**
 *
 * @author kenl
 *
 */
class NullJoin extends Merge {

  public FlowDot reifyDot(FlowDot cur) {
    return new MergeDot(cur, this){
      public FlowDot eval(Job j) {
        return null;
      }
    };
  }

  public void realize(FlowDot cur) {}

  public NullJoin() {
    super(null);
  }

}
