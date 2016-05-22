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

import java.util.ArrayList;
import java.util.List;

/**
 * @author kenl
 *
 */
abstract class Composite extends Activity {

  private List<Activity> _innards= new ArrayList<>();

  public int size() { return _innards.size(); }

  protected Composite(String name) {
    super(name);
  }

  protected Composite() {}

  protected void add(Activity a) {
    _innards.add(a);
  }

  public Iterable<Activity> listChildren() {
    return _innards;
  }

  public void realize(FlowDot fp) {
    CompositeDot p= (CompositeDot) fp;
    p.reifyInner( listChildren() );
  }

}


/**
 *
 * @author kenl
 *
 */
abstract class CompositeDot extends FlowDot {

  protected CompositeDot(FlowDot c, Activity a) {
    super(c,a);
  }

  public void reifyInner( Iterable<Activity> c) {
    _inner=new Innards(this,c);
  }

  public void reifyInner() {
    _inner=new Innards(this);
  }

  public Innards inner() { return _inner; }
  protected void setInner(Innards n) {
    _inner=n;
  }

  private Innards _inner = null;
}

/**
 *
 * @author kenl
 *
 */
class Innards {

  public boolean isEmpty() { return  _acts.size() == 0; }

  private List<Activity> _acts= new ArrayList<>();
  private FlowDot _outer;

  public Innards(FlowDot c, Iterable<Activity> a) {
    this(c);
    for (Activity n: a) {
    	_acts.add(n);
    }
  }

  public Innards(FlowDot outer) {
    _outer= outer;
  }

  public FlowDot next() {
    return _acts.size() > 0
      ? _acts.remove(0).reify(_outer)
      :
      null;
  }

  public int size() { return _acts.size(); }

}


