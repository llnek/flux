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

import java.util.ArrayList;
import java.util.List;

/**
 * @author Kenneth Leung
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

  public Step realize(Step me) {
    CompositeStep p= (CompositeStep) me;
    p.reifyInner( listChildren() );
    return me;
  }

}


/**
 *
 * @author Kenneth Leung
 *
 */
abstract class CompositeStep extends Step {

  public void reifyInner(Iterable<Activity> c) {
    _inner=new Innards(this,c);
  }

  protected CompositeStep(Step c, Activity a) {
    super(c, a);
  }

  public Innards inner() { return _inner; }

  public void reifyInner() {
    _inner=new Innards(this);
  }

  protected void setInner(Innards n) {
    _inner=n;
  }

  private Innards _inner = null;
}

/**
 *
 * @author Kenneth Leung
 *
 */
class Innards {

  public boolean isEmpty() { return _acts.size() == 0; }

  private List<Activity> _acts= new ArrayList<>();
  private Step _outer;

  public Innards(Step c, Iterable<Activity> a) {
    this(c);
    for (Activity n: a) {
      _acts.add(n);
    }
  }

  public Innards(Step outer) {
    _outer= outer;
  }

  public Step next() {
    return _acts.size() > 0
      ? _acts.remove(0).reify(_outer) : null;
  }

  public int size() { return _acts.size(); }

}


