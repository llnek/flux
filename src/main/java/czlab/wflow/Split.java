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
public class Split extends Composite {

  public static Split fork(String name, Activity a) {
    return new Split(name).include(a);
  }

  public static Split fork(Activity a) {
    return new Split().include(a);
  }

  public static Split applyAnd(String name, Activity andBody)  {
    return new Split(name, new And(andBody));
  }

  public static Split applyAnd(Activity andBody)  {
    return new Split(new And(andBody));
  }

  public static Split applyOr(String name, Activity orBody)  {
    return new Split(name, new Or(orBody));
  }

  public static Split applyOr(Activity orBody)  {
    return new Split(new Or(orBody));
  }

  public static Split apply(String name, Merge j) {
    return new Split(name, j);
  }

  public static Split apply(Merge j) {
    return new Split(j);
  }

  public static Split apply(String name) {
    return new Split(name);
  }

  public static Split apply() {
    return new Split();
  }

  protected Merge _theMerge;

  public Split(String name, Merge j) {
    super(name);
    _theMerge = j;
  }

  public Split(Merge j) {
    this("", j);
  }

  public Split (String name) {
    this(name,null);
  }

  public Split () {
    this("",null);
  }

  public Split includeMany(Activity... acts) {
    for (Activity a : acts) {
      add(a);
    }
    return this;
  }

  public Split include(Activity a) {
    add(a);
    return this;
  }

  public FlowDot reifyDot(FlowDot cur) {
    return new SplitDot(cur, this);
  }

  public  void realize(FlowDot n) {
    SplitDot p= (SplitDot) n;
    Merge m= _theMerge;

    if ( m == null) {      m = new NullJoin();    }
    m.withBranches( size() );

    FlowDot s = m.reify(p.next() );
    p.withBranches( new Innards(s, listChildren() ) );

    if (m instanceof NullJoin) {
        p.fallThrough();
    }

  }


}


/**
 *
 * @author kenl
 *
 */
class SplitDot extends CompositeDot {

  public SplitDot(FlowDot c, Split a) {
    super(c,a);
  }

  private boolean _fallThru=false;

  public FlowDot eval(Job j) {
    FlowDot rc= null;

    while ( !inner().isEmpty() ) {
      rc = inner().next();
      core().run(rc);
    }

    realize();

    if (_fallThru) {
      //tlog().debug("SplitDot: falling falling falling through.");
    }

    return _fallThru ? next() : null;
  }

  public SplitDot withBranches(Innards w) {
    setInner(w);
    return this;
  }

  public SplitDot fallThrough() {
    _fallThru=true;
    return this;
  }

}


