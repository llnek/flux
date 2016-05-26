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
 * A For is treated sort of like a while with the test-condition being (i &lt; upperlimit).
 *
 * @author kenl
 *
 */
public class For extends While {

  public static final String JS_INDEX = "____index";

  public static For apply(CounterExpr loopCount, Activity body) {
    return new For(loopCount, body);
  }

  public For(CounterExpr loopCount, Activity body) {
    // put a dummy bool-expr, not used.
    super((j) -> { return false; }, body);
    _loopCntr = loopCount;
  }

  public FlowDot reifyDot(FlowDot cur) {
    return new ForDot(cur,this);
  }

  public void realize(FlowDot n) {
    ForDot p= (ForDot) n;
    super.realize(n);
    p.withTest( new LoopExpr(p, _loopCntr));
  }

  private CounterExpr _loopCntr;
}

/**
 * @author kenl
 *
 */
@SuppressWarnings("unused")
class LoopExpr implements BoolExpr {

  public LoopExpr(FlowDot pt, CounterExpr cnt) {
    _point = pt;
    _cnt= cnt;
  }

  private CounterExpr _cnt;
  private FlowDot _point;
  private int _loop=0;

  public boolean ptest(Job j) {
    int c= _cnt.gcount(j);
    int v= _loop;
    if (v < c) {
      //_point.tlog().debug("LoopExpr: loop {}", v);
      j.setv(For.JS_INDEX, v);
      ++_loop;
      return true;
    } else {
      return false;
    }
  }

}


/**
 *
 * @author kenl
 *
 */
class ForDot extends WhileDot {

  public ForDot(FlowDot c, For a) {
    super(c,a);
  }

}



