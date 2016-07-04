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
 * @author Kenneth Leung
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

  public Step createStep(Step cur) {
    return new ForStep(cur, this);
  }

  public Step realize(Step me) {
    ForStep p= (ForStep) me;
    super.realize(me);
    p.withTest(new LoopExpr(p, _loopCntr));
    return me;
  }

  private CounterExpr _loopCntr;
}

/**
 * @author Kenneth Leung
 *
 */
@SuppressWarnings("unused")
class LoopExpr implements BoolExpr {

  public LoopExpr(Step pt, CounterExpr cnt) {
    _point = pt;
    _cnt= cnt;
  }

  private CounterExpr _cnt;
  private Step _point;
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
 * @author Kenneth Leung
 *
 */
class ForStep extends WhileStep {

  public ForStep(Step c, For a) {
    super(c,a);
  }

}



