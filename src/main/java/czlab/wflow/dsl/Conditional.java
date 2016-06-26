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
 * @author Kenneth Leung
 *
 */
abstract class Conditional extends Activity {

  protected Conditional(String name, BoolExpr expr) {
    super(name);
    _expr= expr;
  }

  protected Conditional(BoolExpr expr) {
    this("", expr);
  }

  public BoolExpr expr() { return _expr; }

  private BoolExpr _expr;
}



/**
 *
 * @author Kenneth Leung
 *
 */
abstract class ConditionalDot extends FlowDot {

  protected ConditionalDot(FlowDot c, Conditional a) {
    super(c,a);
  }

  public FlowDot withTest(BoolExpr expr) {
    _expr=expr;
    return this;
  }

  protected boolean test(Job j) {
    return _expr.ptest(j);
  }

  private BoolExpr _expr;
}


