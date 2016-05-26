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
 * @author kenl
 *
 */
public class If extends Conditional {

  public static If apply(BoolExpr expr,Activity then, Activity elze) {
    return new If(expr,then,elze);
  }

  private Activity _thenCode;
  private Activity _elseCode;

  public If(String name, BoolExpr expr,Activity then, Activity elze) {
    super(name, expr);
    _elseCode= elze;
    _thenCode= then;
  }

  public If(BoolExpr expr,Activity then, Activity elze) {
    this("", expr, then, elze);
  }

  public If(String name, BoolExpr expr,Activity then) {
    this(name, expr, then, null);
  }

  public If(BoolExpr expr,Activity then) {
    this(expr, then, null );
  }

  public FlowDot reifyDot(FlowDot cur) {
    return new IfDot(cur,this);
  }

  public void realize(FlowDot n) {
    IfDot s= (IfDot) n;
    FlowDot nx= s.next();
    s.withElse( (_elseCode ==null) ? nx : _elseCode.reify(nx) );
    s.withThen( _thenCode.reify(nx));
    s.withTest( expr());
  }

}


/**
 *
 * @author kenl
 *
 */
class IfDot extends ConditionalDot {

  public IfDot(FlowDot c, If a) {
    super(c,a);
  }

  private FlowDot _then= null;
  private FlowDot _else= null;

  public IfDot withElse(FlowDot n ) {
    _else=n;
    return this;
  }

  public IfDot withThen(FlowDot n ) {
    _then=n;
    return this;
  }

  public FlowDot eval(Job j) {
    boolean b = test(j);
    //TLOG.debug("If: test {}", (b) ? "OK" : "FALSE");
    FlowDot rc = b ? _then : _else;
    realize();
    return rc;
  }

}



