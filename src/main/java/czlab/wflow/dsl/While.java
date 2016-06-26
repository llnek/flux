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
@SuppressWarnings("unused")
public class While extends Conditional {

  public static While apply(String name, BoolExpr b, Activity body) {
    return new While(name, b,body);
  }

  public static While apply(BoolExpr b, Activity body) {
    return apply("", b,body);
  }

  public While(String name, BoolExpr expr, Activity b) {
    super(name, expr);
    _body=b;
  }

  public While(BoolExpr expr, Activity b) {
    this("", expr, b);
  }

  public FlowDot reifyDot(FlowDot cur) {
    return new WhileDot(cur, this);
  }

  public void realize(FlowDot n) {
    WhileDot p= (WhileDot) n;
    assert(_body != null);
    p.withBody(_body.reify(p));
    p.withTest( expr() );
  }

  private Activity _body;
}



/**
 *
 * @author Kenneth Leung
 *
 */
class WhileDot extends ConditionalDot {

  public WhileDot(FlowDot c, While a) {
    super(c,a);
  }

  public FlowDot eval(Job j) {
    FlowDot n, rc = this;

    if ( ! test(j)) {
      //TLOG.debug("WhileDot: test-condition == false")
      rc= next();
      realize();
    } else {
      //TLOG.debug("WhileDot: looping - eval body")
      //normally n is null, but if it is not
      //switch the body to it.
      n= _body.eval(j);
      if (n != null) {

        if (n instanceof DelayDot) {
          ((DelayDot) n).setNext(rc);
          rc=n;
        }
        else
        if (n != this){
          TLOG.error("WhileDot##{}.body should not return anything.",
              getDef().getName());
          // let's not do this now
          //_body = n;
        }
      }
    }

    return rc;
  }

  public WhileDot withBody(FlowDot b) {
    _body=b;
    return this;
  }

  private FlowDot _body = null;
}


