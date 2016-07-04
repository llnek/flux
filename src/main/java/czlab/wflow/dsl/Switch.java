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

import java.util.HashMap;
import java.util.Map;

/**
 * @author Kenneth Leung
 *
 */
public class Switch extends Activity {

  public static Switch apply(String name, ChoiceExpr e) {
    return new Switch(name, e);
  }

  public static Switch apply(ChoiceExpr e) {
    return new Switch(e);
  }

  public Switch(String name, ChoiceExpr expr) {
    super(name);
    _expr= expr;
  }

  public Switch(ChoiceExpr expr) {
    this("",expr);
  }

  private Map<Object,Activity> _choices= new HashMap<>();
  private ChoiceExpr _expr;
  private Activity _def = null;

  public Switch withChoice(Object matcher, Activity body) {
    _choices.put(matcher, body);
    return this;
  }

  public Switch withDft(Activity a) {
    _def=a;
    return this;
  }

  public Step createStep(Step cur) {
    return new SwitchStep(cur, this);
  }

  public Step realize(Step me) {
    Map<Object,Step> t= new HashMap<>();
    SwitchStep p= (SwitchStep) me;
    Step nxt= p.next();

    for (Map.Entry<Object,Activity> en: _choices.entrySet()) {
      t.put(en.getKey(), en.getValue().reify(nxt) );
    }

    p.withChoices(t);

    if (_def != null) {
      p.withDef( _def.reify(nxt));
    }

    p.withExpr(_expr);

    return me;
  }

}



/**
 *
 * @author Kenneth Leung
 *
 */
class SwitchStep extends Step {

  private Map<Object,Step> _cs = new HashMap<>();
  private ChoiceExpr _expr= null;
  private Step _def = null;

  public SwitchStep withChoices(Map<Object,Step> cs) {
    _cs.putAll(cs);
    return this;
  }

  public SwitchStep(Step c, Activity a) {
    super(c,a);
  }

  public SwitchStep withDef(Step c) {
    _def=c;
    return this;
  }

  public SwitchStep withExpr(ChoiceExpr e) {
    _expr=e;
    return this;
  }

  public Map<Object,Step> choices() { return  _cs; }

  public Step defn() { return  _def; }

  public Step handle(Job j) {
    Object m= _expr.choice(j);
    Step a= null;

    if (m != null) {
      a = _cs.get(m);
    }

    // if no match, try default?
    if (a == null) {
      a=_def;
    }

    realize();

    return a;
  }

}



