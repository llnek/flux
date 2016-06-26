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
 * A nothing, nada task.
 *
 * @author Kenneth Leung
 *
 */
public class Nihil  extends Activity {

  public static Nihil apply() {
    return new Nihil();
  }

  public Nihil() {}

  public FlowDot reifyDot(FlowDot cur) {
    return new NihilDot(cur.job());
  }

  public FlowDot reify(Job j) throws Exception {
    return new NihilDot(j);
  }

  public void realize(FlowDot p) {}

}


/**
 *
 * @author Kenneth Leung
 *
 */
class NihilDot extends FlowDot {

  public FlowDot eval(Job j) { return this; }
  public FlowDot next() { return this; }

  public NihilDot(Job j) {
    super(j);
  }

}


