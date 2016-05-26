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
public class Delay extends Activity {

  public static Delay apply(String name, long delay) {
    return new Delay(name, delay);
  }

  public static Delay apply(long delay) {
    return apply("", delay);
  }

  public Delay(String name, long delay) {
    super(name);
    _delayMillis = delay;
  }

  public Delay(long delay) {
    this("", delay);
  }

  public Delay(String name) {
    this(name,0L);
  }

  public Delay() {
    this("", 0L);
  }

  public FlowDot reifyDot(FlowDot cur) {
    return new DelayDot(cur,this);
  }

  public void realize(FlowDot fp) {
    DelayDot p= (DelayDot) fp;
    p.withDelay(_delayMillis);
  }

  public long delayMillis() {
    return _delayMillis;
  }

  private long _delayMillis;
}


/**
 *
 * @author kenl
 *
 */
class DelayDot extends FlowDot {

  public long delayMillis() { return _delayMillis; }
  public FlowDot eval(Job j) { return this; }

  public DelayDot(FlowDot c, Delay a) {
    super(c,a);
  }

  public FlowDot withDelay(long millis) {
    _delayMillis=millis;
    return this;
  }

  private long _delayMillis= 0L;
}





