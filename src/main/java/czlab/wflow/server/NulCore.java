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


package czlab.wflow.server;

import czlab.xlib.Schedulable;

/**
 *
 * @author kenl
 *
 */
public class NulCore implements Schedulable {

  public static NulCore apply() { return new NulCore(); }
  //private String _id;

  private NulCore() {
    //_id= "NulScheduler#" + CoreUtils.nextSeqInt();
  }

  @Override
  public void postpone(Runnable w, long delayMillis) {
    if (delayMillis > 0L)
    try {
      Thread.sleep(delayMillis);
    } catch (Throwable e)
    {}
    run(w);
  }

  @Override
  public void dequeue(Runnable w) {
  }

  @Override
  public void run(Runnable w) {
    if (w != null) {
      w.run();
    }
  }

  @Override
  public void hold(Object pid, Runnable w) {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public void hold(Runnable w) {
    hold(0, w);
  }

  @Override
  public void dispose() {
  }

  @Override
  public void wakeAndRun(Object pid, Runnable w) {
    run(w);
  }

  @Override
  public void wakeup(Runnable w) {
    wakeAndRun(0, w);
  }

  @Override
  public void reschedule(Runnable w) {
    run(w);
  }

}


