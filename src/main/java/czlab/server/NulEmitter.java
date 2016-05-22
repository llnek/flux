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

import java.util.HashMap;
import java.util.Map;

/**
 * @author kenl
 *
 */
public class NulEmitter implements Emitter {

  private static final Map<String,?> _cfg = new HashMap<>();
  private ServerLike _server;

  public NulEmitter(ServerLike s) {
    _server=s;
  }

  @Override
  public Object getConfig() {
    return _cfg;
  }

  @Override
  public ServerLike container() {
    return _server;
  }

  @Override
  public void dispatch(Event evt, Object options) {
  }

  @Override
  public boolean isEnabled() {
    return false;
  }

  @Override
  public boolean isActive() {
    return false;
  }

  @Override
  public void suspend() {
  }

  @Override
  public void resume() {
  }

  @Override
  public EventHolder release(Object obj) {
    return null;
  }

  @Override
  public void hold(EventHolder obj) {
  }

}






