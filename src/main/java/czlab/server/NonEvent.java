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


package czlab.server;


/**
 * @author Kenneth Leung
 */
public class NonEvent implements Event {

  private void init(final ServerLike s) {
    _emit=new EventEmitter(){
          public void dispatch(Event evt, Object options) {}
          public ServerLike container() {return s;}
          public Object getConfig() {return null;}
          public boolean isEnabled() {return true;}
          public boolean isActive() {return true;}
          public void suspend() {}
          public void resume() {}
          public EventHolder release(Object obj) {return null;}
          public void hold(EventHolder obj) {}
    };
  }

  public NonEvent(ServerLike s) {
    init(s);
  }

  public NonEvent() {
    this(null);
  }


  @Override
  public Object id() {
    return "nada";
  }

  @Override
  public EventEmitter emitter() {
    return _emit;
  }

  private EventEmitter _emit;

}



