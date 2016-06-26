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
 */
public class FlowError extends Exception {

  private static final long serialVersionUID = 1L;
  private FlowDot _node;

  public FlowError(FlowDot n, String msg, Throwable e) {
    super(msg,e);
    _node=n;
  }

  public FlowError(String msg,Throwable e) {
    this(null, msg,e);
  }

  public FlowError(Throwable e) {
    this(null,"", e);
  }

  public FlowError(String msg) {
    this(null, msg,null);
  }

  public FlowDot getLastDot() { return _node; }

}



