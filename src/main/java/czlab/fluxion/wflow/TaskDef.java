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

package czlab.fluxion.wflow;

import czlab.xlib.Nameable;

/**
 * An TaskDef is the building block of a workflow.
 *
 * @author Kenneth Leung
 *
 */
public interface TaskDef extends Nameable {

  /**
   * Instantiate a *runtime* version of this work unit as it becomes
   * part of the Workflow.
   *
   * @param nxt next step.
   * @return a *runtime* version of this TaskDef.
   */
  public Step create(Step nxt);


}


