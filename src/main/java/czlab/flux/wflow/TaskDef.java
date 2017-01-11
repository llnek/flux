/**
 * Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 * which can be found in the file epl-v10.html at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 * You must not remove this notice, or any other, from this software.
 */

package czlab.flux.wflow;

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


