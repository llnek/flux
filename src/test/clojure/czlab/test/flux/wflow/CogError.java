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

/**
 * @author Kenneth Leung
 */
public class CogError extends Exception {

  private static final long serialVersionUID = 1L;
  private Cog _cog;

  /**/
  public CogError(Cog n, String msg, Throwable e) {
    super(msg,e);
    _cog=n;
  }

  /**/
  public CogError(Cog n, String msg) {
    super(msg);
    _cog=n;
  }

  /**/
  public CogError(Cog n,Throwable e) {
    this(n, "",e);
  }

  /**/
  public Cog lastCog() { return _cog; }

}



