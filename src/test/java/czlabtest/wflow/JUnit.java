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


package czlabtest.wflow;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.JUnit4TestAdapter;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import czlab.wflow.server.*;
import czlab.wflow.dsl.*;

//////////////////////////////////////////////////////////////////////////////
//
public class JUnit {

  public static junit.framework.Test suite()     {
    return
    new JUnit4TestAdapter(JUnit.class);
  }

  @BeforeClass
  public static void iniz() throws Exception    {
  }

  @AfterClass
  public static void finz()    {
  }

  @Before
  public void open() throws Exception    {
  }

  @After
  public void close() throws Exception    {
  }

  @Test
  public void testDummy() throws Exception {
    assertEquals(1, 1);
  }

  @Test
  public void testWFlowSplit() throws Exception {
    FlowServer s= new FlowServer(NulCore.apply());
    s.start();
    final AtomicInteger out= new AtomicInteger(0);
    int testValue=10;
    Activity a,b,c;
    a=PTask.apply( (FlowDot n, Job j) -> {
      out.set(10);
      //System.out.println("All Right! " + System.currentTimeMillis());
      return null;
    });
    b=PTask.apply( (FlowDot n, Job j) -> {
      //System.out.println("Dude! " + System.currentTimeMillis());
      try { Thread.sleep(2000);  } catch (Exception e) {}
      return null;
    });
    c=PTask.apply( (FlowDot n, Job j) -> {
      //System.out.println("Yo! " + System.currentTimeMillis());
      try { Thread.sleep(3000);  } catch (Exception e) {}
      return null;
    });
    a=Split.applyAnd(a).includeMany(b,c);
    s.handle(a, null);
    //try { Thread.sleep(5000);  } catch (Exception e) {}
    assertEquals(testValue, out.get());

    a=PTask.apply( (FlowDot n, Job j) -> {
      out.set(10);
      //System.out.println("All Right! " + System.currentTimeMillis());
      return null;
    });
    b=PTask.apply( (FlowDot n, Job j) -> {
      //System.out.println("Dude! " + System.currentTimeMillis());
      try { Thread.sleep(2000);  } catch (Exception e) {}
      return null;
    });
    c=PTask.apply( (FlowDot n, Job j) -> {
      //System.out.println("Yo! " + System.currentTimeMillis());
      try { Thread.sleep(3000);  } catch (Exception e) {}
      return null;
    });
    a=Split.applyOr(a).includeMany(b,c);
    s.handle(a, null);
    //try { Thread.sleep(5000);  } catch (Exception e) {}
    assertEquals(testValue, out.get());


    a=PTask.apply( (FlowDot n, Job j) -> {
      out.set(10);
      //System.out.println("****All Right! " + System.currentTimeMillis());
      return null;
    });
    b=PTask.apply( (FlowDot n, Job j) -> {
      //System.out.println("Dude! " + System.currentTimeMillis());
      //try { Thread.sleep(2000);  } catch (Exception e) {}
      return null;
    });
    c=PTask.apply( (FlowDot n, Job j) -> {
     // System.out.println("Yo! " + System.currentTimeMillis());
      //try { Thread.sleep(3000);  } catch (Exception e) {}
      return null;
    });
    a=Split.apply().includeMany(b,c).chain(a);
    s.handle(a, null);
    //try { Thread.sleep(5000);  } catch (Exception e) {}
    assertEquals(testValue, out.get());
  }

  @Test
  public void testWFlowIf() throws Exception {
    FlowServer s= new FlowServer(NulCore.apply());
    s.start();
    AtomicInteger out= new AtomicInteger(0);
    int testValue=10;
    Activity a;
    Activity t= new PTask( (FlowDot n, Job j)-> {
      out.set(10);
      return null;
    });
    Activity e= new PTask( (FlowDot n, Job j)-> {
      out.set(20);
      return null;
    });
    a= If.apply( (Job j) -> {
      return true;
    }, t,e);
    s.handle(a,  null);
    assertEquals(testValue, out.get());

    testValue=20;
    t= new PTask( (FlowDot n, Job j)-> {
      out.set(10);
      return null;
    });
    e= new PTask( (FlowDot n, Job j)-> {
      out.set(20);
      return null;
    });
    a= If.apply( (Job j) -> {
      return false;
    }, t,e);
    s.handle(a,  null);
    assertEquals(testValue, out.get());

  }

  @Test
  public void testWFlowSwitch() throws Exception {
    FlowServer s= new FlowServer(NulCore.apply());
    s.start();
    AtomicInteger out= new AtomicInteger(0);
    final int testValue=10;
    Activity a=null;
    a= PTask.apply( (FlowDot cur, Job j) -> {
        out.set(10);
        return null;
    });
    Activity dummy= new PTask( (FlowDot n, Job j)-> {
      return null;
    });
    a=Switch.apply((Job j) -> {
      return "bonjour";
    }).withChoice("hello", dummy)
    .withChoice("goodbye", dummy)
    .withChoice("bonjour", a);
    s.handle(a,null);
    assertEquals(testValue, out.get());

    a=Switch.apply((Job j) -> {
      return "bonjour";
    }).withChoice("hello", dummy)
    .withChoice("goodbye", dummy)
    .withDft(a);
    s.handle(a,null);
    assertEquals(testValue, out.get());

  }

  @Test
  public void testWFlowFor() throws Exception {
    FlowServer s= new FlowServer(NulCore.apply());
    s.start();
    AtomicInteger out= new AtomicInteger(0);
    final int testValue=10;
    Activity a=null;
    a= PTask.apply( (FlowDot cur, Job j) -> {
        //System.out.println("index = " + j.getv(For.JS_INDEX));
        out.incrementAndGet();
        return null;
    });
    a=For.apply( (Job j) -> { return testValue; }, a);
    s.handle(a,null);
    assertEquals(testValue, out.get());
  }

  @Test
  public void testWFlowWhile() throws Exception {
    FlowServer s= new FlowServer(NulCore.apply());
    s.start();
    AtomicInteger out= new AtomicInteger(0);
    final int testValue=10;
    Activity a=null;
    a= PTask.apply( (FlowDot cur, Job j) -> {
        int v= (int) j.getv("count");
        j.setv("count", (v+1));
        out.getAndIncrement();
        System.out.println("count = " + v);
        return null;
    });
    a=While.apply( (Job j) -> {
      Object v= j.getv("count");
      if (v==null) {
        j.setv("count", 0);
      }
      return (int)j.getv("count")< testValue;
    }, a);
    s.handle(a,null);
    assertEquals(testValue, out.get());
  }


}


