/*
 * Copyright 2020 nghiatc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ntc.leveldb;

import org.junit.*;

/**
 *
 * @author nghiatc
 * @since Jun 4, 2020
 */
public class TestNLSerializer {
    private static NLSerializer nls;
    
    @BeforeClass
    public static void init() {
        nls = new NLSerializer();
    }
 
    @Before
    public void beforeEachTest() {
        //System.out.println("This is executed before each Test");
    }
 
    @After
    public void afterEachTest() {
        //System.out.println("This is executed after each Test");
    }
 
    @Test
    public void testSDBool() {
        boolean b1 = true;
        byte[] bb = nls.serializeBool(b1);
        boolean b2 = nls.deserializeBool(bb);
        Assert.assertEquals("testSDBool", b1, b2);
    }
    
    @Test
    public void testSDInt() {
        int i1 = 10;
        byte[] ii = nls.serializeInt(i1);
        int i2 = nls.deserializeInt(ii);
        Assert.assertEquals("testSDInt", i1, i2);
    }
    
    @Test
    public void testSDLong() {
        long l1 = 11;
        byte[] ll = nls.serializeLong(l1);
        long l2 = nls.deserializeLong(ll);
        Assert.assertEquals("testSDLong", l1, l2);
    }
    
    @Test
    public void testSDFloat() {
        float f1 = 12.0000001F;
        byte[] ff = nls.serializeFloat(f1);
        float f2 = nls.deserializeFloat(ff);
        Assert.assertEquals("testSDFloat", f1, f2, 0.00000001F);
    }
    
    @Test
    public void testSDDouble() {
        double d1 = 13.0000001D;
        byte[] dd = nls.serializeDouble(d1);
        double d2 = nls.deserializeDouble(dd);
        Assert.assertEquals("testSDDouble", d1, d2, 0.00000001D);
    }
    
    @Test
    public void testSDString() {;
        String s1 = "nghiatc";
        byte[] ss = nls.serializeString(s1);
        String s2 = nls.deserializeString(ss);
        Assert.assertEquals("testSDString", s1, s2);
    }
}
