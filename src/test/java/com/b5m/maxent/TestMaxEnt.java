package com.b5m.maxent;

import junit.framework.Test;
import com.b5m.maxent.MaxEnt;

import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestMaxEnt 
    extends TestCase
{
	private MaxEnt maxent = null;
	static final public String MODEL_FILE = "/home/kevinlin/codebase/pig-udf/Model.txt";
    public TestMaxEnt( String testName )
    {
        super( testName );
        maxent = MaxEnt.instance();
        maxent.loadModel(MODEL_FILE);
    }

    public static Test suite()
    {
        return new TestSuite( TestMaxEnt.class );
    }

    public void testMaxEnt()
    {
    	String outcome = new String("服装服饰");
		assertTrue(maxent.eval(new String("蔻玲2013冬新款女狐狸毛领羊绒呢子短款大衣寇玲原价1999专柜正品")).equalsIgnoreCase(outcome));
		outcome = new String("图书音像");
		assertTrue(maxent.eval(new String("深部条带煤柱长期稳定性基础实验研究 正版包邮")).equalsIgnoreCase(outcome));
        assertTrue( true );
    }
}