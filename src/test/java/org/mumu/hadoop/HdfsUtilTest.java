package org.mumu.hadoop;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple HdfsUtil.
 */
public class HdfsUtilTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public HdfsUtilTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( HdfsUtilTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testHdfsUtil()
    {
        assertTrue( true );
    }
}
