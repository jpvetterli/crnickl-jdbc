package ch.agent.crnickl.jdbc.junit;

import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTests {

	public static Test suite() {
		TestSuite suite = new TestSuite(AllTests.class.getName());
		//$JUnit-BEGIN$
		suite.addTestSuite(T005_BasicTest.class);
		suite.addTestSuite(T005_CacheTest.class);
		suite.addTestSuite(T006_ChronicleTest_StrictMode.class);
		suite.addTestSuite(T006_ChronicleTest_NonStrictMode.class);
		suite.addTestSuite(T012_ValueTypeTest.class);
		suite.addTestSuite(T013_PropertyTest.class);
		suite.addTestSuite(T015_SchemaChronicleSeriesValueTest.class);
		suite.addTestSuite(T017_SchemaTest.class);
		suite.addTestSuite(T040_SeriesTest.class);
		suite.addTestSuite(T041_UpdatableSeriesTest.class);
		suite.addTestSuite(T042_SeriesValuesTest.class);
		suite.addTestSuite(T043_SparseSeriesTest.class);
		suite.addTestSuite(T045_EventTest.class);
		suite.addTestSuite(T050_ChronicleTest.class);
		suite.addTestSuite(T060_ByAttributeValueTest.class);
		//$JUnit-END$
		return suite;
	}

}
