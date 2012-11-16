/*
 *   Copyright 2012 Hauser Olsson GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
 * Package: ch.agent.crnickl.junit
 * Type: AllTests
 * Version: 1.0.0
 */
package ch.agent.crnickl.junit;

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
