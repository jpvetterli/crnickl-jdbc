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
 * Type: T050_ChronicleTest
 * Version: 1.0.1
 */
package ch.agent.crnickl.junit;

import junit.framework.TestCase;
import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.api.Attribute;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.UpdatableChronicle;

public class T050_ChronicleTest extends TestCase {

	private Database db;
	private static boolean clean;
	private static final String FULLNAME = "bt.entitytest";
	private static final String SIMPLENAME = "entitytest";
	
	@Override
	protected void setUp() throws Exception {
		db = DBSetUp.getDatabase();
		if (!clean) {
			Chronicle testData = db.getChronicle(FULLNAME, false);
			if (testData != null) {
				new SpecialMethodsForChronicles().deleteChronicleCollection(testData);
				UpdatableChronicle upd = testData.edit();
				upd.destroy();
				upd.applyUpdates();
//				db.commit();
			}
			clean = true;
		}
	}
	
	public void test1() {
		try {
			UpdatableChronicle testEntity = ((UpdatableChronicle)db.getTopChronicle()).createChronicle(SIMPLENAME, false, "test", null, null);
			testEntity.applyUpdates();
			assertEquals(FULLNAME, testEntity.getName(true));
//			db.commit();
		} catch (Exception e) {
//			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public void test2() {
		try {
			UpdatableChronicle testEntity = ((UpdatableChronicle)db.getTopChronicle()).createChronicle(SIMPLENAME, false, "test", null, null);
			testEntity.applyUpdates();
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D40126, e.getMsg().getKey());
		}
	}
	
	public void test3() {
		try {
			UpdatableChronicle testEntity = db.getChronicle(FULLNAME, true).edit();
			testEntity.destroy();
			testEntity.applyUpdates();
//			db.commit();
			assertFalse(testEntity.getSurrogate().getObject().isValid());
		} catch (T2DBException e) {
			fail(e.getMessage());
		}
	}
	
	public void test4() {
		// original bug: NPE when getting non-existing attribute of entity in construction 
		try {
			UpdatableChronicle e = db.getTopChronicle().edit().createChronicle(DBSetUp.SIMPLE_NAME, false, "junit test 001", null, null);
			Attribute<?> a = e.getAttribute("foo", false);
			assertNull(a);
			a = e.getAttribute("bar", true);
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D40114, e.getMsg().getKey());
		}
	}
	
	public void test5() {
		try {
			UpdatableChronicle e = db.getTopChronicle().edit().createChronicle(DBSetUp.SIMPLE_NAME, false, "junit test 001", null, null);
			e.applyUpdates();
			Attribute<?> a = e.getAttribute("foo", false);
			assertNull(a);
			a = e.getAttribute("bar", true);
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D40114, e.getMsg().getKey());
		}
	}
	
	public void test6() {
		// original bug: NPE when creating non-existing attribute of entity in construction 
		try {
			UpdatableChronicle e = db.getChronicle(DBSetUp.BASE_NAME, true).edit();
			e.createSeries("foo");
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D40114, e.getMsg().getKey());
		}
	}
	
}