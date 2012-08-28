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
 * Type: T006_ChronicleTest_NonStrictMode
 * Version: 1.0.1
 */
package ch.agent.crnickl.junit;

import junit.framework.TestCase;
import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.impl.DatabaseBackend;

/**
 * Standalone_EntityTest tests that the name space can be left out in non-strict
 * mode: <code>dbStrictNameSpace=false</code>.
 * 
 * @author Jean-Paul Vetterli
 * @version 1.0.0
 */
public class T006_ChronicleTest_NonStrictMode extends TestCase {

	private Database db;
	private static boolean clean;
	private static final String FULLNAME = "bt.standalonetest";
	private static final String SIMPLENAME = "standalonetest";
	
	@Override
	protected void tearDown() throws Exception {
		((DatabaseBackend) db).setStrictNameSpaceMode(true);
		super.tearDown();
	}

	@Override
	protected void setUp() throws Exception {
		db = DBSetUp.getDatabase();
		((DatabaseBackend) db).setStrictNameSpaceMode(false);
		if (!clean) {
			Chronicle testData = db.getChronicle(SIMPLENAME, false);
			if (testData != null) {
				new SpecialMethodsForChronicles().deleteChronicleCollection(testData);
				UpdatableChronicle upd = testData.edit();
				upd.destroy();
				upd.applyUpdates();
			}
			UpdatableChronicle ex = db.getTopChronicle().edit().createChronicle(SIMPLENAME, false, "standalone test", null, null);
			ex.applyUpdates();
//			db.commit();
			clean = true;
		}
	}
	
	public void test1() {
		assertFalse(((DatabaseBackend) db).isStrictNameSpaceMode());
		try {
			UpdatableChronicle e = db.getChronicle(SIMPLENAME, true).edit();
			UpdatableChronicle ex = e.createChronicle("x", false, "it's x", null, null);
			ex.applyUpdates();
			Chronicle ent = db.getChronicle(SIMPLENAME + ".x", true);
			assertEquals(SIMPLENAME + ".x", ent.getName(true));
		} catch (T2DBException e) {
			fail(e.toString());
		}
	}
	
	public void test2() {
		assertFalse(((DatabaseBackend) db).isStrictNameSpaceMode());
		try {
			Chronicle en = db.getChronicle(SIMPLENAME + ".x", true);
			assertEquals(SIMPLENAME + ".x", en.getName(true));
		} catch (T2DBException e) {
			fail(e.toString());
		}
	}
	
	public void test3() {
		assertFalse(((DatabaseBackend) db).isStrictNameSpaceMode());
		try {
			// full name tolerated in non-strict mode
			Chronicle en = db.getChronicle(FULLNAME + ".x", true);
			assertEquals(SIMPLENAME + ".x", en.getName(true));
		} catch (T2DBException e) {
			fail(e.toString());
		}
	}

	
}