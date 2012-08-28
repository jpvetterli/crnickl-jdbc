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
 * Type: T017_SchemaTest
 * Version: 1.0.0
 */
package ch.agent.crnickl.junit;

import junit.framework.TestCase;
import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.api.UpdatableValueType;

public class T017_SchemaTest extends TestCase {

	private Database db;
	private static boolean init = false;
	
	@Override
	protected void setUp() throws Exception {
		db = DBSetUp.getDatabase();
		if (!init) {
			// need 2 value types
			UpdatableValueType<String> vt1 = db.createValueType("type1", true, "TEXT");
			vt1.addValue(vt1.getScanner().scan("t1v1"), "type1 value1");
			vt1.addValue(vt1.getScanner().scan("t1v2"), "type1 value1");
			vt1.applyUpdates();
			UpdatableValueType<String> vt2 = db.createValueType("type2", true, "TEXT");
			vt2.addValue(vt2.getScanner().scan("t2v2"), "type1 value2");
			vt2.applyUpdates();

			// need 2 properties
			db.createProperty("prop1", vt1, false).applyUpdates();
			db.createProperty("prop2", vt2, false).applyUpdates();
			init = true;
		}
	}

	public void test01_create_schema() {
		try {
			UpdatableSchema schema = db.createSchema("schema1", null);
			schema.addAttribute(1);
			schema.setAttributeProperty(1, db.getProperty("prop1", true));
			schema.setAttributeDefault(1, "t1v1");
			schema.setAttributeProperty(1, db.getProperty("prop2", true));
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D30133, e.getMsg().getKey());
		}
	}

	public void test02_create_schema() {
		try {
			UpdatableSchema schema = db.createSchema("schema1", null);
			schema.addAttribute(1);
			schema.setAttributeProperty(1, db.getProperty("prop1", true));
			schema.setAttributeDefault(1, "t1v1");
			schema.addAttribute(1);
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D30127, e.getMsg().getKey());
		}
	}
	
	public void test03_create_schema() {
		try {
			UpdatableSchema schema = db.createSchema("schema1", null);
			schema.addAttribute(1);
			schema.setAttributeProperty(1, db.getProperty("prop1", true));
			schema.setAttributeDefault(1, "t1v1");
			schema.deleteAttribute(1);
			schema.addAttribute(1);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	/**
	 * Bug 20120626.1: cannot extend a schema on existing component.
	 * The bug appears when schema2.applyUpdates() tries to merge. 
	 */
	public void test04_create_schema() {
		try {
			UpdatableSchema schema1 = db.createSchema("schema1", null);
			schema1.addAttribute(1);
			schema1.setAttributeProperty(1, db.getProperty("prop1", true));
			schema1.setAttributeDefault(1, "t1v1");
			schema1.applyUpdates();
			UpdatableSchema schema2 = db.createSchema("schema2", "schema1");
			schema2.addAttribute(1);
			schema2.setAttributeProperty(1, db.getProperty("prop1", true));
			schema2.setAttributeDefault(1, "t1v2");
			schema2.applyUpdates();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
}
