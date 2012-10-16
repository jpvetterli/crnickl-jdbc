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
 * Type: T015_SchemaChronicleSeriesValueTest
 * Version: 1.0.0
 */
package ch.agent.crnickl.junit;

import java.util.Collection;

import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.UpdatableProperty;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.jdbc.T2DBJMsg.J;

/**
 * These tests must be executed together. They build upon each other. 
 * The sequence is important. The last tests cleanup.
 */
public class T013_PropertyTest extends AbstractTest {

	private Database db;
	private static boolean DUMP = false;
	
	@Override
	protected void setUp() throws Exception {
		db = DBSetUp.getDatabase();
	}

	public void test_create_type() {
		try {
			UpdatableValueType<String> vt = db.createValueType("foo type", true, "TEXT");
			vt.addValue(vt.getScanner().scan("bar"), "it's bar");
			vt.addValue(vt.getScanner().scan("baf"), "it's baf");
			vt.applyUpdates();
			assertEquals("foo type", db.getValueType(vt.getSurrogate()).getName());
			assertEquals("it's bar", db.getValueType(vt.getSurrogate()).getValueDescriptions().get("bar"));
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_create_another_type() {
		try {
			UpdatableValueType<String> vt = db.createValueType("bar type", true, "TEXT");
			vt.addValue(vt.getScanner().scan("foo"), "it's foo");
			vt.addValue(vt.getScanner().scan("baf"), "it's baf");
			vt.applyUpdates();
			assertEquals("bar type", db.getValueType(vt.getSurrogate()).getName());
			assertEquals("it's foo", db.getValueType(vt.getSurrogate()).getValueDescriptions().get("foo"));
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_create_property() {
		try {
			ValueType<String> type = db.getValueType("foo type");
			UpdatableProperty<String> p = db.createProperty("foo property", type, true);
			p.applyUpdates();
			assertEquals("foo type", db.getProperty(p.getSurrogate()).getValueType().getName());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_create_another_property() {
		try {
			ValueType<String> type = db.getValueType("bar type");
			UpdatableProperty<String> p = db.createProperty("bar property", type, true);
			p.applyUpdates();
			assertEquals("bar type", db.getProperty(p.getSurrogate()).getValueType().getName());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_cannot_create_existing_property() {
		try {
			ValueType<String> type = db.getValueType("bar type");
			UpdatableProperty<String> p = db.createProperty("bar property", type, true);
			p.applyUpdates();
			expectException();
		} catch (Exception e) {
			assertException(e, J.J20114);
		}
	}
	
	public void test_property_detects_bad_value() {
		try {
			UpdatableProperty<String> p = db.getProperty("foo property", true).typeCheck(String.class).edit();
			p.scan("baz");
			expectException();
		} catch (Exception e) {
			assertException(e, D.D20110);
		}
	}
	
	public void test_property_accepts_good_value() {
		try {
			UpdatableProperty<String> p = db.getProperty("foo property", true).typeCheck(String.class).edit();
			p.scan("baf");
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void test_get_properties_by_pattern() {
		try {
			Collection<Property<?>> props = db.getProperties("*prop*");
			if (DUMP) {
				for (Property<?> prop : props) {
					System.err.println(prop.toString());
					ValueType<?> vt = prop.getValueType();
					System.err.println("  " + vt.toString());
					if (vt.isRestricted()) {
						for (String v : vt.getValues(null)) {
							System.err.println("    " + v);
						}
					}
				}
			}
			assertEquals(2, props.size());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_rename_property() {
		try {
			UpdatableProperty<String> p = db.getProperty("foo property", true).typeCheck(String.class).edit();
			p.setName("moo property");
			p.applyUpdates();
			assertEquals("moo property", db.getProperty("moo property", true).getName());
			db.getProperty("foo property", true);
			expectException();
		} catch (Exception e) {
			assertException(e, D.D20109);
		}
	}
	
	public void test_cannot_delete_value_type_in_use() {
		try {
			UpdatableValueType<String> type = db.getValueType("foo type").typeCheck(String.class).edit();
			type.destroy();
			type.applyUpdates();
			expectException();
		} catch (Exception e) {
			assertException(e, J.J10115, J.J10119);
		}
	}
	
	public void test_delete_property_and_type() {
		try {
			UpdatableProperty<String> p = db.getProperty("moo property", true).typeCheck(String.class).edit();
			UpdatableValueType<String> vt = p.getValueType().edit();
			p.destroy();
			p.applyUpdates();
			if (db.getProperty("moo property", false) != null)
				fail("foo property found");
			vt.destroy();
			vt.applyUpdates();
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_delete_other_property_and_type() {
		try {
			UpdatableProperty<String> p = db.getProperty("bar property", true).typeCheck(String.class).edit();
			UpdatableValueType<String> vt = p.getValueType().edit();
			p.destroy();
			p.applyUpdates();
			vt.destroy();
			vt.applyUpdates();
			if (db.getProperty("bar property", false) != null)
				fail("bar property found");
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
}
