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

import junit.framework.TestCase;
import ch.agent.core.KeyedException;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.crnickl.api.ValueType;

public class T012_ValueTypeTest extends TestCase {

	private Database db;
	
	@Override
	protected void setUp() throws Exception {
		db = DBSetUp.getDatabase();
	}

	public void test002_create_type() {
		try {
			UpdatableValueType<String> vt = db.createValueType("foo-type", true, "TEXT");
			vt.addValue(vt.getScanner().scan("bar"), "it's bar");
			vt.addValue(vt.getScanner().scan("baz"), "it's baz");
			vt.applyUpdates();
			assertEquals("foo-type", db.getValueType(vt.getSurrogate()).getName());
			assertEquals("it's bar", db.getValueType(vt.getSurrogate()).getValueDescriptions().get("bar"));
		} catch (Exception e) {
//			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public void test003_create_type() {
		try {
			UpdatableValueType<String> vt = db.createValueType("bar-type", false, "TEXT");
			vt.applyUpdates();
			assertEquals(2, db.getValueTypes("*-type").size());
			if (DBSetUp.STANDARD_REGEXP)
				assertEquals(2, db.getValueTypes("/.*-type/").size());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test004_update_type() {
		try {
			ValueType<String> vt = db.getValueType("foo-type");
			Surrogate s = vt.getSurrogate();
			UpdatableValueType<String> uvt = vt.edit();
			uvt.setName("moo-type");
			uvt.applyUpdates();
			assertEquals("moo-type", db.getValueType(s).getName());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	private void helper_delete_type(String name) {
		try {
			ValueType<String> vt = db.getValueType(name);
			UpdatableValueType<String> uvt = vt.edit();
			uvt.destroy();
			uvt.applyUpdates();
			db.getValueType(name);
			fail("exception expected");
		} catch (KeyedException e) {
//			e.printStackTrace();
			assertEquals("J10109", e.getMsg().getKey());
		}
	}
	
	public void test006_delete_type() {
		try {
			Collection<ValueType<?>> vts = db.getValueTypes("*-type");
			for (ValueType<?> vt : vts) {
				helper_delete_type(vt.getName());
			}
			helper_delete_type("foo");
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test010_create_type() {
		try {
			UpdatableValueType<String> vt = db.createValueType("foo", true, "TEXT");
//			vt.addValue(vt.getScanner().scan("foo1"), "Foo 1");
			vt.applyUpdates();
			assertEquals(0, vt.getValues().size());
		} catch (Exception e) {
//			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	public void test011_update_type_add_value() {
		try {
			UpdatableValueType<String> vt = 
					db.getValueType("foo").typeCheck(String.class).edit();
			vt.addValue(vt.getScanner().scan("foo1"), "Foo 1");
			vt.applyUpdates();
			assertEquals(1, vt.getValues().size());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test012_update_type_add_more_values() {
		try {
			UpdatableValueType<String> vt = 
					db.getValueType("foo").typeCheck(String.class).edit();
			vt.addValue(vt.getScanner().scan("foo2"), "Foo 2");
			vt.addValue(vt.getScanner().scan("foo4"), "Foo 4");
			vt.applyUpdates();
			assertEquals(3, vt.getValues().size());
		} catch (KeyedException e) {
			fail(e.getMessage());
		}
	}
	
	public void test013_update_type_no_update() {
		try {
			UpdatableValueType<String> vt = 
					db.getValueType("foo").typeCheck(String.class).edit();
			vt.applyUpdates();
		} catch (KeyedException e) {
			fail(e.getMessage());
		}
	}
	
	public void test015_update_type_edit_existing() {
		try {
			UpdatableValueType<String> vt = 
					db.getValueType("foo").typeCheck(String.class).edit();
			vt.updateValue(vt.getScanner().scan("foo1"), "Foo 1 edited");
			vt.applyUpdates();
			assertEquals("Foo 1 edited", vt.getValueDescriptions().get("foo1"));
		} catch (KeyedException e) {
			fail(e.getMessage());
		}
	}
	
	public void test016_update_type_edit_non_existing() {
		try {
			UpdatableValueType<String> vt = 
					db.getValueType("foo").typeCheck(String.class).edit();
			vt.updateValue(vt.getScanner().scan("foo3"), "Foo 3");
			vt.applyUpdates();
			fail("exception expected");
		} catch (KeyedException e) {
			assertEquals("D10123", e.getMsg().getKey());
		}
	}
	
	public void test017_update_type_delete_non_existing_value() {
		try {
			UpdatableValueType<String> vt = 
					db.getValueType("foo").typeCheck(String.class).edit();
			vt.deleteValue(vt.getScanner().scan("foo3"));
			vt.applyUpdates();
			fail("exception expected");
		} catch (KeyedException e) {
			assertEquals("D10122", e.getMsg().getKey());
		}
	}
	
	public void test018_update_type_delete_existing_value() {
		try {
			UpdatableValueType<String> vt = 
					db.getValueType("foo").typeCheck(String.class).edit();
			vt.deleteValue(vt.getScanner().scan("foo4"));
			vt.applyUpdates();
			assertEquals(2, vt.getValues().size());
		} catch (KeyedException e) {
			fail(e.getMessage());
		}
	}

//	public void test013_value_type() {
//		try {
//			ValueType<String> vt = db.getValueType("foo type");
//			Collection<String> values = vt.getValues(null);
//			assertEquals("bar - it's bar", values.iterator().next());
//		} catch (Exception e) {
//			fail(e.getMessage());
//		}
//	}
//	
//	public void test016_add_value_but_should_not() {
//		try {
//			UpdatableValueType<String> vt = db.getValueType("foo type").typeCheck(String.class).edit();
//			vt.updateValue(vt.getScanner().scan("baz"), "BAZ");
//			vt.applyUpdates();
//			fail("exception expected");
//		} catch (T2DBException e) {
//			assertEquals(D.D10123, e.getMsg().getKey());
//		}
//	}
//	
//	public void test020_add_value_and_delete_it() {
//		try {
//			UpdatableValueType<String> vt = db.getValueType("foo type").typeCheck(String.class).edit();
//			vt.addValue(vt.getScanner().scan("baz"), "BAZ");
//			vt.addValue(vt.getScanner().scan("barf"), "BARF");
//			vt.applyUpdates();
//			vt.deleteValue(vt.getScanner().scan("baz"));
//			vt.applyUpdates();
//			assertTrue(vt.getValueDescriptions().get("baz")== null);
//		} catch (T2DBException e) {
//			fail(e.toString());
//		}
//	}
}
