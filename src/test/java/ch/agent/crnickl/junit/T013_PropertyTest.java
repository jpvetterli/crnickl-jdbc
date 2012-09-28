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
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.UpdatableProperty;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.crnickl.api.ValueType;

public class T013_PropertyTest extends TestCase {

	private Database db;
	private static boolean DUMP = false;
	
	@Override
	protected void setUp() throws Exception {
		db = DBSetUp.getDatabase();
	}

	public void test02_create_type() {
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
	
	public void test03_create_type() {
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
	
	public void test08_create_property() {
		try {
			ValueType<String> type = db.getValueType("foo type");
			UpdatableProperty<String> p = db.createProperty("foo property", type, true);
			p.applyUpdates();
			assertEquals("foo type", db.getProperty(p.getSurrogate()).getValueType().getName());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test09_create_property() {
		try {
			ValueType<String> type = db.getValueType("bar type");
			UpdatableProperty<String> p = db.createProperty("bar property", type, true);
			p.applyUpdates();
			assertEquals("bar type", db.getProperty(p.getSurrogate()).getValueType().getName());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test10_create_property() {
		try {
			ValueType<String> type = db.getValueType("bar type");
			UpdatableProperty<String> p = db.createProperty("bar property", type, true);
			p.applyUpdates();
			fail("exception expected");
		} catch (KeyedException e) {
			assertEquals("J20114", e.getMsg().getKey());
		}
	}
	
	public void test12_use_property_bad_value() {
		try {
			UpdatableProperty<String> p = db.getProperty("foo property", true).typeCheck(String.class).edit();
			p.scan("baz");
			fail("exception expected");
		} catch (KeyedException e) {
			assertEquals("D20110", e.getMsg().getKey());
		}
	}
	
	public void test13_get_properties_by_pattern() {
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
	
	public void test14_use_property_good_value() {
		try {
			UpdatableProperty<String> p = db.getProperty("foo property", true).typeCheck(String.class).edit();
			p.scan("baf");
		} catch (KeyedException e) {
			fail(e.getMessage());
		}
	}

	public void test18_update_property() {
		try {
			UpdatableProperty<String> p = db.getProperty("foo property", true).typeCheck(String.class).edit();
			p.setName("moo property");
			p.applyUpdates();
			assertEquals("moo property", db.getProperty("moo property", true).getName());
			db.getProperty("foo property", true);
			fail("exception expected");
		} catch (KeyedException e) {
			assertEquals("D20109", e.getMsg().getKey());
		}
	}
	
	public void test20_delete_value_type_in_use() {
		try {
			UpdatableValueType<String> type = db.getValueType("foo type").typeCheck(String.class).edit();
			type.destroy();
			type.applyUpdates();
			fail("exception expected");
		} catch (KeyedException e) {
			assertEquals("J10115", e.getMsg().getKey());
			assertEquals("J10119", ((KeyedException)e.getCause()).getMsg().getKey());
		}
	}
	
	public void test24_delete_property_and_type() {
		try {
			UpdatableProperty<String> p = db.getProperty("moo property", true).typeCheck(String.class).edit();
			UpdatableValueType<String> vt = p.getValueType().edit();
			p.destroy();
			p.applyUpdates();
			if (db.getProperty("moo property", false) != null)
				fail("foo property found");
			vt.destroy();
			vt.applyUpdates();
		} catch (KeyedException e) {
			assertEquals("D20109", e.getMsg().getKey());
		}
	}
	
	public void test25_delete_property_and_type() {
		try {
			UpdatableProperty<String> p = db.getProperty("bar property", true).typeCheck(String.class).edit();
			UpdatableValueType<String> vt = p.getValueType().edit();
			p.destroy();
			p.applyUpdates();
			vt.destroy();
			vt.applyUpdates();
			db.getProperty("bar property", false);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
}
