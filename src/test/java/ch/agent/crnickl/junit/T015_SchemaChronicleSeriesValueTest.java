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
import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.api.Attribute;
import ch.agent.crnickl.api.AttributeDefinition;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Schema;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableProperty;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.crnickl.api.ValueType;
import ch.agent.t2.time.Day;

public class T015_SchemaChronicleSeriesValueTest extends TestCase {

	private Database db;
	
	@Override
	protected void setUp() throws Exception {
		db = DBSetUp.getDatabase();
	}

	public void test012_create_type() {
		try {
			UpdatableValueType<String> vt = db.createValueType("foo type", true, "TEXT");
			vt.addValue(vt.getScanner().scan("bar"), "it's bar");
			vt.applyUpdates();
			assertEquals("foo type", db.getValueType(vt.getSurrogate()).getName());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test013_value_type() {
		try {
			ValueType<String> vt = db.getValueType("foo type");
			Collection<String> values = vt.getValues(null);
			assertEquals("bar - it's bar", values.iterator().next());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test016_add_value_but_should_not() {
		try {
			UpdatableValueType<String> vt = db.getValueType("foo type").typeCheck(String.class).edit();
			vt.updateValue(vt.getScanner().scan("baz"), "BAZ");
			vt.applyUpdates();
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D10123, e.getMsg().getKey());
		}
	}
	
	public void test020_add_value_and_delete_it() {
		try {
			UpdatableValueType<String> vt = db.getValueType("foo type").typeCheck(String.class).edit();
			vt.addValue(vt.getScanner().scan("baz"), "BAZ");
			vt.addValue(vt.getScanner().scan("barf"), "BARF");
			vt.applyUpdates();
			vt.deleteValue(vt.getScanner().scan("baz"));
			vt.applyUpdates();
			assertTrue(vt.getValueDescriptions().get("baz")== null);
		} catch (T2DBException e) {
			fail(e.toString());
		}
	}

	public void test024_create_property() {
		try {
			ValueType<String> type = db.getValueType("foo type");
			UpdatableProperty<String> p = db.createProperty("foo property", type, true);
			p.applyUpdates();
			assertEquals("foo type", db.getProperty(p.getSurrogate()).getValueType().getName());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test028_create_schema() {
		try {
			UpdatableSchema schema = db.createSchema("foo schema", null);
			schema.addAttribute(2);
			schema.setAttributeProperty(2, db.getProperty("foo property", true));
			schema.setAttributeDefault(2, "bar");
			schema.addSeries(1);
			schema.setSeriesName(1, "fooser");
			schema.setSeriesType(1, "numeric");
			schema.setSeriesTimeDomain(1, Day.DOMAIN);
			schema.applyUpdates();
			assertEquals("foo property", db.getSchemas("foo schema").iterator().next().
					getAttributeDefinition(2, true).getName());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test032_create_entity() {
		try {
			Schema schema = db.getSchemas("foo schema").iterator().next();
			UpdatableChronicle ent = db.getTopChronicle().edit().createChronicle("fooent", false, "foo entity", null, schema);
			ent.getAttribute("foo property", true);
			ent.applyUpdates();
			assertNotNull(ent.getAttribute("foo property", false));
			assertNull(ent.getAttribute("bar property", false));
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void test036_create_series_and_add_value() {
		try {
			UpdatableChronicle ent = db.getChronicle("bt.fooent", true).edit();
			UpdatableSeries<Double> s = ent.updateSeries("fooser");
			assertNull(s);
			s = db.getUpdatableSeries("bt.fooent.fooser", true);
			assertNotNull(s);
			s.setValue(Day.DOMAIN.time("2011-06-30"), 42.);
			s.applyUpdates();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public void test040_create_series_and_delete_value() {
		try {
			UpdatableChronicle ent = db.getChronicle("bt.fooent", true).edit();
			UpdatableSeries<Double> s = ent.updateSeries("fooser");
			assertNotNull(s);
			s.setValue(Day.DOMAIN.time("2011-06-30"), Double.NaN);
			s.applyUpdates();
			// next should not throw an exception
			s.setValue(Day.DOMAIN.time("2011-06-30"), Double.NaN);
			s.applyUpdates();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public void test044_update_attribute() {
		try {
			UpdatableChronicle ent = db.getChronicle("bt.fooent", true).edit();
			Attribute<String> attr = ent.getAttribute("foo property", true).typeCheck(String.class);
			assertEquals("bar", attr.get());
			attr.set("baz");
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D20110, e.getMsg().getKey());
			assertEquals(D.D10115, ((T2DBException) e.getCause()).getMsg().getKey());
		}
	}
	
	public void test048_update_attribute() {
		try {
			UpdatableProperty<String> prop = db.getProperty("foo property", true).typeCheck(String.class).edit();
			UpdatableValueType<String> vt = prop.getValueType().edit();
			vt.addValue("baz", "this is baz");
			vt.applyUpdates();
			UpdatableChronicle ent = db.getChronicle("bt.fooent", true).edit();
			Attribute<String> attr = ent.getAttribute("foo property", true).typeCheck(String.class);
			attr.set("baz");
			ent.setAttribute(attr);
			ent.applyUpdates();
			Attribute<String> a = ent.getAttribute("foo property", true).typeCheck(String.class);
			assertEquals("baz", a.get());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test049_update_attribute() {
		try {
			UpdatableChronicle ent = db.getChronicle("bt.fooent", true).edit();
			Attribute<String> attr = ent.getAttribute("foo property", true).typeCheck(String.class);
			assertEquals("baz", attr.get());
			attr.set("barf");
			ent.setAttribute(attr);
			ent.applyUpdates();
			attr.set("baz");
			ent.setAttribute(attr);
			ent.applyUpdates();
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test052_get_attribute() {
		try {
			Chronicle ent = db.getChronicle("bt.fooent", true);
			Attribute<String> attr = ent.getAttribute("foo property", true).typeCheck(String.class);
			assertEquals("baz", attr.get());
			assertEquals("this is baz", attr.getDescription(true));
			assertEquals(null, attr.getDescription(false));
			assertEquals("this is baz", attr.getProperty().getValueType().getValueDescriptions().get("baz"));
			AttributeDefinition<String> def = ent.getSchema(true).getAttributeDefinition("foo property", true).typeCheck(String.class);
			assertEquals("bar", def.getAttribute().get());
			assertEquals("it's bar", def.getAttribute().getDescription(true));
			assertEquals(null, def.getAttribute().getDescription(false));
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
}
