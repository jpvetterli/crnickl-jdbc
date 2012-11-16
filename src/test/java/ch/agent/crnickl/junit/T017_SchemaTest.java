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

import java.util.Collection;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.api.Attribute;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Schema;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.t2.time.DateTime;
import ch.agent.t2.time.Day;
import ch.agent.t2.time.Workday;

public class T017_SchemaTest extends AbstractTest {

	private static Database db;
	private static Boolean DEBUG = false;
	
	@Override
	protected void firstSetUp() throws Exception {
		db = DBSetUp.getDatabase();
		// create 2 value types
		UpdatableValueType<String> vt1 = db.createValueType("type1", true, "TEXT");
		vt1.addValue(vt1.getScanner().scan("t1v1"), "type1 value1");
		vt1.addValue(vt1.getScanner().scan("t1v2"), "type1 value2");
		vt1.addValue(vt1.getScanner().scan("t1v3"), "type1 value3");
		vt1.applyUpdates();
		UpdatableValueType<String> vt2 = db.createValueType("type2", true, "TEXT");
		vt2.addValue(vt2.getScanner().scan("t2v2"), "type2 value2");
		vt2.applyUpdates();
		UpdatableValueType<String> vt3 = db.createValueType("type3", false, "TEXT");
		vt3.applyUpdates();

		// create 2 properties
		db.createProperty("prop1", vt1, false).applyUpdates();
		db.createProperty("prop2", vt2, false).applyUpdates();
		db.createProperty("prop3", vt3, false).applyUpdates();

		db.commit();
	}
	
	@Override
	protected void lastTearDown() throws Exception {
		Util.deleteChronicles(db, "bt.schema1achro", "bt.schema4chro", "bt.schema3chro");
		Util.deleteSchema(db, "schema1f", "schema5", "schema4", "schema3", "schema2","schema1a");
		if (DEBUG)
			listSchemas("schema*");
		Util.deleteProperties(db, "prop1", "prop2", "prop3");
		Util.deleteValueTypes(db, "type1", "type2", "type3");
		db.commit();
	}

	private void listSchemas(String pattern) throws Exception {
		Collection<UpdatableSchema> schemas = db.getUpdatableSchemas(pattern);
		for (UpdatableSchema schema : schemas) {
			UpdatableSchema base = schema.getBase();
			if (base == null)
				System.out.println(schema.getName());
			else
				System.out.println(schema.getName() + "--->" + schema.getBase().getName());
		}
	}
	
	public void test_create_schema_failure_series_name_used_twice () {
		try {
			UpdatableSchema schema = db.createSchema("schema1", null);
			schema.addSeries(1);
			schema.setSeriesName(1, "x25");
			schema.addSeries(2);
			// name already used
			schema.setSeriesName(2, "x25");
			expectException();
		} catch (T2DBException e) {
			assertException(e, D.D30153);
		}
	}
	
	public void test_create_schema_ok_but_not_applied() {
		try {
			UpdatableSchema schema = db.createSchema("schema1", null);
			schema.addSeries(1);
			schema.setSeriesName(1, "x25");
			schema.addAttribute(1);
			schema.setAttributeProperty(1, db.getProperty("prop1", true));
			schema.setAttributeDefault(1, "t1v1");
			assertEquals("t1v1", schema.getAttributeDefinition(1, true).getValue().toString());
			assertEquals("t1v1", schema.getAttributeDefinition("prop1", true).getValue().toString());
		} catch (T2DBException e) {
			fail(e.getMessage());
		}
	}
	public void test_create_schema_failure_illegal_value_on_property_modification() {
		try {
			UpdatableSchema schema = db.createSchema("schema1", null);
			schema.addAttribute(1);
			schema.setAttributeProperty(1, db.getProperty("prop1", true));
			schema.setAttributeDefault(1, "t1v1");
			// next one makes default value illegal
			schema.setAttributeProperty(1, db.getProperty("prop2", true));
			schema.applyUpdates();
			expectException();
		} catch (T2DBException e) {
			assertException(e, D.D30133);
		}
	}

	public void test_create_schema_failure_incomplete_attribute() {
		try {
			// adding an incomplete, non-erasing attribute should fail
			UpdatableSchema schema = db.createSchema("schema1", null);
			schema.addAttribute(1);
			schema.applyUpdates();
			expectException();
		} catch (T2DBException e) {
			assertException(e, D.D30105, D.D30111);
		}
	}
	
	public void test_create_schema_failure_incomplete_series() {
		try {
			// adding an incomplete, non-erasing series should fail
			UpdatableSchema schema = db.createSchema("schema1", null);
			schema.addSeries(1);
			schema.applyUpdates();
			expectException();
		} catch (T2DBException e) {
			assertException(e, D.D30105, D.D30112);
		}
	}
	
	public void test_create_schema_schema1f() {
		try {
			UpdatableSchema schema = db.createSchema("schema1f", null);
			schema.addSeries(1);
			schema.setSeriesName(1, "x");
			schema.setSeriesType(1, "numeric");
			schema.setSeriesTimeDomain(1, Day.DOMAIN);
			schema.applyUpdates();
		} catch (T2DBException e) {
			fail(e.getMessage());
		}
	}
	
	public void test_create_existing_schema_fails() {
		try {
			// adding an incomplete, non-erasing series attribute should fail
			UpdatableSchema schema = db.createSchema("schema1f", null);
			schema.applyUpdates();
			expectException();
		} catch (T2DBException e) {
			assertException(e, D.D30108);
		}
	}
	
	public void test_add_incomplete_series_attribute() {
		try {
			// adding an incomplete, non-erasing series attribute should fail
			UpdatableSchema schema = db.getUpdatableSchemas("schema1f").iterator().next();
			schema.addAttribute(1, 4);
			schema.applyUpdates();
			expectException();
		} catch (T2DBException e) {
			assertException(e, D.D30105, D.D30113);
		}
	}
	
	public void test_add_incomplete_series_attribute_value_required() {
		try {
			// adding an incomplete, non-erasing series attribute should fail
			UpdatableSchema schema = db.getUpdatableSchemas("schema1f").iterator().next();
			schema.addAttribute(1, 4);
			schema.setAttributeProperty(1, 4, db.getProperty("prop2", true));
			// default value is not set, but null is not okay for prop2
			schema.applyUpdates();
			expectException();
		} catch (T2DBException e) {
			assertException(e, D.D30105, D.D30113);
		}
	}
	
	public void test_add_series_attribute() {
		try {
			UpdatableSchema schema = db.getUpdatableSchemas("schema1f").iterator().next();
			schema.addAttribute(1, 4);
			schema.setAttributeProperty(1, 4, db.getProperty("prop2", true));
			schema.setAttributeDefault(1, 4, "t2v2");
			schema.applyUpdates();
		} catch (T2DBException e) {
			fail(e.getMessage());
		}
	}

	public void test_create_schema_failure_same_attribute_nr() {
		try {
			UpdatableSchema schema = db.createSchema("schema1", null);
			schema.addAttribute(1);
			schema.setAttributeProperty(1, db.getProperty("prop1", true));
			schema.setAttributeDefault(1, "t1v1");
			schema.addAttribute(1);
			schema.applyUpdates();
			expectException();
		} catch (T2DBException e) {
			assertException(e, D.D30127);
		}
	}
	
	public void test_create_schema_failure_attribute_incomplete_after_edit() {
		try {
			UpdatableSchema schema = db.createSchema("schema1", null);
			Collection<Schema> ss = db.getSchemas("schema1");
			assertEquals(0,  ss.size());

			schema.addAttribute(1);
			schema.setAttributeProperty(1, db.getProperty("prop1", true));
			schema.setAttributeDefault(1, "t1v1");
			schema.deleteAttribute(1);
			schema.addAttribute(1);
			schema.applyUpdates();
			expectException();
		} catch (Exception e) {
			assertException(e, D.D30105, D.D30111);
		}
	}
	
	public void test_create_schema_failure_attribute_property_duplicate() {
		try {
			UpdatableSchema schema = db.createSchema("schema1", null);
			schema.addAttribute(1);
			schema.setAttributeProperty(1, db.getProperty("prop1", true));
			schema.setAttributeDefault(1, "t1v1");
			schema.addAttribute(2);
			schema.setAttributeProperty(2, db.getProperty("prop1", true));
			schema.setAttributeDefault(2, "t1v1");
			schema.applyUpdates();
			schema.resolve();
			expectException();
		} catch (Exception e) {
			assertException(e, D.D30151, D.D30130);
		}
	}
	
	public void test_create_empty_schema() {
		try {
			UpdatableSchema schema = db.createSchema("schema5", null);
			schema.applyUpdates();
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_create_chronicle_with_updatable_schema_fails() {
		try {
			// creating a chronicle with an updatable schema should be impossible
			UpdatableSchema schema = db.getUpdatableSchemas("schema5").iterator().next();
			UpdatableChronicle chro = db.getTopChronicle().edit().createChronicle("schema5chro", false, "test chronicle", null, schema);
			chro.applyUpdates();
			expectException();
		} catch (Exception e) {
			assertException(e, D.D40104);
		}
	}
	
	public void test_create_chronicle_with_empty_schema_okay() {
		try {
			// creating a chronicle with an empty schema should be possible
			Schema schema = db.getSchemas("schema5").iterator().next();
			UpdatableChronicle chro = db.getTopChronicle().edit().createChronicle("schema5chro", false, "test chronicle", null, schema);
			chro.applyUpdates();
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_delete_chronicle() {
		try {
			/*
			 * There was a bug allowing to create chronicle with incomplete
			 * schemas (see earlier test). This showed a bug making it
			 * impossible to delete a chronicle with no series defined.
			 */
			Util.deleteChronicles(db, "bt.schema5chro");
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void test_create_schema1_and_schema2() {
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
			fail(e.getMessage());
		}
	}
	
	public void test_define_series_in_schema1() {
		try {
			UpdatableSchema schema1 = db.getSchemas("schema1").iterator().next().edit();
			Schema schema2 = db.getSchemas("schema2").iterator().next();
			schema1.addSeries(1);
			schema1.setSeriesName(1, "foo");
			schema1.setSeriesDescription(1, "foo series");
			schema1.setSeriesTimeDomain(1, Workday.DOMAIN);
			schema1.setSeriesType(1, db.getValueType("numeric"));
			schema1.addAttribute(1, 6);
			schema1.setAttributeProperty(1, 6, db.getProperty("prop2", true));
			schema1.setAttributeDefault(1, 6, "t2v2");
			schema1.applyUpdates();
			assertEquals("foo", schema1.getSeriesDefinition(1, true).getName());
			assertEquals("foo series", schema1.getSeriesDefinition(1, true).getDescription());
			assertEquals(null, schema2.getSeriesDefinition(1, false));
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_edit_and_add_series_in_schema1() {
		try {
			UpdatableSchema schema1 = db.getSchemas("schema1").iterator().next().edit();
			schema1.setSeriesName(1, "fou");
			schema1.setSeriesDescription(1, "fou series");
			schema1.applyUpdates();
			assertEquals("fou", schema1.getSeriesDefinition(1, true).getName());
			assertEquals("fou series", schema1.getSeriesDefinition(1, true).getDescription());
			schema1.addSeries(3);
			schema1.setSeriesName(3, "foo");
			schema1.setSeriesDescription(3, "fully foo");
			schema1.setSeriesTimeDomain(3, Workday.DOMAIN);
			schema1.setSeriesType(3, db.getValueType("numeric"));
			schema1.applyUpdates();
			assertEquals(Workday.DOMAIN, schema1.getSeriesDefinition(3, true).getTimeDomain());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void test_rename_schema1_schema1a() {
		try {
			UpdatableSchema schema1 = db.getSchemas("schema1").iterator().next().edit();
			schema1.setName("schema1a");
			schema1.applyUpdates();
			assertEquals(1, db.getSchemas("schema1a").size());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_create_schema3_for_base_of_schema1a() {
		try {
			UpdatableSchema schema3 = db.createSchema("schema3", null);
			schema3.applyUpdates();
			UpdatableSchema schema1 = db.getSchemas("schema1a").iterator().next().edit();
			schema1.setBase(schema3);
			schema1.applyUpdates();
			assertEquals(schema3, schema1.getBase());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_reset_base_of_schema1a() {
		try {
			UpdatableSchema schema1 = db.getSchemas("schema1a").iterator().next().edit();
			schema1.setBase(null);
			schema1.applyUpdates();
			assertEquals(null, schema1.getBase());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_cannot_set_base_to_schema_in_construction() {
		try {
			UpdatableSchema schema4 = db.createSchema("schema4", null);
			schema4.addAttribute(1);
			schema4.setAttributeProperty(1, db.getProperty("prop1", true));
			schema4.setAttributeDefault(1, "t1v1");
//			schema4.applyUpdates();
			UpdatableSchema schema1 = db.getSchemas("schema1a").iterator().next().edit();
			schema1.setBase(schema4);
			expectException();
		} catch (Exception e) {
			assertException(e, D.D30128);
		}
	}

	public void test_create_schema4_based_on_schema1a_and_override_attribute() {
		UpdatableSchema schema1 = null;
		try {
			UpdatableSchema schema4 = db.createSchema("schema4", "schema1a");
			Schema s = schema4.resolve();
			assertEquals("prop1", s.getAttributeDefinition(1, true).getName());
			schema4.addAttribute(1);
			schema4.eraseAttribute(1);
			assertEquals(true, schema4.getAttributeDefinition(1, true).isErasing());
			assertEquals("fou", s.getSeriesDefinition(1, true).getName());
			schema4.setAttributeProperty(1, db.getProperty("prop1", true));
			schema4.setAttributeDefault(1, "t1v1");
			schema4.applyUpdates();
			schema1 = db.getSchemas("schema1a").iterator().next().edit();
			schema1.setBase(schema4);
			schema1.applyUpdates();
			expectException();
		} catch (Exception e) {
			assertException(e, null, D.D30110);
		}
	}
	
	public void test_add_series_to_schema4_but_dont_apply() {
		try {
			UpdatableSchema schema4 = db.getUpdatableSchemas("schema4").iterator().next();
			Schema s = schema4.resolve();
			assertEquals("fou", s.getSeriesDefinition(1, true).getName());
			schema4.addSeries(1);
			schema4.eraseSeries(1);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_set_schema2_as_base_of_schema3() {
		// related to test37
		try {
			UpdatableSchema schema2 = db.getUpdatableSchemas("schema2").iterator().next();
			UpdatableSchema schema3 = db.getUpdatableSchemas("schema3").iterator().next();
			schema3.setBase(schema2);
			schema3.applyUpdates();
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void test_delete_non_existing_series() {
		try {
			UpdatableSchema schema4 = db.getUpdatableSchemas("schema4").iterator().next();
			schema4.deleteSeries(1);
			expectException();
		} catch (Exception e) {
			assertException(e, D.D30125);
		}
	}
	
	public void test_delete_series_in_actual_use_fails() {
		try {
			Schema schema = db.getSchemas("schema1a").iterator().next();
			UpdatableChronicle chro = db.getTopChronicle().edit().createChronicle("schema1achro", false, "test chronicle", null, schema);
			chro.applyUpdates();
			UpdatableSeries<Double> ser = db.getUpdatableSeries("bt.schema1achro.fou", true);
			ser.setValue(Workday.DOMAIN.time("2011-06-30"), 42.);
			ser.applyUpdates();
			// erase the series from the schema (should fail, of course)
			UpdatableSchema sch1 = schema.edit();
			sch1.deleteSeries(1);
			sch1.applyUpdates();
			expectException();
		} catch (Exception e) {
			assertException(e, null, "D30150");
		}
	}
	
	public void test_rename_series_in_actual_use_okay() {
		try {
			Schema schema = db.getSchemas("schema1a").iterator().next();
			Series<Double> ser = db.getSeries("bt.schema1achro.fou", true);
			assertEquals(42., ser.getValue(Workday.DOMAIN.time("2011-06-30")));
			// renaming the series should be okay 
			UpdatableSchema sch1 = schema.edit();
			//sch1.setSeriesName(1, "fooo"); next line should be equivalent
			sch1.setAttributeDefault(1, 1, "fooo");
			sch1.applyUpdates();
			ser = db.getSeries("bt.schema1achro.fooo", true);
			assertEquals(42., ser.getValue(Workday.DOMAIN.time("2011-06-30")));
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_modify_time_domain_of_series_fails() {
		try {
			Schema schema = db.getSchemas("schema1a").iterator().next();
			Series<Double> ser = db.getSeries("bt.schema1achro.fooo", true);
			assertEquals(42., ser.getValue(Workday.DOMAIN.time("2011-06-30")));
			// changing the time domain should fail 
			UpdatableSchema sch1 = schema.edit();
			sch1.setSeriesTimeDomain(1, DateTime.DOMAIN);
			sch1.applyUpdates();
			expectException();
		} catch (Exception e) {
			assertException(e, D.D30105, D.D30149);
		}
	}
	
	public void test_update_attribute_value_okay() {
		try {
			Schema schema = db.getSchemas("schema1a").iterator().next();
			Series<Double> ser = db.getSeries("bt.schema1achro.fooo", true);
			UpdatableSchema sch1 = schema.edit();
			sch1.addAttribute(7);
			sch1.setAttributeDefault(7, "foo");
			sch1.setAttributeProperty(7, db.getProperty("prop3", true));
			sch1.applyUpdates();

			// important: get it again
			ser = db.getSeries("bt.schema1achro.fooo", true);
			Attribute<?> sa = ser.getAttribute("prop3", false);
			assertEquals("foo", sa.get().toString());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_series_attribute_overrides_chronicle_attribute() {
		try {
			Schema schema = db.getSchemas("schema1a").iterator().next();
			UpdatableSchema sch1 = schema.edit();
			sch1.addAttribute(1, 5);
			sch1.setAttributeDefault(1, 5, "bar");
			sch1.setAttributeProperty(1, 5, db.getProperty("prop3", true));
			sch1.applyUpdates();

			Series<Double> ser = db.getSeries("bt.schema1achro.fooo", true);
			Attribute<?> sa = ser.getAttribute("prop3", false);
			assertEquals("bar", sa.get().toString());
			Attribute<?> ca = ser.getChronicle().getAttribute("prop3", false);
			assertEquals("foo", ca.get().toString());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void test_delete_series_in_use_via_base_schema() {
		try {
			Schema schema = db.getSchemas("schema4").iterator().next();
			UpdatableChronicle chro = db.getTopChronicle().edit().createChronicle("schema4chro", false, "test chronicle", null, schema);
			chro.applyUpdates();
			UpdatableSeries<Double> ser = db.getUpdatableSeries("bt.schema4chro.fooo", true);
			ser.applyUpdates();
			// note that the series is deleted in the base schema 
			UpdatableSchema sch1 = db.getUpdatableSchemas("schema1a").iterator().next();
			sch1.deleteSeries(1);
			sch1.applyUpdates();
			expectException();
		} catch (Exception e) {
			assertException(e, null, D.D30150);
		}
	}
	
	public void test_delete_and_add_attribute() {
		try {
			UpdatableSchema schema = db.getUpdatableSchemas("schema1a").iterator().next();
			schema.deleteAttribute(1);
			schema.applyUpdates();
			assertEquals(1, schema.getAttributeDefinitions().size());
			// put it back we still need it
			schema.addAttribute(1);
			schema.setAttributeProperty(1, db.getProperty("prop1", true));
			schema.setAttributeDefault(1, "t1v1");
			schema.applyUpdates();
			assertEquals(2, schema.getAttributeDefinitions().size());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test_delete_chronicle_attribute_with_actual_values() {
		try {
			UpdatableSchema schema = db.getUpdatableSchemas("schema1a").iterator().next();

			// use the attribute
			UpdatableChronicle chro = db.getChronicle("bt.schema1achro", true).edit();
			assertEquals("t1v1", chro.getAttribute("prop1", true).get().toString());
			Attribute<?> a = chro.getAttribute("prop1", true);
			a.scan("t1v2");
			assertEquals("t1v2", a.get().toString());
			chro.setAttribute(a);
			chro.applyUpdates();
			assertEquals("t1v2", chro.getAttribute("prop1", true).get().toString());
			schema.deleteAttribute(1);
			schema.applyUpdates();
			expectException();
		} catch (Exception e) {
			assertException(e, null, D.D30146);
		}
	}
	
	public void test_delete_chronicle_attribute_via_base_with_actual_values() {
		try {
			Schema schema3 = db.getSchemas("schema3").iterator().next();
			UpdatableChronicle chro = db.getTopChronicle().edit().createChronicle("schema3chro", false, "test chronicle", null, schema3);
			chro.applyUpdates();
			// use the attribute
			Attribute<?> a = chro.getAttribute("prop1", true);
			a.scan("t1v3");
			assertEquals("t1v3", a.get().toString());
			chro.setAttribute(a);
			chro.applyUpdates();
			assertEquals("t1v3", chro.getAttribute("prop1", true).get().toString());
			// remove the attribute from the base schema of schema3 (should fail)
			UpdatableSchema schema1 = db.getUpdatableSchemas("schema1a").iterator().next();
			schema1.deleteAttribute(1);
			schema1.applyUpdates();
			expectException();
		} catch (Exception e) {
			assertException(e, null, D.D30146);
		}
	}
	
	public void test_erase_attribute_in_use() {
		try {
			// erase attribute 
			Chronicle chro = db.getChronicle("bt.schema3chro", true);
			assertEquals("t1v3", chro.getAttribute("prop1", true).get().toString());
			UpdatableSchema schema3 = db.getUpdatableSchemas("schema3").iterator().next();
			schema3.addAttribute(1);
			schema3.eraseAttribute(1);
			schema3.applyUpdates();
			assertEquals(null, schema3.getAttributeDefinition("prop1", false));
			expectException();
		} catch (Exception e) {
			assertException(e, D.D30105, D.D30146);
		}
	}

	public void test_erase_attribute_still_in_use() {
		try {
			// erase attribute 
			UpdatableChronicle chro = db.getChronicle("bt.schema3chro", true).edit();
			assertEquals("t1v3", chro.getAttribute("prop1", true).get().toString());
			// use the attribute
			Attribute<?> a = chro.getAttribute("prop1", true);
			a.reset();
			chro.setAttribute(a);
			chro.applyUpdates();
			assertEquals("t1v2", chro.getAttribute("prop1", true).get().toString());
			
			UpdatableSchema schema3 = db.getUpdatableSchemas("schema3").iterator().next();
			schema3.addAttribute(1);
			schema3.eraseAttribute(1);
			schema3.applyUpdates();
			assertEquals(null, schema3.getAttributeDefinition("prop1", false));
			expectException();
		} catch (Exception e) {
			assertException(e, null, D.D30146);
		}
	}
	
	public void test_delete_schema_in_use() {
		try {
			UpdatableSchema schema1 = db.getSchemas("schema1a").iterator().next().edit();
			schema1.destroy();
			schema1.applyUpdates();
			expectException();
		} catch (Exception e) {
			assertException(e, D.D30140);
		}
	}
	
}
