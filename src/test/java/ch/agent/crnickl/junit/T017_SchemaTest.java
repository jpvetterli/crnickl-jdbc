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

import junit.framework.TestCase;
import ch.agent.core.KeyedException;
import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.api.Attribute;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Schema;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableProperty;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.t2.time.Workday;

public class T017_SchemaTest extends TestCase {

	private Database db;
	
	@Override
	protected void setUp() throws Exception {
		db = DBSetUp.getDatabase();
	}

	public void test01_setup() {
		try {
			// create 2 value types
			UpdatableValueType<String> vt1 = db.createValueType("type1", true, "TEXT");
			vt1.addValue(vt1.getScanner().scan("t1v1"), "type1 value1");
			vt1.addValue(vt1.getScanner().scan("t1v2"), "type1 value2");
			vt1.addValue(vt1.getScanner().scan("t1v3"), "type1 value3");
			vt1.applyUpdates();
			UpdatableValueType<String> vt2 = db.createValueType("type2", true, "TEXT");
			vt2.addValue(vt2.getScanner().scan("t2v2"), "type2 value2");
			vt2.applyUpdates();

			// create 2 properties
			db.createProperty("prop1", vt1, false).applyUpdates();
			db.createProperty("prop2", vt2, false).applyUpdates();
		} catch (T2DBException e) {
			fail(e.getMessage());
		}
	}
	
	public void test11_create_schema() {
		try {
			UpdatableSchema schema = db.createSchema("schema1", null);
			schema.addAttribute(1);
			schema.setAttributeProperty(1, db.getProperty("prop1", true));
			schema.setAttributeDefault(1, "t1v1");
			schema.setAttributeProperty(1, db.getProperty("prop2", true));
			schema.applyUpdates();
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D30133, e.getMsg().getKey());
		}
	}

	public void test12_create_schema() {
		try {
			UpdatableSchema schema = db.createSchema("schema1", null);
			schema.addAttribute(1);
			schema.setAttributeProperty(1, db.getProperty("prop1", true));
			schema.setAttributeDefault(1, "t1v1");
			schema.addAttribute(1);
			schema.applyUpdates();
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D30127, e.getMsg().getKey());
		}
	}
	
	public void test13_create_schema() {
		try {
			/*
			 * why commit/rollback ?
			 * commit: because subsequent tests need properties etc. created previously
			 * rollback: because this test would leave an incomplete schema1 behind
			 */
			db.commit();
			UpdatableSchema schema = db.createSchema("schema1", null);
			schema.addAttribute(1);
			schema.setAttributeProperty(1, db.getProperty("prop1", true));
			schema.setAttributeDefault(1, "t1v1");
			schema.deleteAttribute(1);
			schema.addAttribute(1);
			schema.applyUpdates();
			fail("exception expected");
		} catch (Exception e) {
			assertEquals("J30124",  ((KeyedException)e.getCause()).getMsg().getKey());
			try {
				db.rollback();
				Collection<Schema> ss = db.getSchemas("schema1");
				assertEquals(0,  ss.size());
			} catch (Exception e1) {
			}
		}
	}

	/**
	 * Bug 20120626.1: cannot extend a schema on existing component.
	 * The bug appears when schema2.applyUpdates() tries to merge. 
	 */
	public void test14_create_schema() {
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
	
	public void test15_create_schema() {
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
	
	public void test18_edit_schema() {
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

	public void test30_edit_schema() {
		try {
			UpdatableSchema schema1 = db.getSchemas("schema1").iterator().next().edit();
			schema1.setName("schema1a");
			schema1.applyUpdates();
			assertEquals(1, db.getSchemas("schema1a").size());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test31_edit_schema() {
		try {
			
			UpdatableSchema schema3 = db.createSchema("schema3", null);
//			schema3.addAttribute(1);
//			schema3.setAttributeProperty(1, db.getProperty("prop1", true));
//			schema3.setAttributeDefault(1, "t1v1");
			schema3.applyUpdates();
			
			UpdatableSchema schema1 = db.getSchemas("schema1a").iterator().next().edit();
			schema1.setBase(schema3);
			schema1.applyUpdates();
			assertEquals(schema3, schema1.getBase());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test32_edit_schema() {
		try {
			UpdatableSchema schema1 = db.getSchemas("schema1a").iterator().next().edit();
			schema1.setBase(null);
			schema1.applyUpdates();
			assertEquals(null, schema1.getBase());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test33_edit_schema() {
		try {
			UpdatableSchema schema4 = db.createSchema("schema4", null);
			schema4.addAttribute(1);
			schema4.setAttributeProperty(1, db.getProperty("prop1", true));
			schema4.setAttributeDefault(1, "t1v1");
//			schema4.applyUpdates();
			UpdatableSchema schema1 = db.getSchemas("schema1a").iterator().next().edit();
			schema1.setBase(schema4);
			fail("exception expected");
		} catch (KeyedException e) {
			assertEquals("D30128",  e.getMsg().getKey());
		}
	}

	public void test34_edit_schema() {
		UpdatableSchema schema1 = null;
		try {
			UpdatableSchema schema4 = db.createSchema("schema4", "schema1a");
			Schema s = schema4.resolve();
			assertEquals("prop1", s.getAttributeDefinition(1, true).getName());
			schema4.addAttribute(1);
			schema4.eraseAttribute(1);
			schema4.applyUpdates();
			s = schema4.resolve();
			assertEquals(null, s.getAttributeDefinition(1, false));
			assertEquals(true, schema4.getAttributeDefinition(1, true).isErasing());
			assertEquals("fou", s.getSeriesDefinition(1, true).getName());
			schema4.setAttributeProperty(1, db.getProperty("prop1", true));
			schema4.setAttributeDefault(1, "t1v1");
			schema4.applyUpdates();
			schema1 = db.getSchemas("schema1a").iterator().next().edit();
			schema1.setBase(schema4);
			schema1.applyUpdates();
			fail("exception expected");
		} catch (Exception e) {
			assertEquals("D30110",  ((KeyedException) e).getMsg().getKey());
		}
	}
	
	public void test35_edit_schema() {
		try {
			UpdatableSchema schema4 = db.getUpdatableSchemas("schema4").iterator().next();
			Schema s = schema4.resolve();
			assertEquals("fou", s.getSeriesDefinition(1, true).getName());
			schema4.addSeries(1);
			schema4.eraseSeries(1);
			schema4.applyUpdates();
			s = schema4.resolve();
			assertEquals(null, s.getSeriesDefinition(1, false));
			Schema s4 = db.getSchemas("schema4").iterator().next();
			assertEquals(null, s4.getSeriesDefinition(1, false));
		} catch (KeyedException e) {
			fail(e.getMessage());
		}
	}
	
	public void test35B_edit_schema() {
		// related to test37
		try {
			UpdatableSchema schema2 = db.getUpdatableSchemas("schema2").iterator().next();
			UpdatableSchema schema3 = db.getUpdatableSchemas("schema3").iterator().next();
			schema3.setBase(schema2);
			schema3.applyUpdates();
		} catch (KeyedException e) {
			fail(e.getMessage());
		}
	}

	/**
	 * Delete a series. In this case delete an erasing series.
	 */
	public void test35C_edit_schema() {
		try {
			UpdatableSchema schema4 = db.getUpdatableSchemas("schema4").iterator().next();
			schema4.deleteSeries(1);
			schema4.applyUpdates();
			Schema s = schema4.resolve();
			assertEquals("fou", s.getSeriesDefinition(1, false).getName());
			Schema s4 = db.getSchemas("schema4").iterator().next();
			assertEquals("fou", s4.getSeriesDefinition(1, false).getName());
		} catch (KeyedException e) {
			fail(e.getMessage());
		}
	}
	
	public void test35D_edit_schema() {
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
			fail("exception expected");
		} catch (KeyedException e) {
			assertEquals("D30150",  ((KeyedException) e).getMsg().getKey());
		}
	}

	public void test35E_edit_schema() {
		try {
			Schema schema = db.getSchemas("schema4").iterator().next();
			UpdatableChronicle chro = db.getTopChronicle().edit().createChronicle("schema4chro", false, "test chronicle", null, schema);
			chro.applyUpdates();
			UpdatableSeries<Double> ser = db.getUpdatableSeries("bt.schema4chro.fou", true);
			ser.applyUpdates();
			// note that the series is deleted in the base schema 
			UpdatableSchema sch1 = db.getUpdatableSchemas("schema1a").iterator().next();
			sch1.deleteSeries(1);
			sch1.applyUpdates();
			fail("exception expected");
		} catch (KeyedException e) {
			assertEquals("D30150",  ((KeyedException) e).getMsg().getKey());
		}
	}
	
	public void test35F_edit_schema() {
		try {
			UpdatableSchema schema4 = db.getUpdatableSchemas("schema4").iterator().next();
			// note that the series is erased 
			schema4.addSeries(1);
			schema4.eraseSeries(1);
			schema4.applyUpdates();
			fail("exception expected");
		} catch (KeyedException e) {
			assertEquals("D30150",  ((KeyedException) e).getMsg().getKey());
		}
	}

	public void test35G_edit_schema() {
		try {
			UpdatableSchema schema = db.getUpdatableSchemas("schema1a").iterator().next();
			schema.deleteAttribute(1);
			schema.applyUpdates();
			assertEquals(0, schema.getAttributeDefinitions().size());
			// put it back we still need it
			schema.addAttribute(1);
			schema.setAttributeProperty(1, db.getProperty("prop1", true));
			schema.setAttributeDefault(1, "t1v1");
			schema.applyUpdates();
			assertEquals(1, schema.getAttributeDefinitions().size());
		} catch (KeyedException e) {
			fail(e.getMessage());
		}
	}
	
	public void test35H_edit_schema() {
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
			fail("exception expected");
		} catch (KeyedException e) {
			assertEquals("D30146",  ((KeyedException) e).getMsg().getKey());
		}
	}
	
	public void test35I_edit_schema() {
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
			fail("exception expected");
		} catch (KeyedException e) {
			assertEquals("D30146",  ((KeyedException) e).getMsg().getKey());
		}
	}
	public void test35J_edit_schema() {
		try {
			// erase attribute 
			Chronicle chro = db.getChronicle("bt.schema3chro", true);
			assertEquals("t1v3", chro.getAttribute("prop1", true).get().toString());
			
			UpdatableSchema schema3 = db.getUpdatableSchemas("schema3").iterator().next();
			schema3.addAttribute(1);
			schema3.eraseAttribute(1);
			schema3.applyUpdates();
			fail("exception expected");
		} catch (KeyedException e) {
			assertEquals("D30146",  ((KeyedException) e).getMsg().getKey());
		}
	}

	public void test36_delete_schema() {
		try {
			UpdatableSchema schema1 = db.getSchemas("schema1a").iterator().next().edit();
			schema1.destroy();
			schema1.applyUpdates();
			fail("exception expected");
		} catch (Exception e) {
			assertEquals(D.D30140, ((KeyedException)e).getMsg().getKey());
		}
	}
	
	public void test37_delete_schema() {
		try {
			deleteChron("bt.schema1achro", "bt.schema4chro", "bt.schema3chro");

			int count = 1;
			Collection<UpdatableSchema> schemas = null;
			boolean retry = true;
			while (retry) {
				retry = false;
				schemas = db.getUpdatableSchemas("schema*");
				for (UpdatableSchema schema : schemas) {
					try {
						schema.destroy();
						schema.applyUpdates();
					} catch (Exception e) {
						// loop a few times to get schema dependencies
						if (count++ > 6) {
							throw new RuntimeException(e);
						}
						retry = true;
					}
				}
			}
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	public void test99_cleanup() {
		try {
			deleteProp("prop1", "prop2");
			deleteVT("type1", "type2");
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	private void deleteChron(String... chrons) throws Exception {
		for (String chron : chrons) {
			UpdatableChronicle chro = db.getChronicle(chron, true).edit();
			for (Series<?> s : chro.getSeries()) {
				UpdatableSeries<?> us = s.edit();
				us.setRange(null);
				us.applyUpdates();
				us.destroy();
				us.applyUpdates();
			}
			chro.destroy();
			chro.applyUpdates();
		}
	}
	
	private void deleteProp(String... props) throws Exception {
		for (String prop : props) {
			UpdatableProperty<?> p = db.getProperty(prop, true).edit();
			p.destroy();
			p.applyUpdates();
		}
	}
	
	private void deleteVT(String... vts) throws Exception {
		for (String vt : vts) {
			UpdatableValueType<?> v = db.getValueType(vt).edit();
			v.destroy();
			v.applyUpdates();
		}
	}
	
}
