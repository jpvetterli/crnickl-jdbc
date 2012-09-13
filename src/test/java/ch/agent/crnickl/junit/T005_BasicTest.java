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
 * Type: T005_BasicTest
 * Version: 1.0.0
 */
package ch.agent.crnickl.junit;

import junit.framework.TestCase;
import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.crnickl.impl.DatabaseBackend;
import ch.agent.crnickl.impl.SurrogateImpl;
import ch.agent.crnickl.jdbc.JDBCObjectId;
import ch.agent.t2.time.TimeDomain;

public class T005_BasicTest extends TestCase {

	private Database db;
	private static boolean clean;
	private static final String BASE = "bt.basictest";
	private static final String ENTITY = "bt.basictest.test";
	
	private JDBCObjectId id(int id) {
		return new JDBCObjectId(id);
	}
	
	@Override
	protected void setUp() throws Exception {
		db = DBSetUp.getDatabase();
		if (!clean) {
			Chronicle basic = db.getChronicle(BASE, false);
			if (basic == null) {
				String[] split = db.getNamingPolicy().split(BASE);
				Chronicle base = db.getChronicle(split[0], true);
				base.edit().createChronicle(split[1], false, "Unit tests", null, null).applyUpdates();
			} else
				new SpecialMethodsForChronicles().deleteChronicleCollection(basic);
//			db.commit();
			clean = true;
		}
	}

	public void test1() {
		try {
			Chronicle e = db.getChronicle(BASE, true);
			assertEquals(BASE, e.getName(true));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public void test2() {
		try {
			db.getChronicle("foo.bar", true);
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D40103, e.getMsg().getKey());
		}
	}

	public void test3() {
		try {
			Chronicle e1 = db.getChronicle(BASE, true);
			Chronicle e2 = db.getChronicle(e1.getSurrogate());
			assertEquals(e1, e2);
		} catch (T2DBException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public void test4() {
		try {
			Chronicle e1 = db.getChronicle(BASE, true);
			Chronicle e2 = db.getChronicle(e1.getSurrogate());
			assertEquals(e1.getSchema(true), e2.getSchema(true));
		} catch (T2DBException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public void test5() {
		try {
			Chronicle e1 = db.getChronicle(BASE, true);
			Chronicle e2 = db.getChronicle(e1.getSurrogate());
			assertEquals(e1.getCollection(), e2.getCollection());
		} catch (T2DBException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public void test6() {
		try {
			Chronicle e = db.getChronicle(db.getTopChronicle().getName(true), true);
			e.getAttribute("Currency", true);
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D40101, e.getMsg().getKey());
		}
	}

	public void test7() {
		try {
			Chronicle e = db.getChronicle(db.getTopChronicle().getName(true), true);
			assertNull(e.getSurrogate().getObject());
		} catch (T2DBException e) {
			fail(e.toString());
		}
	}
	
	/**
	 * Bug 4423 
	 * 2012-03-16 
	 */
	public void test8() {
		try {
			Chronicle e1 = db.getChronicle(db.getTopChronicle().getName(true), true);
			db.getChronicle(e1.getSurrogate());
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D02102, e.getMsg().getKey());
		}
	}
	
	public void test9() {
		try {
			Chronicle e = db.getChronicle(BASE, true);
			e.getAttribute("Currency", true);
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D40114, e.getMsg().getKey());
		}
	}
	
	public void test10() {
		try {
			String[] split = db.getNamingPolicy().split(ENTITY);
			Chronicle base = db.getChronicle(split[0], true);
			base.edit().createChronicle(split[1], false, "test entity", null, null).applyUpdates();
//			db.commit();
			assertEquals(ENTITY, db.getChronicle(ENTITY, true).getName(true));
		} catch (T2DBException e) {
			fail(e.getMessage());
		}
	}
	public void test11() {
		try {
			Chronicle e1 = db.getChronicle(ENTITY, true);
			Chronicle e2 = db.getChronicle(e1.getSurrogate());
			assertEquals(e1, e2);
		} catch (T2DBException e) {
			fail(e.getMessage());
		}
	}
	
	public void test12() {
		try {
			Chronicle e1 = db.getChronicle(ENTITY, true);
			Chronicle e2 = db.getChronicle(e1.getSurrogate());
			assertEquals(e1.getSchema(true), e2.getSchema(true));
		} catch (T2DBException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public void test13() {
		try {
			Chronicle e1 = db.getChronicle(ENTITY, true);
			Chronicle e2 = db.getChronicle(e1.getSurrogate());
			assertEquals(e1.getCollection(), e2.getCollection());
		} catch (T2DBException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	/**
	 * Bug 4424 2013-03-16
	 */
	public void test14() {
		try {
			Surrogate k = new SurrogateImpl((DatabaseBackend)db, DBObjectType.CHRONICLE, id(42));
			db.getSeries(k);
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D02102, e.getMsg().getKey());
		}
	}

	/**
	 * Bug 4424 2013-03-16
	 */
	public void test15() {
		try {
			Surrogate k = new SurrogateImpl((DatabaseBackend)db, DBObjectType.CHRONICLE, id(42));
			db.getSchema(k);
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D02102, e.getMsg().getKey());
		}
	}

	/**
	 * Bug 4424 2013-03-16
	 */
	public void test16() {
		try {
			Surrogate k = new SurrogateImpl((DatabaseBackend)db, DBObjectType.CHRONICLE, id(42));
			db.getProperty(k);
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D02102, e.getMsg().getKey());
		}
	}

	/**
	 * Bug 4424 2013-03-16
	 */
	public void test17() {
		try {
			Surrogate k = new SurrogateImpl((DatabaseBackend)db, DBObjectType.CHRONICLE, id(42));
			db.getValueType(k);
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D02102, e.getMsg().getKey());
		}
	}

	/**
	 * Bug 4424 2013-03-16
	 */
	public void test18() {
		try {
			Surrogate k = new SurrogateImpl((DatabaseBackend)db, DBObjectType.SCHEMA, id(42));
			db.getChronicle(k);
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D02102, e.getMsg().getKey());
		}
	}
	
	/**
	 * Bug 4425 2012-03-16
	 * 
	 * Verify built-in value types and properties.
	 */
	public void test19() {
		try {
			Property<?> nameProp = db.getProperty(new SurrogateImpl((DatabaseBackend) db, DBObjectType.PROPERTY, id(1)));
			assertEquals("NAME", nameProp.getValueType().getExternalRepresentation());
			Property<?> typeProp = db.getProperty(new SurrogateImpl((DatabaseBackend) db, DBObjectType.PROPERTY, id(2)));
			assertEquals("TYPE", typeProp.getValueType().getExternalRepresentation());
			Property<?> tdProp = db.getProperty(new SurrogateImpl((DatabaseBackend) db, DBObjectType.PROPERTY, id(3)));
			assertEquals("TIMEDOMAIN", tdProp.getValueType().getExternalRepresentation());
			assertFalse(nameProp.getValueType().isRestricted());
			assertTrue(tdProp.getValueType().getValues().size() >= 5);
			if (DBSetUp.inMemory())
				assertEquals(1, typeProp.getValueType().getValues().size()); // "number" defined in the DDL
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.toString());
		}
	}
	
	public <S>void test20() {
		try {
			Surrogate k = new SurrogateImpl((DatabaseBackend) db, DBObjectType.VALUE_TYPE, id(2));
			@SuppressWarnings("unchecked")
			UpdatableValueType<S> vt = (UpdatableValueType<S>) db.getValueType(k).edit();
			vt.addValue(vt.scan("foo"), null);
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D10107, e.getMsg().getKey());
		}
	}
	
	public void test21() {
		try {
			Surrogate k = new SurrogateImpl((DatabaseBackend) db, DBObjectType.VALUE_TYPE, id(3));
			UpdatableValueType<TimeDomain> vt = db.getValueType(k).typeCheck(TimeDomain.class).edit();
			vt.updateValue(vt.scan("daily"), "new daily");
		} catch (T2DBException e) {
			fail(e.toString());
		}
	}
	
	public void test22() {
		try {
			Surrogate k = new SurrogateImpl((DatabaseBackend) db, DBObjectType.VALUE_TYPE, id(2));
			UpdatableValueType<TimeDomain> vt = db.getValueType(k).typeCheck(TimeDomain.class).edit();
			vt.updateValue(vt.scan("daily"), "new daily");
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D10101, e.getMsg().getKey());
		}
	}

}
