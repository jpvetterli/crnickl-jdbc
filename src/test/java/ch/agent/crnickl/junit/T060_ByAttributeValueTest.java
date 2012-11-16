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
 * Type: T060_ByAttributeValueTest
 * Version: 1.0.0
 */
package ch.agent.crnickl.junit;

import java.util.List;

import ch.agent.crnickl.api.Attribute;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.Schema;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableProperty;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.crnickl.impl.DatabaseBackend;
import ch.agent.t2.time.Day;

public class T060_ByAttributeValueTest extends AbstractTest {

	private static DatabaseBackend db;
	
	@Override
	protected void firstSetUp() throws Exception {
		db = (DatabaseBackend) DBSetUp.getDatabase();
		setup1();
		setup2();
	}
	
	@Override
	protected void lastTearDown() throws Exception {
		Util.deleteChronicles(db, "bt.sm");
		Util.deleteSchema(db, "Stocks");
		Util.deleteProperties(db, "Ticker");
		Util.deleteValueTypes(db, "Ticker");
	}

	private void setup1() throws Exception {
		// create "Ticker" value type
		UpdatableValueType<String> vt = db.createValueType("Ticker", false, "TEXT");
		vt.applyUpdates();
			
		// create "Ticker" property
		UpdatableProperty<String> p = db.createProperty("Ticker", vt, true);
		p.applyUpdates();
			
		// create "Stocks" schema, with a "Ticker" property and a "price" series
		UpdatableSchema schema = db.createSchema("Stocks", null);
		schema.addAttribute(2);
		schema.setAttributeProperty(2, p);
		schema.addSeries(1);
		schema.setSeriesName(1, "price");
		schema.setSeriesType(1, "numeric");
		schema.setSeriesTimeDomain(1, Day.DOMAIN);
		schema.applyUpdates();
			
		db.commit();
	}

	private void setup2() throws Exception {
		Schema stocks = db.getSchemas("Stocks").iterator().next();
		UpdatableChronicle sm = db.getTopChronicle().edit().createChronicle("sm", false, "Stock markets", null, stocks);
		sm.applyUpdates();

		UpdatableChronicle ch = sm.createChronicle("ch", false, "CH", null, null);
		ch.applyUpdates();
			
		UpdatableChronicle chsun = ch.createChronicle("sunxyzzy", false, "ch's sun xyzzy", null, null);
		Attribute<?> ticker = chsun.getAttribute("Ticker", true);
		ticker.scan("SUN");
		chsun.setAttribute(ticker);
		chsun.applyUpdates();
			
		UpdatableChronicle us = sm.createChronicle("us", false, "US", null, null);
		us.applyUpdates();
			
		UpdatableChronicle ussun = us.createChronicle("sungobdigook", false, "us's sun gobbledygook", null, null);
		ussun.setAttribute(ticker);
		ussun.applyUpdates();
		
		UpdatableChronicle ussun2 = us.createChronicle("sunco", false, "another us sun", null, null);
		ticker.scan("SUN2");
		ussun2.setAttribute(ticker);
		ussun2.applyUpdates();

		db.commit();
	}
	
	public void test1() {
		try {
			@SuppressWarnings("rawtypes")
			Property ticker = db.getProperty("Ticker", true);
			@SuppressWarnings("unchecked")
			List<Chronicle> result = ticker.getChronicles(ticker.scan("SUN"), 42);
			assertEquals(2, result.size());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
}
