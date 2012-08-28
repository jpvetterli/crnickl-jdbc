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
 * Type: T045_EventTest
 * Version: 1.0.1
 */
package ch.agent.crnickl.junit;

import junit.framework.TestCase;
import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdateEvent;
import ch.agent.crnickl.api.UpdateEventSubscriber;

public class T045_EventTest extends TestCase {
	
	private class EventTester implements UpdateEventSubscriber {
		private UpdateEvent event;
		@Override
		public void notify(UpdateEvent event) {
			this.event = event;
		}
		public UpdateEvent getEvent() {
			return event;
		}
	}
	
	private Database db;
	private static EventTester tester;
	
	@Override
	protected void setUp() throws Exception {
		db = DBSetUp.getDatabase();
		if (tester == null) {
			tester = new EventTester();
			db.getUpdateEventPublisher().subscribe(tester, DBObjectType.CHRONICLE, false);
		}
	}
	
	public void test001() {
		try {
			UpdatableChronicle e = db.getTopChronicle().edit().createChronicle(DBSetUp.SIMPLE_NAME, false, "junit test 001", null, null);
			assertEquals(DBSetUp.BASE_NAME, e.getName(true));
			e.applyUpdates();
			db.commit();
			assertEquals(DBSetUp.BASE_NAME, ((Chronicle) tester.getEvent().getSourceOrNull()).getName(true));
		} catch (T2DBException e) {
			fail(e.toString());
		}
	}
	
	public void test002() {
		try {
			UpdatableChronicle e = db.getChronicle(DBSetUp.BASE_NAME, true).edit();
			e.destroy();
			e.applyUpdates();
			db.commit();
			assertEquals(db.getNamingPolicy().joinValueAndDescription(DBSetUp.BASE_NAME, "junit test 001"), tester.getEvent().getComment());
		} catch (Exception e) {
			fail(e.toString());
		}
	}


}

