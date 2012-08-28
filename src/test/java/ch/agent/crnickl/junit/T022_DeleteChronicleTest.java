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
 * Type: T022_DeleteChronicleTest
 * Version: 1.0.1
 */
package ch.agent.crnickl.junit;

import junit.framework.TestCase;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.UpdatableChronicle;

public class T022_DeleteChronicleTest extends TestCase {

	private Database db;

	@Override
	protected void setUp() throws Exception {
		db = DBSetUp.getDatabase();
	}
	
	public void test01_delete_entity() {
		try {
			UpdatableChronicle ent = db.getChronicle("bt.fooent", true).edit();
			new SpecialMethodsForChronicles().deleteChronicleCollection(ent);
			ent.destroy();
			ent.applyUpdates();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	
}
