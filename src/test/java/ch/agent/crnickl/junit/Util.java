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
 * Type: SpecialMethodsForChronicles
 * Version: 1.0.1
 */
package ch.agent.crnickl.junit;

import ch.agent.core.KeyedException;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Schema;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableProperty;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.api.UpdatableValueType;

public class Util {
	
	private Util() {
	}

	/**
	 * Delete everything belonging to the entity, except the entity itself. This
	 * deletes all entities, series, values, and their dependent objects. The
	 * method returns an array with the number of entities and the number of
	 * series deleted. The method does not commit.
	 * 
	 * @param chronicle
	 * @return
	 * @throws KeyedException
	 */
	public static void deleteChronicleCollection(Chronicle chronicle) throws KeyedException {
		deleteChronicle(chronicle, true);
	}
	
	private static void deleteChronicle(Chronicle chronicle, boolean keepTop) throws KeyedException {
		for (Series<?> s : chronicle.getSeries()) {
			UpdatableSeries<?> us = s.edit();
			us.setRange(null);
			us.applyUpdates();
			us.destroy();
			us.applyUpdates();
		}
		for (Chronicle e : chronicle.getMembers()) {
			deleteChronicle(e, false);
		}
		if (!keepTop) {
			UpdatableChronicle ue = chronicle.edit();
			ue.destroy();
			ue.applyUpdates();
		}
	}

	public static void deleteChronicles(Database db, String... chrons) throws Exception {
		for (String chron : chrons) {
			Chronicle chronicle = db.getChronicle(chron, false);
			if (chronicle != null)
				deleteChronicle(db.getChronicle(chron, true), false);
		}
	}
	
	public static void deleteProperties(Database db, String... props) throws Exception {
		for (String prop : props) {
			UpdatableProperty<?> p = db.getProperty(prop, true).edit();
			p.destroy();
			p.applyUpdates();
		}
	}
	
	public static void deleteValueTypes(Database db, String... vts) throws Exception {
		for (String vt : vts) {
			UpdatableValueType<?> v = db.getValueType(vt).edit();
			v.destroy();
			v.applyUpdates();
		}
	}

	public static void deleteSchema(Database db, String... schemas) throws Exception {
		for (String schema : schemas) {
			for (Schema s : db.getSchemas(schema)) {
				UpdatableSchema usch = s.edit();
				usch.destroy();
				usch.applyUpdates();
			}
		}
	}
	
	
}
