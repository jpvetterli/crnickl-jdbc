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
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.jdbc.JDBCDatabaseMethods;

public class SpecialMethodsForChronicles extends JDBCDatabaseMethods {
	
	public SpecialMethodsForChronicles() throws KeyedException {
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
	public int[] deleteChronicleCollection(Chronicle chronicle) throws KeyedException {
		return deleteChronicle(chronicle, true);
	}
	
	private int[] deleteChronicle(Chronicle chronicle, boolean top) throws KeyedException {
		int ecount = 0;
		int scount = 0;
		for (Series<?> s : chronicle.getSeries()) {
			UpdatableSeries<?> us = s.edit();
			us.setRange(null);
			us.applyUpdates();
			us.destroy();
			us.applyUpdates();
			scount++;
		}
		for (Chronicle e : chronicle.getMembers()) {
			int[] counts = deleteChronicle(e, false);
			ecount += counts[0];
			scount += counts[1];
		}
		if (!top) {
			UpdatableChronicle ue = chronicle.edit();
			ue.destroy();
			ue.applyUpdates();
			ecount++;
		}
		return new int[] { ecount, scount };
	}

}
