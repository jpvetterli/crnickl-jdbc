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
 */
package ch.agent.crnickl.jdbc;

import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.impl.DatabaseBackend;
import ch.agent.crnickl.impl.SurrogateImpl;

/**
 * JDBCSurrogate is the JDBC variant of Surrogate.
 * JDBC databases provide an int32 as the id of inserted rows.
 * 
 * @author Jean-Paul Vetterli
 *
 */
public class JDBCSurrogate extends SurrogateImpl {

	/**
	 * Construct a surrogate using an int32 object id.
	 * 
	 * @param db the database
	 * @param dot the database object type
	 * @param id the object id, which must be a positive integer
	 */
	public JDBCSurrogate(DatabaseBackend db, DBObjectType dot, int id) {
		super(db, dot, new JDBCObjectId(id));
	}

}
