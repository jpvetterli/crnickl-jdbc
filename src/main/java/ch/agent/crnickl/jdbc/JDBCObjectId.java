/*
 *   Copyright 2012-2013 Hauser Olsson GmbH
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

import ch.agent.crnickl.api.DBObjectId;

/**
 * JDBCObjectId is the JDBC variant of DBObjectId.
 * JDBC databases provide an int32 as the id of inserted rows.
 * 
 * @author Jean-Paul Vetterli
 *
 */
public class JDBCObjectId implements DBObjectId {

	private int id;

	/**
	 * Construct an object id from a positive integer.
	 * 
	 * @param id a positive integer
	 */
	public JDBCObjectId(int id) {
		if (id < 1)
			throw new IllegalArgumentException("id < 1");
		this.id = id;
	}
	
	/**
	 * Return the JDBC object id.
	 * 
	 * @return a positive integer
	 */
	public int value() {
		return id;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		JDBCObjectId other = (JDBCObjectId) obj;
		if (id != other.id)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "" + id;
	}

}
