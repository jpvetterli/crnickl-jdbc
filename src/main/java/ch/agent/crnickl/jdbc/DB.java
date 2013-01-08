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
 * 
 */
package ch.agent.crnickl.jdbc;

/**
 * Provides constants for table names. Makes it easier to find where tables are accessed.
 * 
 * @author Jean-Paul Vetterli
 */
public interface DB {

	static final String CHRONICLE = "chronicle";
	static final String SERIES = "series";
	static final String VALUE_DOUBLE = "value_double";
	static final String ATTRIBUTE_VALUE = "attribute_value";
	static final String SCHEMA_NAME = "schema_name";
	static final String SCHEMA_ITEM = "schema_item";
	static final String PROPERTY = "property";
	static final String VALUE_TYPE = "value_type";
	static final String VALUE_TYPE_VALUE = "value_type_value";

}
