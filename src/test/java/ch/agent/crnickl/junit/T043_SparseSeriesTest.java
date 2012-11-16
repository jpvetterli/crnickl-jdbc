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
 * Type: T043_SparseSeriesTest
 * Version: 1.0.1
 */
package ch.agent.crnickl.junit;

public class T043_SparseSeriesTest extends T042_SeriesValuesTest {

	protected static String SCHEMA = "t043";
	
	@Override
	protected boolean isSparse() {
		return true;
	}

	
	
}