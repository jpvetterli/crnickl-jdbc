crnickl-jdbc : JDBC Implementation of the CrNiCKL Database 
==========================================================

	Copyright 2012 Hauser Olsson GmbH.
	
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
    	http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.

*** 

This is a JDBC implementation of the CrNiCKL (chronicle) database. 
CrNiCKL (pronounced "chronicle") is a database for time series written in 
Java running on top of SQL and (soon) NoSQL systems.

Distribution
------------

Starting with version 1.1.1, the distribution consists of a binary JAR with 
compiled classes and of a source JAR:


	crnickl-jdbc-<version>.jar
	crnickl-jdbc-<version>-sources.jar

In the file names `<version>` stands of course for the actual version,
`1.1.1` for example. For earlier versions, the suffix of the source JAR 
is `.source` instead of `-sources`.    

Dependencies
------------

The software is built with maven; dependencies are defined in the <q>POM</q>
file, included in the binary JAR:

	/META-INF/maven/ch.agent/crnickl-jdbc/pom.xml

Building the software
---------------------

The recommended way is to use [git](http://git-scm.com) for accessing the
source and [maven](<http://maven.apache.org/>) for building. The procedure 
is easy, as maven takes care of locating and downloading dependencies:

	$ git clone https://github.com/jpvetterli/crnickl-jdbc.git
	$ cd crnickl-jdbc
	$ mvn install

This builds and installs the distribution JARs in your local maven
repository. They can also be found in the `target` directory.

When building the software by other means, the following dependencies must be
addressed:

- `crnickl-<version>.jar` [CrNiCKL database](http://agent.ch/timeseries/crnickl/)
- `t2-<version>.jar` [Time2 Library](http://agent.ch/timeseries/t2/)  

Versions numbers can be found in the <q>POM</q> file mentionned previously. 

Unit tests
----------

The following command runs unit tests:

	$ mvn -Dmaven.test.skip=false test

When building the software without maven, the following 
additional dependencies must be addressed to compile unit tests:

- `hsqldb-<version>.jar` [HyperSQL Database](http://hsqldb.org/)
- `sqltool-<version>.jar` [HyperSQL Database](http://hsqldb.org/)

Generating the documentation
----------------------------

If you are using maven, you can generate the javadocs with:

	$ mvn javadoc:jar

The documentation is packed into a JAR located in the `target` directory
and can be browsed by pointing at the file:

	target/apidocs/index.html

Browsing the source code
------------------------

The source is available on GitHub at 
<http://github.com/jpvetterli/crnickl-jdbc.git>.

Finding more information
------------------------

More information on CrNiCKL is available at 
<http://agent.ch/timeseries/crnickl/>.

<small>Updated: 2012-08-28/jpv</small>

<link rel="stylesheet" type="text/css" href="README.css"/>

