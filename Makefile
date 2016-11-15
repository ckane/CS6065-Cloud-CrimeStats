CrimeRow.class: CrimeRow.java
	hadoop com.sun.tools.javac.Main CrimeRow.java

CrimeRecordInputFormat.class: CrimeRecordInputFormat.java CrimeRow.class
	hadoop com.sun.tools.javac.Main CrimeRecordInputFormat.java

CrimeStats.class: CrimeStats.java CrimeRecordInputFormat.class CrimeRow.class
	hadoop com.sun.tools.javac.Main CrimeStats.java

crimestats.jar: CrimeStats.class CrimeRecordInputFormat.class CrimeRow.class
	jar cf crimestats.jar *.class
