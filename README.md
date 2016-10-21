![cooltext211559191186936](https://cloud.githubusercontent.com/assets/7671010/19601995/7139c690-9779-11e6-92f4-4c3232ddb91f.png)

Run simple ETL operations between database systems using the JVM (Jython).


## Set CLASSPATH
<http://www.jython.org/jythonbook/en/1.0/DatabasesAndJython.html#getting-started>
```
# Oracle
# Windows
set CLASSPATH=<PATH TO JDBC>\\ojdbc14.jar;%CLASSPATH%

# OS X
export CLASSPATH=<PATH TO JDBC>/ojdbc14.jar:$CLASSPATH

# PostgreSQL
# Windows
set CLASSPATH=<PATH TO JDBC>\\postgresql-x.x.jdbc4.jar;%CLASSPATH%

# OS X
export CLASSPATH=<PATH TO
JDBC>/postgresql-x.x.jdbc4.jar:$CLASSPATH

```

## Examples
```
java -jar etl-runner.jar -tConn 'server1_postgres' -sConn 'server2_mysql' -sTable "common_us_locations" -tTable "common_us_locations" -sSchema "db1" -tSchema "db2" -truncate -showDetails
```
