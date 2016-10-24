
# https://wiki.python.org/jython/DatabaseExamples

import sys

from java.lang import Class
from java.sql  import DriverManager, SQLException

DATABASE    = "solarsys.db"
SQLITE_JDBC_URL    = "jdbc:sqlite:%s" % DATABASE
SQLITE_JDBC_DRIVER = "org.sqlite.JDBC"

def getJdbcConnection(jdbc_url, driverName):
  """
      Given the name of a JDBC driver class and the url to be used
      to connect to a database, attempt to obtain a connection to
      the database.
  """
  # try:
  #   Class.forName(driverName).newInstance()
  # except Exception, msg:
  #   print(msg)
  #   sys.exit(-1)
  try:
    dbConn = DriverManager.getConnection(jdbc_url)
  except SQLException, msg:
    print msg
    sys.exit(-1)
  return dbConn

def convert_string_date(date_str):
  date_str=java.lang.String(date_str)
  sdf1 = java.text.SimpleDateFormat("yyyy-MM-dd")
  date = java.util.Date(sdf1.parse(date_str).getTime())
  return date

# jdbc_url = "jdbc:postgresql://xenial:5432/db1?user=admin&password=delta15!"
# driverName = "org.postgresql.Driver"
# conn = getJdbcConnection(jdbc_url, driverName)

# conn.setAutoCommit(False)


