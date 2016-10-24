from __future__ import with_statement
import sys, datetime
from com.ziclix.python.sql import zxJDBC
from arg_parser import parser_args
from sql import (
  sql_select_columns,
  sql_insert,
)
from itertools import chain
import java.util.Date, java.sql.Date, java.text.SimpleDateFormat
import java.math.BigDecimal
from java.lang import (
  String,
  Integer,
  Long,
  Double,
  Boolean,
)
from helpers import (
  settings,
  fetch_to_array_dict,
  log,
  get_elapsed_time,
  get_exception_message,
)
from jdbc import (
  getJdbcConnection,
)

DATA_TYPE_MAP = {
  'varchar' : 'varchar',
  'varchar2' : 'varchar',
  'mediumint' : 'numeric',
  'integer' : 'numeric',
  'int' : 'numeric',
  'money' : 'numeric',
  'numeric' : 'numeric',
  'decimal' : 'numeric',
  'number' : 'numeric',
  'date' : 'date',
  'datetime' : 'datetime',
  'character varying' : 'varchar',
}


class Conn:
  "A Database connection"
  execute_many_limit = 1000

  def __init__(self, cred, pure_jdbc=False):
    self.table_columns = {}
    self.limit_templ = ''
    self.cred = cred
    self.pure_jdbc = pure_jdbc
    self.conn = zxJDBC.connect(cred['url'], cred['username'], cred['password'], cred['driver'])
      
    self.cursor = self.conn.cursor(True)  # Dynamic cursor
    self.convert_warned = False
    self.convert_i_to_do = None
    self.get_type()

    if self.pure_jdbc:
      jdbc_str_template = "%s;user=%s;password=%s;" if self.type_ == 'sqlserver' else "%s?user=%s&password=%s"
      jdbc_str =  jdbc_str_template % (cred['url'], cred['username'], cred['password'])
      self.conn2 = getJdbcConnection(jdbc_str, cred['driver'])
      self.conn2.setAutoCommit(False)

  def get_type(self):
    "Get databse conn type"
    url = self.cred['url']

    if 'postgres' in url:
      self.type = "PostgreSQL"
      self.type_ = "postgres"
      self.limit_templ = 'LIMIT {}'
      self.name_qual = '"'

    elif 'oracle' in url:
      self.type = "Oracle"
      self.type_ = "oracle"
      self.limit_templ = 'AND ROWNUM <= {}'
      self.name_qual = '"'

    elif 'sqlserver' in url:
      self.type = "Microsoft SQL Server"
      self.type_ = "sqlserver"
      self.limit_templ = 'TOP {}'
      self.name_qual = '"'

    elif 'mysql' in url:
      self.type = "MySQL"
      self.type_ = "mysql"
      self.limit_templ = 'LIMIT {}'
      self.name_qual = '`'
    
    self.type = None
  
  def get_columns(self,table):
    "Get fields / types for a table"
    schema, table = table.split('.')
    self.cursor.execute(sql_select_columns[self.type_], [table, schema])
    self.table_columns[table] = fetch_to_array_dict(self.cursor)
    return self.table_columns[table]
  
  def get_select_sql(self, table, limit=None):
    limit_str = self.limit_templ.format(limit) if limit else ''
    sql='select {limit2} * from {from_table} WHERE 1=1 {where} {limit}'.format(
      limit2=limit_str if self.type_ == 'sqlserver' else '',
      from_table=table,
      where='',
      limit=limit_str if self.type_ != 'sqlserver' else ''
    )
    return sql
  
  def get_insert_sql(self, table, names):
    if self.type_ == 'oracle':
      insert_statement = sql_insert['_into'].format(
        table=table,
        names=names,
        values='{values}'
      )
    else:
      insert_statement = sql_insert[self.type_].format(
        table=table,
        names=names,
        values='{values}'
      )
    
    return insert_statement
  
  def table_select(self, table, limit=None, fetch_all=True):
    select_sql = self.get_select_sql(table, limit)
    if parser_args.show_details: log(select_sql)
    self.cursor.execute(select_sql)
    if fetch_all:
      return fetch_to_array_dict(self.cursor)
  
  def query_batch(self, statement, fields, data):
    "run batch query"
    if len(data) == 0 : return
    values_placeh = '(' + ','.join(['?'] * len(fields)) + ')'

    try:
      data2 = []
      for i, row in enumerate(data):
        data2.append(row)
        if (i+1) % self.execute_many_limit == 0:
          if self.type_ == 'oracle':
            sql_into = statement.format(values=values_placeh)
            sql = sql_insert['oracle'].format(_into=' '.join([sql_into] * len(data2)))
          else:
            sql = statement.format(values=', '.join([values_placeh] * len(data2)))
          self.cursor.execute(sql, list(chain(*data2)))  # executemany submits many times instead of once
          data2 = []
      
      if len(data2) > 0:
        if self.type_ == 'oracle':
          sql_into = statement.format(values=values_placeh)
          sql = sql_insert['oracle'].format(_into=' '.join([sql_into] * len(data2)))
        else:
          sql = statement.format(values=', '.join([values_placeh] * len(data2)))
        self.cursor.execute(sql, list(chain(*data2)))  # executemany submits many times instead of once
      
      self.conn.commit()
      
    except:
      if parser_args.show_details:
        log(statement.format(values=values_placeh))
        l = min(20,len(data))
        log(data[:l-1])
      
      print(get_exception_message())
      sys.exit(1)

  def query_batch2(self, statement, fields, data):
    "run batch query with JDBC Prepared Statement"
    """ using prepared statements was about 60% slower than using
    the cursor.execute method...I am not sure why. Other conn/cusor
    must be closed."""
    values_placeh = '(' + ','.join(['?'] * len(fields)) + ')'

    sql = statement.format(
      values=values_placeh,
    )

    preppedStmt = self.conn2.prepareStatement(sql)

    for row in data:
      for i, val in enumerate(row):
        # log('%s - %r - %r' % (i, type(val), val))
        if isinstance(val, String): preppedStmt.setString(i+1, val)
        elif isinstance(val, java.sql.Date): preppedStmt.setDate(i+1, val)
        elif isinstance(val, java.math.BigDecimal): preppedStmt.setBigDecimal(i+1, val)
        elif isinstance(val, float): preppedStmt.setDouble(i+1, val)
        elif isinstance(val, int): preppedStmt.setInt(i+1, val)
        elif isinstance(val, long): preppedStmt.setLong(i+1, val)
        else: preppedStmt.setString(i+1, str(val))
      preppedStmt.addBatch()
    
    try:
      preppedStmt.executeBatch()
      self.conn2.commit()
      
    except:
      if parser_args.show_details: log(sql)
      if parser_args.show_details:
        l = min(20,len(data))
        log(data[:l-1])
      
      print(get_exception_message())
      sys.exit(1)

  def fetch_size(self, size):
    
    def is_date(val):
      try:
        d=datetime.datetime.strptime(val, '%Y-%m-%d')
        return True
      except:
        return False

    try:
      data = self.cursor.fetchmany(size)
    except zxJDBC.DatabaseError:
      log('zxJDBC.DatabaseError...')
      return []

    if self.type_ == 'sqlserver2':
      # check if contains date because it converts to string (datetime is fine)

      if self.convert_i_to_do == None:
        i_to_check={i:False for i,d in enumerate(self.cursor.description) if d[1] == -9 and d[2] == 10 }
        
        rows_to_audit = min(100, len(data))
        for ri in range(rows_to_audit):
          row = data[ri]
          for i in i_to_check:
            if is_date(row[i]):
              i_to_check[i] = True
        
        if any(i_to_check.values()):
          i_to_do = [k for k,v in i_to_check.items() if v]

          if not self.convert_warned:
            log(" >> WARNING: Need to convert DATE values from string to datetime.. Will take longer than usual.")
            self.convert_warned = True
            self.convert_i_to_do = i_to_do
            log(" >> WARNING: %r = %s" % ('convert_i_to_do', self.convert_i_to_do))
      
      if len(self.convert_i_to_do) > 0:
        data2 = []
        for row in data:
          row = list(row)
          for i in self.convert_i_to_do:
            row[i] = datetime.datetime.strptime(row[i], '%Y-%m-%d')
            # row[i] = convert_string_date(row[i])
          data2.append(row)
        return data2

    return data


def init_connections():
  "Connect to source / target databases"
  conns = {}
  for conn_ in ['source_conn','target_conn']:
    conn_name = parser_args[conn_]
    if not conn_name in settings['databases']:
      log('Connection {} not found in settings.yml!'.format(conn_name))
      sys.exit(1)
    
    cred = settings['databases'][conn_name]
    conns[conn_] = Conn(cred)
    log('Connected to {} -> {}'.format(conn_name, cred['url']))
  
  return conns['source_conn'], conns['target_conn']


def run_etl(s_conn,t_conn,source_table,
      target_table,show_details = False,truncate = False,
      batch_size=1000,limit=None):
  "Run the ETL"
  log("Running:  {}  ->  {}".format(source_table, target_table))

  # Truncate
  if truncate:
    t_conn.cursor.execute("TRUNCATE TABLE {}".format(target_table))
    log("  > Truncated TARGET Table {}.".format(target_table))

  # verify table / columns exists in target
  s_fields = s_conn.get_columns(source_table)
  # if show_details: log('s_fields -> ' + str(s_fields))
  t_fields = t_conn.get_columns(target_table)
  # if show_details: log('t_fields -> ' + str(t_fields))

  equal = len(s_fields) == len(t_fields)
  
  if equal:
    for i, s_field in enumerate(s_fields):
      t_field = t_fields[i]
      s_field.data_type = s_field.data_type.lower()
      t_field.data_type = t_field.data_type.lower()

      if not s_field.data_type in DATA_TYPE_MAP:
        log('{} not in DATA_TYPE_MAP...'.format(s_field.data_type))
        equal = False
        continue
      
      if not t_field.data_type in DATA_TYPE_MAP:
        log('{} not in DATA_TYPE_MAP...'.format(t_field.data_type))
        equal = False
        continue
      
      if DATA_TYPE_MAP[s_field.data_type] != DATA_TYPE_MAP[t_field.data_type]:
        log(' >> Field Type mismatch: {}.{} != {}.{}'.format(s_field.column_name, s_field.data_type,t_field.column_name, t_field.data_type))
        # equal = False
  
  if not equal:
    log(' >> Source / Target table structure not equal for: {} != {}'.format(source_table,target_table))
    log(' >> Number of fields: {}  vs  {}'.format(len(s_fields),len(t_fields)))
    log(' >> Aborting ETL.')
    return False
  
  # Run ETL!
  
  etl_time_start = datetime.datetime.now()
  s_conn.table_select(source_table, limit=limit, fetch_all=False)

  nq =  t_conn.name_qual
  names = ','.join([nq + f['column_name'] + nq for f in t_fields])
  insert_statement = t_conn.get_insert_sql(target_table, names)
  
  if show_details: log(insert_statement)
  
  record_count = 0
  inc = 0

  while True:
    data = s_conn.fetch_size(batch_size)
    if len(data) == 0: break
    
    record_count += len(data)
    inc += len(data)

    if inc > 10000:
      log('  > row {}...'.format(record_count))
      inc = 0

    t_conn.query_batch(insert_statement, t_fields, data)
  
  etl_time_delta = (datetime.datetime.now() - etl_time_start).total_seconds()
  log('Inserted {} records into {} in {} seconds [{} rec/sec].'.format(record_count, target_table, etl_time_delta, round(record_count/etl_time_delta,1)))
  return record_count
  

def main():
  "Run ETL"
  source_conn,target_conn = init_connections()

  source_tables = parser_args.source_table.split(',')
  target_tables = parser_args.target_table.split(',')

  if len(source_tables) != len(target_tables):
    log('Source/Target number of tables mismatch! {} != {}.'.format(len(source_tables), len(target_tables)))
    sys.exit(1)

  for i, source_table in enumerate(source_tables):
    target_table = target_tables[i]

    parser_args.batch_size = int(parser_args.batch_size) if parser_args.batch_size else 5000
    
    r_count = run_etl(
      s_conn=source_conn,
      t_conn=target_conn,
      source_table=source_table,
      target_table=target_table,
      show_details=parser_args.show_details,
      truncate=parser_args.truncate,
      batch_size=parser_args.batch_size,
      limit=parser_args.limit,
    )
  
  log(get_elapsed_time())

main()


