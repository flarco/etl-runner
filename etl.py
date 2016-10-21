from __future__ import with_statement
import sys
from com.ziclix.python.sql import zxJDBC
from arg_parser import parser_args
from sql import sql_select_columns
from itertools import chain
from helpers import (
  settings,
  fetch_to_array_dict,
  log,
  get_elapsed_time,
  get_exception_message,
)

DATA_TYPE_MAP = {
  'varchar' : 'varchar',
  'mediumint' : 'int',
  'integer' : 'int',
  'int' : 'int',
  'money' : 'numeric',
  'date' : 'date',
  'datetime' : 'datetime',
  'character varying' : 'varchar',
}

class Conn:
  "A Database connection"

  def __init__(self, cred):
    self.table_columns = {}
    self.limit_templ = ''
    self.cred = cred
    self.conn = zxJDBC.connect(cred['url'], cred['username'], cred['password'], cred['driver'])
    self.cursor = self.conn.cursor(True)  # Dynamic cursor
    
    self.get_type()

  def get_type(self):
    "Get databse conn type"
    url = self.cred['url']

    if 'postgres' in url:
      self.type = "PostgreSQL"
      self.type_ = "postgres"
      self.limit_templ = 'LIMIT {}'

    elif 'oracle' in url:
      self.type = "Oracle"
      self.type_ = "oracle"
      self.limit_templ = 'AND ROWNUM <= {}'

    elif 'sqlserver' in url:
      self.type = "Microsoft SQL Server"
      self.type_ = "sqlserver"
      self.limit_templ = 'LIMIT {}'

    elif 'mysql' in url:
      self.type = "MySQL"
      self.type_ = "mysql"
      self.limit_templ = 'LIMIT {}'
    
    self.type = None
  
  def get_columns(self,table):
    "Get fields / types for a table"
    schema, table = table.split('.')
    self.cursor.execute(sql_select_columns[self.type_], [table, schema])
    self.table_columns[table] = fetch_to_array_dict(self.cursor)
    return self.table_columns[table]
  
  def get_select_sql(self, table, limit=None):
    limit_str = self.limit_templ.format(limit) if limit else ''
    sql='select * from {} WHERE 1=1 {}'.format(table, limit_str)
    return sql
  
  def fetch_size(self, size):
    data = self.cursor.fetchmany(size)
    if self.type_ == 'sqlserver':
      # check if contains date
      pass
    return data


def init_connections():
  "Connect to source / target databases"
  conns = {}
  for conn_name in [parser_args.source_conn,parser_args.target_conn]:
    if not conn_name in settings['databases']:
      log('Connection {} not found in settings.yml!'.format(conn_name))
      sys.exit(1)
    
    cred = settings['databases'][conn_name]
    conns[conn_name] = Conn(cred)
    log('Connected to {} -> {}'.format(conn_name, cred['url']))
  
  return conns


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
        equal = False
  
  if not equal:
    log(' >> Source / Target table structure not equal for: {} != {}'.format(source_table,target_table))
    log(' >> Number of fields: {}  vs  {}'.format(len(s_fields),len(t_fields)))
    log(' >> Aborting ETL.')
    return False
  
  # Run ETL!
  
  select_sql = s_conn.get_select_sql(source_table, limit)
  if show_details: log(select_sql)

  s_conn.cursor.execute(select_sql)

  insert_statement = 'INSERT INTO {table} ({names}) VALUES {values}'
  names = ','.join(['"' + f['column_name'] + '"' for f in t_fields])
  
  record_count = 0
  while True:
    data = s_conn.fetch_size(batch_size)
    if len(data) == 0: break
    
    record_count += len(data)
    log('  > row {}...'.format(record_count))
    values_placeh = '(' + ','.join(['?'] * len(t_fields)) + ')'
    insert_sql = insert_statement.format(
      table=target_table,
      names=names,
      values=', '.join([values_placeh] * len(data)),
    )

    try:
      t_conn.cursor.execute(insert_sql, list(chain(*data)))
      t_conn.conn.commit()
    except:
      if show_details: log(insert_sql)
      if show_details: log(data)
      print(get_exception_message())
      sys.exit(1)

  log('Inserted {} records in {}.'.format(record_count, target_table))
  

def main():
  "Run ETL"
  conns = init_connections()

  source_tables = parser_args.source_table.split(',')
  target_tables = parser_args.target_table.split(',')

  if len(source_tables) != len(target_tables):
    log('Source/Target number of tables mismatch! {} != {}.'.format(len(source_tables), len(target_tables)))
    sys.exit(1)

  for i, source_table in enumerate(source_tables):
    target_table = target_tables[i]

    parser_args.batch_size = int(parser_args.batch_size) if parser_args.batch_size else 5000
    
    run_etl(
      s_conn=conns[parser_args.source_conn],
      t_conn=conns[parser_args.target_conn],
      source_table=source_table,
      target_table=target_table,
      show_details=parser_args.show_details,
      truncate=parser_args.truncate,
      batch_size=parser_args.batch_size,
      limit=parser_args.limit,
    )
  
  log(get_elapsed_time())

main()


