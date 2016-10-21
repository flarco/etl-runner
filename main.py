
'''
from java.lang import ClassLoader
cl = ClassLoader.getSystemClassLoader()
paths = map(lambda url: url.getFile(), cl.getURLs())
print(paths)

'''
from __future__ import with_statement
from com.ziclix.python.sql import zxJDBC
from helpers import settings, get_rec
from arg_parser import parser_args

cred = settings['databases']['xenial_pg']
conn = zxJDBC.connect(cred['url'], cred['username'], cred['password'], cred['driver'])

cursor = conn.cursor()
cursor.execute("select * from information_schema.tables")
headers = tuple([k[0] for k in cursor.description])
data = [get_rec(row, headers) for row in cursor.fetchall()]
for rec in data:
  print('{}'.format(rec.table_name))

cursor.close()
conn.close()
