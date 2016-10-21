import yaml, os, sys
from yaml import Loader, SafeLoader
import logging, datetime
from collections import OrderedDict

time_start = datetime.datetime.now()
# logger = logging.getLogger()
# handler = logging.StreamHandler()
# formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)
# logger.setLevel(logging.INFO)

try:
  DIR = os.path.dirname(os.path.realpath(__file__))
except:
  DIR = os.path.abspath('')
  # DIR = os.path.dirname(os.path.realpath(sys.argv[0]))


def construct_yaml_str(self, node):
    # Override the default string handling function
    # to always return unicode objects
    return self.construct_scalar(node)

Loader.add_constructor(u'tag:yaml.org,2002:str', construct_yaml_str)
SafeLoader.add_constructor(u'tag:yaml.org,2002:str', construct_yaml_str)


class dict2(dict):
  """ Dict with attributes getter/setter. """
  def __getattr__(self, name):
    return self[name]
  
  def __setattr__(self, name, value):
    self[name] = value

def load_settings():
  with open(DIR + '/settings.yml') as settings_file:
    settings = yaml.load(settings_file)
  return settings

def log(text):
  # logger.info(text)
  print('{} >> {}'.format(datetime.datetime.now(), text))

get_rec = lambda row, headers: dict2({h:row[i] for i,h in enumerate(headers)})

# def get_rec(row, headers):
#   rec = OrderedDict()
#   for i,h in enumerate(headers):
#     rec[h] = row[i]
#   return rec

def fetch_to_array_dict(cursor, size=None):
  headers = tuple([k[0] for k in cursor.description])
  if size: data = [get_rec(row, headers) for row in cursor.fetchmany(size)]
  else: data = [get_rec(row, headers) for row in cursor.fetchall()]
  return data

def get_elapsed_time():
  time_end = datetime.datetime.now().now()
  # delta_seconds= (time_end - time_start).total_seconds()
  # return delta_seconds

  delta_time = divmod((time_end - time_start).days * 86400 + (time_end - time_start).seconds, 60)
  return str(delta_time[0]) + ' mins ' + str(delta_time[1]) + ' seconds elapsed'

def get_exception_message(append_message = ''):
    import linecache
    import traceback
    
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    message = '-'*65 + '\n' +'EXCEPTION IN ({}, LINE {} "{}"): {} \n---\n{}'.format(filename, lineno, line.strip(), exc_obj, traceback.format_exc()) + '\n' + append_message
    return message

# Get credentials & settings
settings = load_settings()