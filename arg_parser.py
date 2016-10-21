import argparse
from helpers import dict2

parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description='Run various ETL tasks between servers.')

parser.add_argument('-sConn','--source-conn', help='Source database server', required=True)
parser.add_argument('-tConn','--target-conn', help='Target database server', required=True)
# parser.add_argument('-sSchema','--source-schema', help='Schema on source database server')
# parser.add_argument('-tSchema','--target-schema', help='Schema on target database server')
parser.add_argument('-sTable','--source-table', help='Table on source database server (schema.table)', required=True)
parser.add_argument('-tTable','--target-table', help='Table on target database server (schema.table)', required=True)
parser.add_argument('-batchSize', dest='batch_size', help='The batch size / commit interval.')
parser.add_argument('-limit', dest='limit', help='The limit of records to read.')
parser.add_argument('-delimiter', dest='delimiter', help="The delimeter of the text file. For tab: '/t'")
parser.add_argument('-fileExtension', dest='file_extension', help="The extension of the text file. Example: '.txt'")
parser.add_argument('-selectSql', dest='select_sql', help="Override the Select SQL Query. Example: 'SELECT COLUMN1, COLUMN1 + COLUMN2 from textFile'")
parser.add_argument('-selectSqlFile', dest='select_sql_file', help="Override the Select SQL Query with query in file path. Example: 'C:\\Temp\\sql_view1.sql'")

parser.add_argument('-truncate', dest='truncate', action='store_true', help='Indicates to truncate TARGET table before INSERT.')
parser.add_argument('-showDetails', dest='show_details', action='store_true', help='Show details such as INSERT rate, SQL Query, number of columns.')
parser.add_argument('-noHeaders', dest='no_headers', action='store_true', help='Specifies that Delimited Separated File / CSV has no Header row.')

parser_args = dict2(vars(parser.parse_args()))