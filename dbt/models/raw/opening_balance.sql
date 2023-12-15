select * 
from read_csv('/Users/olli/Downloads/kitsas-saldot-2022.csv', delim = ',', header = true, columns = {'account': 'VARCHAR', 'name': 'VARCHAR', 'balance':'DECIMAL'},
ignore_errors=true)