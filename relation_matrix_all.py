import pandas as pd
from os import listdir
import time

def process(data_path):
	table_names = listdir(data_path)

	tables = list(name for name in table_names) # list of table names
	print("tables are: {}".format(tables))

	return get_relation_matrix(tables, data_path)

def get_relation_matrix(table_names, data_path):
	"""
	Parameter: 
	tables_names: list of strings, the other table file names

	Return:
	relation_matrix
	"""
	
	start_time = time.time()

	other_tables = {} # dict, key: name; value : pandas.DataFrame
	for x in table_names:
		other_tables[x] = pd.read_csv(data_path + x)

	print("=====>> data readin finished: {}".format(time.time() - start_time))

	# 1. define the relation_matrix: index and link
	relation_matrix_index = []	# list of columns name (index)
	source2index = {}			# dict, key: (table_name, col_name), value: index in matrix
	
	for table in other_tables:
		for col_name in other_tables[table].keys():
			ind = table.upper() + "_" + col_name
			relation_matrix_index.append(ind)
			source2index[(table, col_name)] = ind
			
	relation_matrix = pd.DataFrame(index=relation_matrix_index, columns=relation_matrix_index)
	
	# 2. calculate
	# table1 is assumed to be the master table, which will be columns_id in relation_matrix
	for table1 in other_tables:
		for table2 in other_tables:
			start_time = time.time()
			if (table1 == table2): continue
			# compute all column pairs
			for col_name1 in other_tables[table1].keys():
				for col_name2 in other_tables[table2].keys():
					col1 = other_tables[table1][col_name1]
					col2 = other_tables[table2][col_name2]
					i = source2index[(table1, col_name1)]
					j = source2index[(table2, col_name2)]
					relation_matrix[i][j] = cal_relation_val(col1, col2)
			print("=====>> {} vs {} finished: {}".format(table1, table2, time.time() - start_time))
				
				
	return relation_matrix

def cal_relation_val(master_col, table_col):
    master_set = set(master_col)
    table_set = set(table_col)
    if (master_set.issubset(table_set)):
    	return len(master_set)/float(len(table_set))
    return 0