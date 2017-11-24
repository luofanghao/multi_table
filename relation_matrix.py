import pandas as pd
from os import listdir

def process(data_path, master_table_name):
	master_table = data_path + master_table_name
	other_table_names = listdir(data_path)
	other_table_names.remove(master_table_name)

	other_tables = list(data_path+name for name in other_table_names) # key: short name of table, value: fullname

	print("master table is: {}".format(master_table))
	print("other tables are: {}".format(other_tables))

	return get_relation_matrix(master_table, other_tables)

def get_relation_matrix(master_table_name, other_tables_names):
	"""
	Parameter: 
	master_table_name: string, the master table file name
	other_tables_names: list of strings, the other table file names

	Return:
	relation_matrix
	"""
	
	master_table = pd.read_csv(master_table_name)
	other_tables = {}
	for x in other_tables_names:
		other_tables[x] = pd.read_csv(x)

	# 1. define the relation_matrix: index and link
	relation_matrix_index = []
	source2index = {}
	
	for table in other_tables:
		for col_name in other_tables[table].keys():
			ind = table.upper() + "_" + col_name
			relation_matrix_index.append(ind)
			source2index[(table, col_name)] = ind
			
	relation_matrix = pd.DataFrame(index=relation_matrix_index, columns=master_table.keys())
	
	# 2. calculate
	for i in master_table:
		master_col = master_table[i]
		for table in other_tables:
			for col_name in other_tables[table].keys():
				table_col = other_tables[table][col_name]
				j = source2index[(table, col_name)]
				relation_matrix[i][j] = cal_relation_val(master_col, table_col)
				
				
	return relation_matrix

def cal_relation_val(master_col, table_col):
    master_set = set(master_col)
    table_set = set(table_col)
    if (master_set.issubset(table_set)):
    	return len(master_set)/float(len(table_set))
    return 0