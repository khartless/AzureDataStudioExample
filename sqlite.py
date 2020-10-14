#Create the functions to import the data frame into the database
def table_exists(table_name):
    """
    Checks the open database connection and returns true if the table already exists in database
    """
    c.execute("SELECT count(name) FROM sqlite_master WHERE TYPE = 'table' AND name = '{}'".format(table_name))
    if c.fetchone()[0] == 1:
        return True 
    return False

def create_tables(cols, data_types, table_name):
    if not table_exists(table_name):
        sql_string = "CREATE TABLE {}(".format(table_name)
        for index, col in enumerate(cols):
            if index == 0:
                sql_string += "{} {}".format(col, data_types[index])
            else:
                sql_string += ", {} {}".format(col, data_types[index])
        sql_string += ")"
        c.execute(sql_string)
        return sql_string

def get_data_types(df):
    dfModified = df.copy()
    dataTypesList = []
    dataTypes = dict(df.dtypes)
    dataTypes['ComplexEntityNo']
    for key, value in dataTypes.items():
        if value in ['object', 'datetime64[ns]', '<M8[ns]']:
            dataTypesList.append('TEXT')
            dfModified[key] = [str(x) for x in dfModified[key]]
        elif value in ['int64']:
            dataTypesList.append('INTEGER')
        elif value in ['float64']:
            dataTypesList.append('REAL')
        else:
            dataTypesList.append('BLOB')
    return dfModified, dataTypesList

def insert_data(table_name, cols, df):
    sql_string = "INSERT INTO {} (".format(table_name)
    for index, col in enumerate(cols):
        if index == 0:
            sql_string += col 
        else:
            sql_string += ", {}".format(col)
    sql_string += ") VALUES("
    sql_string += "?, "*(len(cols)-1)
    sql_string += "?)"
    for index, row in df.iterrows():
        c.execute(sql_string, tuple(row))
        conn.commit()

def to_sqlite(df, table_name, **cols):
    if (len(cols) == 0) | (len(cols) != len(list(df.columns))) | (type(cols) != 'list'):
        cols = list(df.columns)
    df, dataTypes = get_data_types(df)
    create_tables(cols, dataTypes, table_name)
    #print(table_exists(table_name))
    insert_data(table_name, cols, df)

def query_table(table_name, top_n):
    if table_exists(table_name):
        if top_n > 0:
            c.execute("SELECT * FROM {} LIMIT {}".format(table_name, top_n))
        else:
            c.execute("SELECT * FROM {}".format(table_name))
        data = []
        for row in c.fetchall():
            data.append(row)
        return data
    else:
        return "Table {} does not exist.".format(table_name)

def delete_table(table_name):
    c.execute("DROP TABLE IF EXISTS {}".format(table_name))
