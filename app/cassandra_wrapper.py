from cassandra.cluster import Cluster

class CassandraWrapper():
    def __init__(self, cluster_list: list[str], keyspace: str):
        self.__keyspace = keyspace
        self._cluster = Cluster(cluster_list)
        self._session = self._cluster.connect(keyspace)

    def get_keyspace(self):
        return self.__keyspace

    def execute_query(self, query: str):
        return self._session.execute(query)

    def get_tables(self):
        return [r.name for r in self.execute_query("DESCRIBE TABLES").current_rows]

    def table_exists(self, table_name: str):
        tables = self.get_tables()
        return table_name in tables

    def create_table_based_on_dataframe(self, table_name, dataframe, primary_key = None):
        if self.table_exists(table_name):
            raise Exception(f"Table named '{table_name}' already exists inside '{self.__keyspace}' keyspace")
            
        if primary_key == None:
            primary_key = dataframe.dtypes[0][0]

        schema = []
        for col in dataframe.dtypes:
            if col[0] == primary_key:
                schema.append(f'\"{col[0]}\" {self.__correct_types(col[1])} PRIMARY KEY')
            else:
                schema.append(f'\"{col[0]}\" {self.__correct_types(col[1])}')

        query = f"CREATE TABLE {self.__keyspace}.{table_name} ({', '.join(schema)});"
        print("CQL Query:", query)
        self.execute_query(query)
        

    def insert_dataframe(self, table_name: str, dataframe, create_if_missing = False, primary_key = None):

        if not self.table_exists(table_name):
            if create_if_missing:
                self.create_table_based_on_dataframe(table_name, dataframe, primary_key=primary_key)
            else:
                raise Exception(f"Table named '{table_name}' does not exist inside '{self.__keyspace}' keyspace")

        iterator = dataframe.toLocalIterator()

        for row in iterator:
            d = row.asDict()

            keys = []
            values = []
            for col_name, value in d.items():
                keys.append(f'\"{col_name}\"')
                if type(value) == str:
                    values.append(f"$${value}$$") # using double $ sings prevents issues when " or ' are present in the string
                else:
                    values.append(str(value))
            query = f"INSERT INTO {self.__keyspace}.{table_name} ({', '.join(keys)}) VALUES ({', '.join(values)})"
            self.execute_query(query)


    __correction = {
            "string": "text"
        }

    def __correct_types(self, type: str) -> str:
            if type in self.__correction:
                return self.__correction[type]
            else:
                return type
