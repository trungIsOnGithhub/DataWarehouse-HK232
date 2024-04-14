from commons import *
from luigi import *

class EstablishDBConnection(Task):
    def output(self):
        connection_data_tuple = get_postgres_db_connection()
        return LocalTarget(connection_data_tuple)

    def run(self):
        pass

class GetAllTableNames(Task):
    schema_name = Parameter()

    def requires(self):
        return [EstablishDBConnection]

    def output(self):
        count_rows_query_sql = f"SELECT tablename FROM pg_tables WHERE schemaname = '{self.schema_name}'"
        self.input[1].execute(count_rows_query_sql) # use ealier task input as postgres connection

        column_names = self.input[1].fetchall()

        return LocalTarget(column_names)

    def run(self):
        pass

class GettingTableStructure(Task):
    def requires(self):
        return [GetAllTableNames]

    def output(self):
        table_structure_query_sql = f'''SELECT column_name, data_type,
                                        character_maximum_length, is_nullable, column_default
                                FROM information_schema.columns WHERE table_name = 'film';'''

        self.input[1].execute(table_structure_query_sql) # use ealier task input as postgres connection

        table_structures = self.input[1].fetchall()

        return LocalTarget(table_structures)

    def run(self):
        pass

class GettingTableRecordNum(Task):
    def requires(self):
        return [GetAllTableNames]

    def output(self):
        table_row_num_list = []

        for names in self.input():
            table_structure_query_sql = f'''SELECT COUNT(*) FROM '{names}';'''

            self.input[1].execute(table_structure_query_sql) # use ealier task input as postgres connection

            table_row_num_list.append(self.input[1].fetchall())

        return LocalTarget(table_row_num_list)

    def run(self):
        pass

class GenerateReportFile(Task):
    params = DictParameter()

    def output(self):
        return LocalTarget(self.params['output_folder'])

    def run(self):
        lines_to_write = []

        with self.output().open('w') as file_out:
            for line in lines_to_write:
                file_out.write(line)