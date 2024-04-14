import os
from luigi import *

# INPUT_FOLDER = 'input_files'
OUTPUT_FOLDER = 'output_files'
# RESULT_FILE = 'all_sales.csv'

class DownloadFile(Task):
    input_folder = Parameter()
    file_name = Parameter()
    index = IntParameter()

    def output(self): # target
        path = os.path.join(OUTPUT_FOLDER, str(self.index), self.file_name)

        return LocalTarget(path)


    def run(self): # logic
        input_path = os.path.join(self.input_folder, self.file_name)

        with open(input_path) as file_in:
            with self.output().open('w') as file_out:
                for line in file_in:
                    if ',' in line: # csv files
                        file_out.write(line)

class IntegrateSalesData(Task):
    params = DictParameter()

    def output(self):
        return LocalTarget(self.params['output'])

    def run(self):
        processed_files = []

        input_files = sorted(os.listdir(self.params['input']))

        tasks = []
        task_counter = 1

        for file in input_files:
            tasks.append(DownloadFile(self.params['input'], file, task_counter))

            self.set_progress_percentage(100 * task_counter / len(input_files))
            task_counter += 1

        processed_files = yield tasks

        with self.output().open('w') as file_out:
            for file in processed_files:
                with file.open() as file_in:
                    for line in file_in:
                        file_out.write(line)