import os
import luigi
from luigi import Task, Parameter, LocalTarget

INPUT_FOLDER = 'input_files'
OUTPUT_FOLDER = 'output_files'

class DownloadFile(Task):
    file_name = Parameter()

    def output(self): # target
        path = os.path.join(OUTPUT_FOLDER, self.file_name)
        return LocalTarget(path)

    def run(self): # logic
        input_path = os.path.join(INPUT_FOLDER, self.file_name)

        with open(input_path) as file_in:
            with self.output().open('w') as file_out:
                for line in file_in:
                    if ',' in line:
                        file_out.write(line)


if __name__ == '__main__':
    luigi.run([''])