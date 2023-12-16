import os

level1_dir = 'dwh_pipelines'
level2_dir = 'dwh'
level3_dir = 'datamarts'
common_dir = f'{level1_dir}{os.sep}{level2_dir}{os.sep}{level3_dir}'

for filename in [filename for filename in os.listdir(f'{os.getcwd()}{os.sep}{common_dir}') if filename.startswith('fact_')]:
    if os.sep == '/': # linux, mac
        os.system(f'python3 {common_dir}{os.sep}{filename}')
    else:
        os.system(f'python {common_dir}{os.sep}{filename}')