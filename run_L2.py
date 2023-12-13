import os

level1_dir = 'dwh_pipelines'
level2_dir = 'L2_staging_layer'
level3_dir = 'dev'
common_dir = f'{level1_dir}{os.sep}{level2_dir}{os.sep}{level3_dir}'

for filename in os.listdir(f'{os.getcwd()}{os.sep}{common_dir}'):
    if filename.startswith('stg_'):
        if os.sep == '/': # linux, mac
            os.system(f'python3 {common_dir}{os.sep}{filename}')
        else:
            os.system(f'python {common_dir}{os.sep}{filename}')