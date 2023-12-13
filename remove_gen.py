import os

level1_dir = 'jsondata'
common_dir = f'{os.getcwd()}{os.sep}{level1_dir}'

for filename in os.listdir(common_dir):
    os.remove(f'{common_dir}{os.sep}{filename}')
    print(f'Removed File: {filename}')