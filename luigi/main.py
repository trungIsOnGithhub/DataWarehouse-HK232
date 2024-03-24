import luigi
from download_sales import *

if __name__ == '__main__':
    luigi.run(['IntegrateSalesData'])