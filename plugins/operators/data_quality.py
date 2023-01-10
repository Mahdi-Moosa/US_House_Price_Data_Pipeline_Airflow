from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import logging

class TableDataQualityOperator(BaseOperator):
    ''' This class checks for the integrity of individual tables. Accepts tables if they have > 1 rows or if none of the rows are all null.
    Returns error if the number of row is < 1.
    
    Input:
        data_path: to parquet file(s) that will be read by pandas in a dataframe. .
    Output:
        Returns error if table size is == 0 or one of the colums have all null values.
    '''
    
    ui_color = '#fbf0ff'

    @apply_defaults
    def __init__(self,
                 data_path = '', # redshift_conn_id="",
                 # tables=[],
                 *args, **kwargs):

        super(TableDataQualityOperator, self).__init__(*args, **kwargs)
        self.data_path = data_path

    def execute(self, context):
        try:
            dataframe = pd.read_parquet(path=self.data_path)
        except:
            raise FileNotFoundError(f'File not present at {self.data_path}')
        df_length = dataframe.shape[0]
        if df_length == 0:
            raise ValueError('Dataframe seems to be empty! .shape[0] gave length of 0!!!')
        elif (dataframe.isnull().sum() == df_length).sum() >0 :
            raise ValueError('At least one of the columns have all Null values.')
        else:
            return logging.info(f"No errors raised for parquet data at {self.data_path}. \nLooks good :)")