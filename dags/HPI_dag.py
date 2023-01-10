from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators.data_quality import TableDataQualityOperator
from helpers.functions import save_pd_to_parquet
import logging, os , time, requests
import pandas as pd
from functools import reduce

default_args = {
    'owner': 'Mahdi Moosa',
    'start_date': datetime(2022, 12, 14),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False,
    'file_presence_check' : True # If you want to replace existing files (i.e., don't want to check for the presence of pre-existing files) set this flag to False. This will apply to all file downloads.
}

def fetch_data_from_url(*args, **kwargs):
    """
    This function fetches files from URL. Input should be provided as task parameters. 
    Inputs include (as parameters)
        url: URL to download.
        file_name: Name of the file to be downloaded from the URL.
        folder_name: Folder name where downloaded file will be saved. (If file is already present, then download will be skipped).
    """
    # import os, logging, time, requests
    url = kwargs["params"]["url"]                   # URL to fetch
    fname = kwargs["params"]["file_name"]           # File name 
    folder_name = kwargs["params"]["folder_name"]   # Destination folder name
    file_presence_check = default_args['file_presence_check']
    fpath = folder_name + '/' + fname
    
    if file_presence_check and os.path.exists(f'{fpath}'):
        return logging.info(f'File named {fname} already present.')
    
    if not os.path.isdir(folder_name):
        os.makedirs(folder_name)
    
    response = requests.get(url, stream=True)
    time.sleep(2)
    total_length = response.headers.get('content-length')
    total_length = round(int(total_length)/1e6,2)
    logging.info(f'To be saved as {fpath}. Total size to be written is: {total_length} MB') 
    
    with open(fpath, "wb") as f:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:
                f.write(chunk)
    
    if os.path.exists(f'{fpath}'):
        logging.info(f'File download succesful for {fname}')

def etl_master_uad(*args, **kwargs):
    # Load uad data in pandas dataframe
    # import pandas as pd
    # from functools import reduce
    
    if os.path.exists(f'data/etl_data/uad_appraisal/zip_uad_table/'):
        return logging.info(f'Folder data/etl_data/uad_appraisal/zip_uad_table/ already exists.')
    
    # Load all files
    uad_df = pd.read_csv('data/raw_data/FHFA-UAD/UADAggs_tract.zip')
    logging.info('UAD data loaded successfully.')
    # Load tract to zip code data, drop one-to-many relationships
    tract_2_zip = pd.read_excel('data/raw_data/HUD-USPS/TRACT_ZIP_122021.xlsx')
    tract_2_zip = tract_2_zip.drop_duplicates(subset='tract')
    logging.info('Mapping data from tract_2010 to zip code loaded.')
    # Load tract 2010 to tract 2020 map, select relevant colums and drop one-to-many relationships
    tract_2010_to_tract_2020 = pd.read_csv('data/raw_data/census/tab20_tract20_tract10_natl.txt', sep='|')
    logging.info('Census tract 2020 to census tract 2010 map loaded.')
    
    # Data selection, drop duplicates
    tract_2020_2010_map = tract_2010_to_tract_2020[['GEOID_TRACT_20','GEOID_TRACT_10']]
    tract_2020_2010_map = tract_2020_2010_map.drop_duplicates(subset='GEOID_TRACT_20')
    # Maping census tract 2020 to census tract 2010.
    uad_df = uad_df[uad_df.VALUE.notna()] # Removing unnecessary datapoints before merge operation.
    uad_df = uad_df.merge(tract_2020_2010_map, left_on='TRACT', right_on='GEOID_TRACT_20', how='left')
    logging.info('Merge of uad_df with tract_2020_2010_map was successful.')
    # Dropping Null entries on GEOID_TRACT_20, if any
    uad_df = uad_df[uad_df.GEOID_TRACT_20.notna()]
    # Column data type assignment
    uad_df.GEOID_TRACT_10 = uad_df.GEOID_TRACT_10.astype('int64')
    uad_df.GEOID_TRACT_20 = uad_df.GEOID_TRACT_20.astype('int64')
    
    logging.info('Mapping census track to zip map.')
    # Mapping Zip to Census tract 2010
    uad_df_merged = uad_df.merge(tract_2_zip[['tract', 'zip']], left_on = 'GEOID_TRACT_10', right_on='tract', how='left')
    # Dropping null Zip entries.
    uad_df_merged = uad_df_merged[uad_df_merged.zip.notna()].copy(deep=True)
    # Data type conversion
    uad_df_merged.zip = uad_df_merged.zip.astype(int)
    # Dropping dupllicate columns
    uad_df_merged.drop(columns='tract', inplace=True)
    
    #Selecting relevant columns, and save final merged UAD table
    # Selecting only relevant columns
    uad_df_merged_slice_to_save =  uad_df_merged[['SERIESID', 'PURPOSE', 'YEAR', 'VALUE', 'GEOID_TRACT_10', 'GEOID_TRACT_20', 'zip']]
    # Filtering dataframe based on relevant SERIESIDs.
    uad_df_merged_slice_to_save = uad_df_merged_slice_to_save[uad_df_merged_slice_to_save.SERIESID.isin(['COUNT', 'MEDIAN', 'P25', 'P75', 'MEAN'])]
    # Splitting UAD table into count, mean, median, p25 and p75 tables
    count_table = uad_df_merged_slice_to_save[(uad_df_merged_slice_to_save.PURPOSE == 'Both')
                           & (uad_df_merged_slice_to_save.SERIESID == 'COUNT')]
    mean_table = uad_df_merged_slice_to_save[(uad_df_merged_slice_to_save.PURPOSE == 'Both')
                           & (uad_df_merged_slice_to_save.SERIESID == 'MEAN')]
    median_table = uad_df_merged_slice_to_save[(uad_df_merged_slice_to_save.PURPOSE == 'Both')
                           & (uad_df_merged_slice_to_save.SERIESID == 'MEDIAN')]
    p25_table = uad_df_merged_slice_to_save[(uad_df_merged_slice_to_save.PURPOSE == 'Both')
                           & (uad_df_merged_slice_to_save.SERIESID == 'P25')]
    p75_table = uad_df_merged_slice_to_save[(uad_df_merged_slice_to_save.PURPOSE == 'Both')
                           & (uad_df_merged_slice_to_save.SERIESID == 'P75')]
    #Performing appropriate aggregation operation on count, mean, median, p25 and p75 tables. 
    zip_count_table = count_table[['zip', 'YEAR', 'VALUE']].groupby(['zip', 'YEAR']).sum('VALUE').rename(columns={'VALUE':'VALUE_count'})
    zip_mean_table = mean_table[['zip', 'YEAR', 'VALUE']].groupby(['zip', 'YEAR']).mean('VALUE').rename(columns={'VALUE':'VALUE_mean'})
    zip_median_table = median_table[['zip', 'YEAR', 'VALUE']].groupby(['zip', 'YEAR']).mean('VALUE').rename(columns={'VALUE':'VALUE_median'})
    zip_p25_table = p25_table[['zip', 'YEAR', 'VALUE']].groupby(['zip', 'YEAR']).mean('VALUE').rename(columns={'VALUE':'VALUE_p25'})
    zip_p75_table = p75_table[['zip', 'YEAR', 'VALUE']].groupby(['zip', 'YEAR']).mean('VALUE').rename(columns={'VALUE':'VALUE_p75'})
    # Combining zip count, mean, median, p25 and p75 tables into single table where UAD data is turned into single table. This table is the end table of the ETL process for UAD data
    # from functools import reduce
    dfs_to_merge = [zip_count_table, zip_mean_table, zip_median_table, zip_p25_table, zip_p75_table]
    zip_uad_df_merged = reduce(lambda  left,right: pd.merge(left,right,left_index=True, right_index=True,
                                            how='left'), dfs_to_merge)

    # Save file to specified directory
    out_dir = 'data/etl_data/uad_appraisal'
    save_pd_to_parquet(dtframe=zip_uad_df_merged, fldr_name=out_dir, table_name='zip_uad_table')

def LT_to_HPA_master_DF(*args, **kwargs):
    
    # Read zip_uad_df_merged
    zip_uad_df_merged = pd.read_parquet('data/etl_data/uad_appraisal/zip_uad_table/')
    
    # Realtor Data Process
    realtor_df = pd.read_csv('data/raw_data/realtor_data/RDC_Inventory_Core_Metrics_Zip_History.csv', low_memory=False)
    logging.info('Realtor.com data successfully read from the .csv file.')
    realtor_df = realtor_df.iloc[0:-1] # Dropping last line that contains aggregated summary (line Total).
    # Selecting relevant columns.
    realtor_cols = ['month_date_yyyymm', 'postal_code', 'median_listing_price', 'average_listing_price', 'active_listing_count', 'median_days_on_market', 'new_listing_count', 
                    'price_increased_count', 'price_reduced_count', 'pending_listing_count', 'median_listing_price_per_square_foot', 'median_square_feet', 'total_listing_count', 
                   'pending_ratio', 'quality_flag']
    realtor_df_slice = realtor_df[realtor_cols].copy(deep=True)
    realtor_df = None # To release memory.
    # Data format conversion
    realtor_df_slice.month_date_yyyymm = pd.to_datetime(realtor_df_slice.month_date_yyyymm, format='%Y%m') #.month_date_yyyymm.astype('datetime64[ns]')
    realtor_df_slice['postal_code'] = realtor_df_slice.postal_code.astype(int)
    # Zip-Year aggregation
    realtor_df_slice_agg = realtor_df_slice.groupby(by=['postal_code', realtor_df_slice.month_date_yyyymm.dt.year]).agg(
                                                                                                median_list_price = ('median_listing_price', 'mean'),
                                                                                                avg_list_price = ('average_listing_price', 'mean'),
                                                                                                active_list_count = ('active_listing_count', 'sum'),
                                                                                                median_DOM = ('median_days_on_market', 'mean'),
                                                                                                new_list_count = ('new_listing_count', 'sum'),
                                                                                                price_increase_count = ('price_increased_count', 'sum'),
                                                                                                price_reduced_count = ('price_reduced_count', 'sum'),
                                                                                                pending_list_count = ('pending_listing_count', 'sum'),
                                                                                                median_list_price_per_square_foot = ('median_listing_price_per_square_foot', 'mean'),
                                                                                                median_square_feet = ('median_square_feet', 'mean'),
                                                                                                total_list_count = ('total_listing_count', 'sum'),
                                                                                                pending_ratio = ('pending_ratio', 'mean')
                                                                                            )
    logging.info('Reading and transforming of Realtor.com data was succesful. Moving to the next data source.')
    
    # Readfin data read and process
    # Reading redfin data from downloaded compressed file.
    redfin_df = pd.read_csv('data/raw_data/redfin_data/zip_code_market_tracker.tsv000.gz', sep='\t')
    logging.info('Redfin data successfully read from the .csv file.')
    # Choosing relevant columns
    column_subset = ['period_end', 'property_type', 'median_sale_price', 'median_list_price', 
                    'median_ppsf', 'homes_sold', 'pending_sales', 'new_listings',  'inventory',
                    'avg_sale_to_list', 'region' ]
    redfin_df_slice = redfin_df[column_subset].copy(deep=True)
    redfin_df = None # Releasing memory.
    # Data reformating/ type conversion
    redfin_df_slice['zip'] = redfin_df_slice.region.str.split(': ', expand=True)[1].astype('int')
    redfin_df_slice.period_end = pd.to_datetime(redfin_df_slice.period_end)
    # Aggregating data on zip code, year.
    redfin_df_slice_zip_agg = redfin_df_slice.groupby(by=['zip', redfin_df_slice.period_end.dt.year]).agg(  median_sale_price = ('median_sale_price', 'mean'),
                                                                                                            median_list_price = ('median_list_price', 'mean'),
                                                                                                            median_ppsf = ('median_ppsf', 'mean'),
                                                                                                            homes_sold = ('homes_sold', 'sum'),
                                                                                                            pending_sales = ('pending_sales', 'sum'),
                                                                                                            new_listings = ('new_listings', 'sum'),
                                                                                                            inventory = ('inventory', 'sum'),
                                                                                                            avg_sale_to_list = ('avg_sale_to_list', 'mean')
                                                                                                         )
    logging.info('Redfin and transforming of Realtor.com data was succesful. Moving to the next data source.')
    
    # Reading zillow Data from downloaded file
    zillow_zhvi_df = pd.read_csv('data/raw_data/zillow_data/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv')
    logging.info("zillow data read succesful.")
    # Selecting columns of interest
    excluded_cols = ['RegionID', 'SizeRank','StateName', 'State','RegionType' ]
    zillow_zhvi_df_slice = zillow_zhvi_df[[x for x in zillow_zhvi_df.columns if x not in excluded_cols]].copy(deep=True)
    zillow_zhvi_df = None # Release memory.
    zillow_zhvi_df_slice = zillow_zhvi_df_slice.set_index(['RegionName', 'City', 'Metro', 'CountyName']).stack().reset_index()
    zillow_zhvi_df_slice.columns = ['zip', 'city', 'metro', 'county', 'date', 'zhvi_usd_dominated']
    # Data type conversion
    zillow_zhvi_df_slice.date = pd.to_datetime(zillow_zhvi_df_slice.date)
    # Aggregate data based on zip code and year
    zillow_zhvi_zip_agg = zillow_zhvi_df_slice.groupby(by=['zip', zillow_zhvi_df_slice.date.dt.year]).mean()
    
    logging.info("Moving to making the master home price dataframe.")
    # Harmonizing column headers for joining
    multi_index_names = ['zip', 'year']
    zip_uad_df_merged.index.names = multi_index_names
    redfin_df_slice_zip_agg.index.names = multi_index_names
    realtor_df_slice_agg.index.names = multi_index_names
    zillow_zhvi_zip_agg.index.names = multi_index_names
    # Adding suffix to column names to keep track of the source in the merged dataframe
    zip_uad_df_merged = zip_uad_df_merged.add_suffix('_uad')
    redfin_df_slice_zip_agg = redfin_df_slice_zip_agg.add_suffix('_redfin')
    realtor_df_slice_agg = realtor_df_slice_agg.add_suffix('_realtor')
    zillow_zhvi_zip_agg = zillow_zhvi_zip_agg.add_suffix('_zillow')

    # Merging dataframe
    dfs_to_merge = [zip_uad_df_merged, redfin_df_slice_zip_agg, realtor_df_slice_agg, zillow_zhvi_zip_agg]
    zip_price_master_df = reduce(lambda  left,right: pd.merge(left,right,left_index=True, right_index=True,
                                                how='outer'), dfs_to_merge)

    # Saving final table as parquet file
    out_dir = 'data/etl_data/zip_year_house_price_table'
    save_pd_to_parquet(dtframe=zip_price_master_df, fldr_name=out_dir, table_name='house_price_table')

    if os.path.exists(f'data/etl_data/zip_year_house_price_table/house_price_table/'):
        return logging.info(f'Master HPA data saved at: data/etl_data/zip_year_house_price_table/house_price_table/')

def generate_zipcode_table(*args, **kwargs):
    # Read data, needs import pandas as pd
    global_postcode_data = pd.read_csv('data/raw_data/geonames/allCountries.zip', sep='\t', low_memory=False, header=None)
    logging.info("GeoNames zipcode data read succesful.")
    column_headers = ['country_code', 'postal_code', 'place_name', 'admin_name1', 'admin_code1', 'admin_name2', 'admin_code2', 'admin_name3', 'admin_code3', 'latitude', 'longitude', 'accuracy']
    global_postcode_data.columns = column_headers
    # Getting only US zip codes
    us_postal_codes = global_postcode_data[global_postcode_data.country_code == 'US'].copy(deep=True)
    global_postcode_data = None # Releasing memory.
    # Selecting relevant columns and renaming for clarity.
    us_postal_codes = us_postal_codes[['postal_code', 'place_name', 'admin_code1', 'latitude', 'longitude', 'accuracy']]
    us_postal_codes.columns = ['zip', 'city', 'state', 'latitude', 'longitude', 'accuracy']
    # Data type conversion
    us_postal_codes.zip = us_postal_codes.zip.astype(int)
    logging.info("GeoNames zipcode data transformation succesful.")
    
    # Reading Metro-City data from Zillow raw data.
    zillow_zhvi_df = pd.read_csv('data/raw_data/zillow_data/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv')
    logging.info("zillow data read succesful.")
    zillow_zhvi_df_slice_zip_unduplicated = zillow_zhvi_df[[ 'RegionName', 'City', 'Metro']].drop_duplicates(subset=['RegionName'])
    zillow_zhvi_df_slice_zip_unduplicated.columns = ['zip','metro', 'county']
    logging.info("Zillow data transformation succesful.")
    # Adding Metro and City names (where present in the Zillow table)
    us_postal_codes_with_names = pd.merge(us_postal_codes, zillow_zhvi_df_slice_zip_unduplicated[[ 'zip','metro', 'county']], on='zip', how='left')
    logging.info("Zipcode-Zillow data join succesful.")

    # Saving final table as parquet file
    out_dir = 'data/etl_data/zipcode_table'
    save_pd_to_parquet(dtframe=us_postal_codes_with_names, fldr_name=out_dir, table_name='zipcode_table')
    logging.info("Zicpcode table save succesful.")

def data_validity_check(*args, **kwargs):
    data_path = kwargs["params"]["data_path"]
    import pandas as pd
    try:
        dataframe = pd.read_parquet(path=data_path)
    except:
        raise FileNotFoundError(f'File not present at {data_path}')
    df_length = dataframe.shape[0]
    if df_length == 0:
        raise ValueError('Dataframe seems to be empty! .shape[0] gave length of 0!!!')
    elif (dataframe.isnull().sum() == df_length).sum() >0 :
        raise ValueError('At least one of the columns have all Null values.')
    else:
        return print(f"No errors raised for parquet data at {data_path}. \nLooks good :)")

dag = DAG('a_MMM_HPA_DAG',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          # template_searchpath = ['/home/workspace/airflow'], 
          # https://stackoverflow.com/questions/59682085/airflow-postgresoperator-only-finds-sql-file-in-the-same-folder-as-the-dag
          # start_date=datetime(2018, 1, 1, 0, 0, 0, 0),
          # end_date=datetime(2018, 12, 1, 0, 0, 0, 0),
          schedule_interval='@monthly'
          # schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

fhfa_uad_data_download = PythonOperator(
    task_id='fhfa_uad_data_download',
    dag=dag,
    python_callable=fetch_data_from_url,
    provide_context=True,
    params={
        "url": 'https://www.fhfa.gov/DataTools/Documents/UAD-Data-Files/UADAggs_tract.zip',
        "file_name": 'UADAggs_tract.zip',
        "folder_name": 'data/raw_data/FHFA-UAD',
    }
)

tract_20_to_tract_2010 = PythonOperator(
    task_id='tract_20_to_tract_10_map_download',
    dag=dag,
    python_callable=fetch_data_from_url,
    provide_context=True,
    params={
        "url": 'https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/tab20_tract20_tract10_natl.txt',
        "file_name": 'tab20_tract20_tract10_natl.txt',
        "folder_name": 'data/raw_data/census',
    }
)

tract_2_zip = PythonOperator(
    task_id='tract_2_zip_map_download',
    dag=dag,
    python_callable=fetch_data_from_url,
    provide_context=True,
    params={
        "url": 'https://www.huduser.gov/portal/datasets/usps/TRACT_ZIP_122021.xlsx',
        "file_name": 'TRACT_ZIP_122021.xlsx',
        "folder_name": 'data/raw_data/HUD-USPS',
    }
)

zip_details = PythonOperator(
    task_id='zip_details_download',
    dag=dag,
    python_callable=fetch_data_from_url,
    provide_context=True,
    params={
        "url": 'http://download.geonames.org/export/zip/allCountries.zip',
        "file_name": 'allCountries.zip',
        "folder_name": 'data/raw_data/geonames',
    }
)

realtor_data = PythonOperator(
    task_id='realtor_data_download',
    dag=dag,
    python_callable=fetch_data_from_url,
    provide_context=True,
    params={
        "url": 'https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Zip_History.csv',
        "file_name": 'RDC_Inventory_Core_Metrics_Zip_History.csv',
        "folder_name": 'data/raw_data/realtor_data',
    }
)

redfin_data = PythonOperator(
    task_id='redfin_data_download',
    dag=dag,
    python_callable=fetch_data_from_url,
    provide_context=True,
    params={
        "url": 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz',
        "file_name": 'zip_code_market_tracker.tsv000.gz',
        "folder_name": 'data/raw_data/redfin_data',
    }
)

zillow_data = PythonOperator(
    task_id='zillow_data_download',
    dag=dag,
    python_callable=fetch_data_from_url,
    provide_context=True,
    params={
        "url": 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1672605804',
        "file_name": 'Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv',
        "folder_name": 'data/raw_data/zillow_data',
    }
)

uad_table_etl = PythonOperator(
    task_id='uad_table_etl_and_save',
    dag=dag,
    python_callable=etl_master_uad,
    provide_context=True
)

hpa_table_lt = PythonOperator(
    task_id='hpa_table_load-transform-join',
    dag=dag,
    python_callable=LT_to_HPA_master_DF,
    provide_context=True
)

get_zip_table = PythonOperator(
    task_id='generate_zipcode_table',
    dag=dag,
    python_callable=generate_zipcode_table,
    provide_context=True
)

house_price_table_validity_check = TableDataQualityOperator(
    task_id='house_price_table_validity_check',
    dag=dag,
    data_path = 'data/etl_data/zip_year_house_price_table/house_price_table/'
)

zipcode_table_validity_check = TableDataQualityOperator(
    task_id='zipcode_table_validity_check',
    dag=dag,
    data_path = 'data/etl_data/zipcode_table/zipcode_table/'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task dependencies

# Tasks to download files
start_operator >> fhfa_uad_data_download
start_operator >> tract_20_to_tract_2010
start_operator >> tract_2_zip
start_operator >> zip_details
start_operator >> realtor_data
start_operator >> redfin_data
start_operator >> zillow_data

# Tasks to prepare final UAD table.
tract_2_zip >> uad_table_etl
fhfa_uad_data_download >> uad_table_etl
tract_20_to_tract_2010 >> uad_table_etl

# ETL of the final house price table
uad_table_etl >> hpa_table_lt
realtor_data >> hpa_table_lt
redfin_data >> hpa_table_lt
zillow_data >> hpa_table_lt

# ETL of zip dtable
zillow_data>> get_zip_table
zip_details>> get_zip_table

# Data validity check
hpa_table_lt >> house_price_table_validity_check
get_zip_table >> zipcode_table_validity_check

# End operator
house_price_table_validity_check >> end_operator
zipcode_table_validity_check >> end_operator