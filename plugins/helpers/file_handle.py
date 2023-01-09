def save_pd_to_parquet(dtframe, fldr_name, table_name):
    """
    This function saves dataframe as parquet file at specified folder locations. 
    Input:
        fldr_name: Folder name where data will be saved. Sub-directory supported. For example, you can specificy "destination_folder" or you can specify "destination_folder/yet_another_folder".
        table_name: This is the table name for parquet file(s). Data will be saved in a subdirectory under specified fldr_name with actual .parquet file with a timestamp. For example, 
            if table name is specified as "abc" then the folder organization will be 
                        | - fldr_name
                        | -- abc
                        | ---- abc_{os.timestamp}.parquet
        dtframe: Input dataframe.
    Return(s):
        print statement saying data write was sucessful.
    """
    import os, logging
    from datetime import datetime
    cur_time = str(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
    file_path = fldr_name + '/' + table_name
    if not os.path.isdir(file_path):
        os.makedirs(file_path)
    dtframe.to_parquet(path=f'{fldr_name}/{table_name}/{table_name}_{cur_time}.parquet')
    return logging.info(f'Table:  {table_name} saved.')