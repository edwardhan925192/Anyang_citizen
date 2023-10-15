import pandas as pd
import os
import argparse
import glob
from tqdm import tqdm

def data_preprocess(data_path):
  data = pd.read_csv(data_path)

  column_list = [0,3,4,8,13,14,16,17,18]

  cctv = data.iloc[:,column_list]
  cctv.columns = ['cctv','id','type','speed','x','y','x_coord','y_coord','time']

  cctv = cctv[cctv['type'] == 1]

  # Split the timestamp column based on 'T'
  cctv[['date', 'time_']] = cctv['time'].str.split('T', expand=True)
  cctv = cctv.drop(['time'],axis = 1)

  # Split the time column based on '.'
  cctv[['time_main', 'time_fraction']] = cctv['time_'].str.split('.', expand=True)
  cctv = cctv.drop(['time_'],axis = 1)

  return cctv

def group_and_filter_by_cctv(df):
    grouped = {}

    for cctv_name, group in df.groupby('cctv'):

        # Convert time_fraction to integer for correct comparison
        group['time_fraction'] = group['time_fraction'].astype(int)

        # Get indices of rows with max and min time_fraction for each time_main value
        max_indices = group.groupby('time_main')['time_fraction'].idxmax().tolist()
        min_indices = group.groupby('time_main')['time_fraction'].idxmin().tolist()

        # Combine indices and filter the DataFrame
        unique_indices = set(max_indices + min_indices)
        filtered_group = group.loc[unique_indices].sort_values(by=['time_main', 'time_fraction'])

        grouped[cctv_name] = filtered_group

    return grouped

def save_grouped_data_to_csv(directory, df):
    for cctv_name, group_df in df.groupby('cctv'):
        group_df.to_csv(f'cctv_{cctv_name}.csv', index=False)

def preprocess_and_store_files_in_directory(directory):
    # List all CSV files in the given directory
    all_files = glob.glob(os.path.join(directory, "*.csv"))

    stored_dataframes = []  # Temporary list for storing preprocessed DataFrames

    # Step 1: Preprocess each CSV file
    for file in tqdm(all_files, desc="Processing CSV files"):
        preprocessed_df = data_preprocess(file)
        stored_dataframes.append(preprocessed_df)

    # Step 3: After preprocessing all, concatenate all DataFrames in the list
    concatenated_df = pd.concat(stored_dataframes, ignore_index=True)


    return concatenated_df

def get_largest_row(df):
    df['time_fraction'] = df['time_fraction'].astype(float)
    return df.sort_values('time_fraction', ascending=False).iloc[0]

def group_and_get_largest(df):
    return df.groupby(['cctv', 'id', 'time_main']).apply(get_largest_row).reset_index(drop=True)

def add_person_count(df):
    df['person_count'] = df.groupby(['cctv', 'time_main']).transform('size')
    return df

def process_dataframe(df):
    # Set the 'time_main' column as index
    df.set_index('time_main', inplace=True)

    # Pivot the dataframe such that each 'cctv' value becomes a column
    result = df.pivot(columns='cctv', values='person_count')

    # Fill NaN values with 0
    result.fillna(0, inplace=True)

    # Convert the values to integers since we filled NaN with a float value (0.0)
    result = result.astype(int)

    # Reset the index
    result.reset_index(inplace=True)

    return result

# ======================== Testing for 2 hours only ========================= #

def fill_missing_times(df):

    all_times = [f"{hour:02}{minute:02}{second:02}"
                 for hour in range(2)
                 for minute in range(60)
                 for second in range(60)]

    # Convert this list into a dataframe
    all_times_df = pd.DataFrame({'time_main': all_times})

    # Merge with the original dataframe
    merged_df = pd.merge(all_times_df, df, on='time_main', how='left')

    # Fill NaN values with 0
    merged_df.fillna(0, inplace=True)

    return merged_df

if __name__ == "__main__":
    # Initialize argparse
    parser = argparse.ArgumentParser(description="Process and save grouped CCTV data from a directory of CSV files.")
    parser.add_argument("directory", type=str, help="Path to the directory containing CSV data files.")

    # Parse the command-line arguments
    args = parser.parse_args()

    concatenated_df = preprocess_and_store_files_in_directory(args.directory)
    concatenated_df1 = group_and_get_largest(concatenated_df)
    df2 = add_person_count(concatenated_df1)
    df2 = df2.iloc[:,[0,-4,-3,-1]]
    df3 = df2.drop_duplicates(subset=['cctv', 'date', 'time_main'], keep='first')
    df4 = process_dataframe(df3)
    df5 = fill_missing_times(df4)

    df5.to_csv('density.csv',index = False)
