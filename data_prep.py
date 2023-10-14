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

def save_grouped_data_to_csv(directory, grouped_data):
    # Save each grouped DataFrame as a separate CSV file in the given directory
    for cctv_name, group_df in grouped_data.items():
        group_df.to_csv(os.path.join(directory, f'cctv_{cctv_name}.csv'), index=False, mode='a', header=False)  # mode='a' appends data if file exists

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

if __name__ == "__main__":
    # Initialize argparse
    parser = argparse.ArgumentParser(description="Process and save grouped CCTV data from a directory of CSV files.")
    parser.add_argument("directory", type=str, help="Path to the directory containing CSV data files.")

    # Parse the command-line arguments
    args = parser.parse_args()

    # Step 2: Save preprocessed data in a temporary list and Step 4: Concatenate
    concatenated_df = preprocess_and_store_files_in_directory(args.directory)

    # Step 5: Group by cctv and save each as a separate CSV
    save_grouped_data_to_csv(args.directory, concatenated_df)
