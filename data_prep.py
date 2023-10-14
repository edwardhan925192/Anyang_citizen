import pandas as pd
import os
import argparse

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

def save_grouped_data_to_csv(data_path):
    directory = os.path.dirname(data_path)  # Extract directory from the data path

    # Preprocess and group the data
    cctv = data_preprocess(data_path)
    grouped = group_and_filter_by_cctv(cctv)

    # Save each grouped DataFrame as a separate CSV file in the same directory as the input data
    for cctv_name, group_df in grouped.items():
        group_df.to_csv(os.path.join(directory, f'cctv_{cctv_name}.csv'), index=False)

if __name__ == "__main__":
    # Initialize argparse
    parser = argparse.ArgumentParser(description="Process and save grouped CCTV data.")
    parser.add_argument("data_path", type=str, help="Path to the input CSV data.")

    # Parse the command-line arguments
    args = parser.parse_args()

    # Use the provided data_path argument
    save_grouped_data_to_csv(args.data_path)

