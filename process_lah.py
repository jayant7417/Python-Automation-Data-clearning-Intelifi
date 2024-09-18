import os
import pathlib
import pandas as pd

def process_lah(folder_path):
    dfs_lah = []
    path = folder_path.joinpath('vms', 'lah')
    print(f"Processing VMS files from: {path}")

    for filename in os.listdir(path):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(path, filename)
            df = pd.read_excel(file_path)
            df['Source_File'] = filename
            dfs_lah.append(df)

    if dfs_lah:
        df_lah = pd.concat(dfs_lah, ignore_index=True)
        df_lah['Facility Name'] = df_lah['Facility Name'].astype(str)
        df_lah = df_lah[~df_lah['Requisition Status'].isin(['Filled', 'Canceled'])]
        df_lah = df_lah[~df_lah['Facility Name'].str.startswith('UMMS:')]
        df_lah = df_lah[df_lah['Facility Name'] != 'UConn John Dempsey Hospital']
        df_lah = df_lah[~df_lah['Facility Name'].str.contains('Applied filters:|Disclaimer is|Client Name is|Client_ID is', na=False)]
        
        # Handle non-finite values in 'Requisition ID' before converting to int
        df_lah = df_lah.dropna(subset=['Requisition ID'])  # Optionally, fillna() can be used if needed
        df_lah['Requisition ID'] = df_lah['Requisition ID'].astype(int)
        df_lah['Requisition Status'] = df_lah['Requisition Status'].replace('OnHold', 'On-Hold')
        df_lah = df_lah[['Requisition ID', 'Requisition Status']]
    else:
        print('No VMS Excel files found or processed.')

    dfs_job = []
    path = folder_path.joinpath('job board')
    print(f"Processing VMS files from: {path}")

    for filename in os.listdir(path):
        if filename.endswith('.csv'):
            file_path = os.path.join(path, filename)
            df = pd.read_csv(file_path)
            df['Source_File'] = filename
            dfs_job.append(df)
    
    if dfs_job:
        df_job = pd.concat(dfs_job, ignore_index=True)
        df_job['External Job Posting Id'] = df_job['External Job Posting Id'].fillna('')
        '''df_job['External Job Posting Id'] = df_job['External Job Posting Id'].str.replace(
            '(48 hours)|(48hours)|(48 hrs)|(48hrs)|(48 HOURS)|(48HOURS)|(48 HRS)|(48Hrs)|\'|(48 Hours)|(48Hours)|(48 Hrs)|(48Hrs)|\"|extension|Extension',
            '',
            regex=True
        )'''
        df_job['External Job Posting Id'] = df_job['External Job Posting Id'].str.replace(
            r'(48 hours)|(48hours)|(48 hrs)|(48hrs)|(48 HOURS)|(48HOURS)|(48 HRS)|(48Hrs)|\'|(48 Hours)|(48Hours)|(48 Hrs)|(48Hrs)|\"|extension|Extension|\(|\)|\(\)',
            '',
        regex=True)

        df_job = df_job.dropna(subset=['Job Status'])
        df_job = df_job.drop_duplicates(subset=['External Job Posting Id'])
        
        '''output_file_path = folder_path.joinpath('result', 'temp.csv')
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        df_job.to_csv(output_file_path, index=False)
        print(f"VMS processing done. Output saved to: {output_file_path}")'''
        
        df_job['External Job Posting Id'] = df_job['External Job Posting Id'].astype(int) 
        
        merged_df = pd.merge(df_lah, df_job, left_on='Requisition ID', right_on='External Job Posting Id', how='outer')
        merged_df = merged_df[['Requisition ID', 'Requisition Status', 'Job Status']]

        # Drop duplicates based on 'External Job Posting Id'
        merged_df = merged_df.dropna(subset=['Requisition ID'])
        merged_df['result'] = merged_df['Requisition Status'] == merged_df['Job Status']
        
        
        
        path3 = folder_path.joinpath('do not post', 'do_not_post_lah.csv') 
    
        try:
            df3 = pd.read_csv(path3)
        except FileNotFoundError:
            print(f"File not found: {path3}")
            return
        
        posting = merged_df[merged_df['Job Status'].isnull()]
        posting = posting[['Requisition ID']].astype('int64')
        dnt_ids = df3['Requisition ID'].astype('int64')
        
        posting = posting[~posting['Requisition ID'].isin(dnt_ids)]
        
        output_file_path = folder_path.joinpath('result', 'Posting.csv')
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        posting.to_csv(output_file_path, index=False)
        print(f"VMS processing done. Output saved to: {output_file_path}")
    
        status = merged_df.dropna(subset='Job Status')
        status['Requisition ID'] = status['Requisition ID'].astype(int)
        status = status[status['result'] == False]

        output_file_path = folder_path.joinpath('result', 'Status.csv')
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        status.to_csv(output_file_path, index=False)
        print(f"VMS processing done. Output saved to: {output_file_path}")
        
    if dfs_lah:
        df_lah = pd.concat(dfs_lah, ignore_index=True)
        df_lah['Facility Name'] = df_lah['Facility Name'].astype(str)
        df_lah = df_lah[~df_lah['Requisition Status'].isin(['OnHold', 'Open'])]
        df_lah = df_lah[~df_lah['Facility Name'].str.startswith('UMMS:')]
        df_lah = df_lah[df_lah['Facility Name'] != 'UConn John Dempsey Hospital']
        df_lah = df_lah[~df_lah['Facility Name'].str.contains('Applied filters:|Disclaimer is|Client Name is|Client_ID is', na=False)]
        
        # Handle non-finite values in 'Requisition ID' before converting to int
        df_lah = df_lah.dropna(subset=['Requisition ID'])  # Optionally, fillna() can be used if needed
        df_lah = df_lah[['Requisition ID']].astype(int)
    else:
        print('No VMS Excel files found or processed.')
        
    merged_df = pd.merge(df_job, df_lah, left_on='External Job Posting Id', right_on='Requisition ID', how='outer')
    merged_df = merged_df[['External Job Posting Id', 'Requisition ID']].dropna().astype(int)
    merged_df = merged_df[['Requisition ID']]
    
    output_file_path = folder_path.joinpath('result', 'Closing.csv')
    output_file_path.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(output_file_path, index=False)
    print(f"VMS processing done. Output saved to: {output_file_path}")
'''current_directory = os.getcwd()
folder_path = pathlib.Path(current_directory)

process_lah(folder_path)'''