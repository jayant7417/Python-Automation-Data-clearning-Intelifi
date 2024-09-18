import glob
import os
from pathlib import Path

def delete_files(folder_path):
    # Define all the paths
    paths = [
        Path(folder_path).joinpath('job board'),
        Path(folder_path).joinpath('result'),
        Path(folder_path).joinpath('vms').joinpath('dna'),
        Path(folder_path).joinpath('vms').joinpath('lah')
    ]
    print("run")
    # Iterate through each path
    for path in paths:
        # Get a list of all files in the current directory
        files = glob.glob(os.path.join(path, '*'))
        
        # Delete each file
        for file in files:
            try:
                os.remove(file)
                print(f"Deleted: {file}")
            except Exception as e:
                print(f"Failed to delete {file}: {e}")