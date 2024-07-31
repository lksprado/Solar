# THIS SCRIPT IS TO APPEND ALL DATA FROM CSV TO A SINGLE FILE FOR TABLEAU PUBLIC

import os
import pandas as pd
import json

def gather_all(done_folder, output_file):
    li = []
    
    for file in os.listdir(done_folder):
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join(done_folder, file), index_col=None, header=0)
            li.append(df)
    
    combined_df = pd.concat(li, axis=0, ignore_index=True)
    combined_df.to_csv(output_file, index=False)

def main():
    config_file = "/media/lucas/Files/2.Projetos/solar/src/config.json"
    
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    done_folder = config['done_folder']
    output_folder = config['output_folder']
    output_file = os.path.join(output_folder, 'combined_data.csv')
    
    gather_all(done_folder, output_file)

if __name__ == "__main__":
    main()
