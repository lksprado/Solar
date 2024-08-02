# THIS SCRIPT IS TO APPEND ALL DATA FROM CSV TO A SINGLE FILE FOR TABLEAU PUBLIC

import os
import pandas as pd
import json

class Gather:
    def __init__(self, config_file = 'src/config.json'):
        self.config_file = config_file
        self.config = self.read_config()
        self.silver_dir = self.config.get('silver_dir')
        self.done_folder = self.config.get('done_folder')
        self.gold_dir = self.config.get('gold_dir')          
    
    # READING CONFIG FILE
    def read_config(self):
        with open(self.config_file, 'r') as file:
            config_data = json.load(file)
        return config_data

    def run(self):        
        li = []
        
        for file in os.listdir(self.done_folder):
            if file.endswith('.csv'):
                df = pd.read_csv(os.path.join(self.done_folder, file), index_col=None, header=0)
                li.append(df)
        
        combined_df = pd.concat(li, axis=0, ignore_index=True)
        
        destination = os.path.join(self.gold_dir, 'all_data.csv')
        combined_df.to_csv(destination, index=False)

if __name__ == "__main__":
    gather = Gather()
    gather.run()    