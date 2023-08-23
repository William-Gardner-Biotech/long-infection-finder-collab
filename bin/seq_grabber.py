#!/bin/python3

import time
import os
import sqlite3
from Bio import SeqIO

def build_seq_dict():
    seq_ID_dict = {}
    data_path = f'{os.getcwd()}/candidate_summary_data/'
    for filename in os.listdir(data_path):
        if 'cluster-seq' in filename:
            #print(filename)
            with open(os.path.join(data_path, filename),'r') as file:
                current_id = None  # Initialize the current ID
                for line in file:
                    line = line.strip()
                    if line.startswith('>'):
                        current_id = line[1:]  # Remove the '>' character
                        if current_id not in seq_ID_dict.keys():
                            seq_ID_dict[current_id] = filename
                    current_id = None
    
    return seq_ID_dict

# Initiate a SQLite database to store the realtional data

db_path = "SEQ_ID_to_loc.db"
conn = sqlite3.connect(db_path)

cursor = conn.cursor()

cursor.execute('''
    DROP TABLE IF EXISTS SeqID_to_file
''')
conn.commit()

def insert_data(tup_list):
    # Insert the whole list using execute many
    cursor.executemany('''
        INSERT INTO SeqID_to_file (id, file)
        VALUES (?, ?)
    ''', tup_list)
    conn.commit()

# primary key is the sequence ID which is unique as per dictionary build rules
# It is possible to also have this db exist then just get the new cluster data inserted every month, but that is a matter of milliseconds
def dict_to_db(seq_dict):
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS SeqID_to_file (
        id TEXT PRIMARY KEY,
        file TEXT)''')
    conn.commit()

    # Create a list of tuples from your dictionary to easily insert into SQL
    data_to_insert = [(key, value) for key, value in seq_dict.items()]

    insert_data(data_to_insert)

# This will get refence cluster file to then open and grab the sequence from
def get_location_by_id(id_to_find):
    cursor.execute('''
        SELECT file FROM SeqID_to_file WHERE id = ?
    ''', (id_to_find,))
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        return None
    
# Function takes the seq_ID and associated file location then parses through the file and grabs the sequence
# It then appends this sequence onto a multifasta file to create the final file
# Important note, ensure that there isn't already a output as it will be appending to it
def grab_seq(seq_ID, file_location):
    data_folder = f'{os.getcwd()}/candidate_summary_data/'
    input_file = os.path.join(data_folder, file_location)
    with open(input_file, "r") as input_handle, open('Output_high_candidate.fasta', "a") as output_handle:
        for record in SeqIO.parse(input_handle, "fasta"):
                if record.id in seq_ID:
                    SeqIO.write(record, output_handle, "fasta")
        pass

start = time.time()
seq_ID_dict = build_seq_dict()
end = time.time()

print(end - start)

dict_to_db(seq_ID_dict)

desired_seqs = ['MW585820.1', 'MW638639.1', 'MW640157.1', 'MW640159.1', 'MW667309.1', 'MW640229.1']

for seq in desired_seqs:
    location = get_location_by_id(seq)
    grab_seq(seq, location)

# Ensure at the end of running main you close the connection
conn.close()

'''for k, v in seq_ID_dict.items():
    print(type(k))'''
