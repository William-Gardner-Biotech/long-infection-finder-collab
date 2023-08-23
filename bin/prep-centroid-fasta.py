#!/usr/bin/env python3

"""
CREATE A CLEAN, ALIGNED CENTROID FASTA FOR DOWNSTREAM USAGE
-----------------------------------------------------------

Vsearch --cluster_fast provides a FASTA of cluster centroid
sequences, which we will use to call a cluster-size-weighted
distance matrix downstream. To do so, the centroids must be 
aligned and all the same length. This script checks whether
the centroids are all the same length, and if they are not,
it aligns them with Clustal Omega. The script also looks for
any vsearch quirks in the FASTA headers, including  cluster
consensus sequences and any pesky "*" symbols. It will do away 
with the consensus sequences and clean the "*" symbols out of 
any remaining records here.
"""

import os
import argparse
import subprocess
import io
import pandas as pd
from Bio import SeqIO
from Bio.Align.Applications import ClustalOmegaCommandline 

def align_centroids(fasta_path: str, threads: int) -> str:
    """
    This function optionally aligns the provided sequences
    depending on whether they are all the same length.

    Args:
    fasta_path: The path to the FASTA file.
    label: A label or prefix for the output file.
    threads: The number of threads to use with Clustal Omega.

    Returns:
    File path to fasta that should be used downstream. This
    is either the same path that was checked as the input
    (keyword argument 'fasta_path'), or a new FASTA that 
    has been aligned with Clustal Omega.
    """

    # Create the seqkit command for checking sequence lengths
    cmd = ["seqkit", "fx2tab", "--length", "--name", fasta_path]

    # Use subprocess.run to execute seqkit and create a tab-delimited sequence
    # length table.
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)

    # Check for errors
    if result.stderr:
        print("Error:", result.stderr)
        return fasta_path
    else:
        # If there's no error, read the output as a tab-delimited file
        seqkit_results = io.StringIO(result.stdout)
        lengths_df = pd.read_csv(seqkit_results, sep="\t")

        # check if the sequences are all the same length. If not, align them
        # with Clustal Omega
        unique_values = lengths_df.iloc[:, -1].nunique()
        if unique_values > 1:

            # multiple sequence lengths indicates that the inputs are not aligned
            new_fasta = "tmp.fasta"
            clustalomega_cline = ClustalOmegaCommandline(infile = fasta_path,
                                                         outfile = new_fasta,
                                                         outfmt = 'fasta',
                                                         threads = threads,
                                                         wrap = 80,
                                                         verbose = True,
                                                         auto = False)
            clustalomega_cline()

            # return the path to the aligned FASTA
            return new_fasta
        else:
            # If input sequences are already aligned, let the script proceed
            # without re-aligning
            return fasta_path
# end align_centroids def

def main(input_path: str, label: str, count: int, threads: int):
    """
    This function parses a FASTA file and removes all records with 
    "consensus" in their defline and then removes an asterisk 
    ("*") symbols in the deflines of the remaining records.

    Args:
    fasta_path: The path to the FASTA file.
    label: A label or prefix for the output file.
    count: An expected count to sanity check with.

    Returns:
    None
    """

    working_fasta = align_centroids(input_path, threads)

    output_filename = f"{label}-aligned-centroids.fasta"

    with open(working_fasta, "r", encoding="utf-8") as infile, open(output_filename, "w", encoding="utf-8") as outfile:
        records = SeqIO.parse(infile, "fasta")
        filtered_records = [record for record in records if "consensus" not in record.description]

        assert len(filtered_records) == count

        for record in filtered_records:
            record.description = record.description.replace(r"*", "")
            record.id = record.id.replace(r"*", "")
            SeqIO.write(record, outfile, "fasta")

        # delete temporary clusalo fasta
        if os.path.exists("tmp.fasta"):
            os.remove("tmp.fasta")


def arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('-f', '--fasta_path', required=True, type=str, default=None,
	    metavar='<str>',
	    help="The path to the FASTA file.")

    parser.add_argument('-l', '--label', required=True, type=str, default=None,
        metavar='<str>',
        help="Label to use when naming the output file.")
    
    parser.add_argument('-c', '--count', required=True, type=int, default=None,
        metavar='<int>',
        help="An expected count of the records in the FASTA.")
    
    parser.add_argument('-t', '--threads', required=True, type=int, default=None,
        metavar='<int>',
        help="The number of processors to use for alignment.")
    
    args = parser.parse_args()
    
    return args
# end main function def

if __name__ == "__main__":

    args = arguments()

    # run main
    main(args.fasta_path, args.label, args.count, args.threads)
