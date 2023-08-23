#!/usr/bin/env python3

"""
REPORT REPEAT LINEAGE SEQUENCES AND METADATA
--------------------------------------------

Accessions flagged in this script clustered together in the 
so-called "meta-clustering", where high-distance sequences
were run through vsearch --cluster_fast with an even tighter
identity threshold, 99.99%. Sequences that cluster together
are likely to represent the same, high-distance viral lineage
that is persistently infecting one host, though other scenarios
are possible.
"""

import argparse
import pandas as pd
import Bio.SeqIO
import pyarrow as arrow

def main(cluster_table_path: str, metadata_path: str):
    """
    This function reads in a cluster table and metadata file,
    and then compiles FASTA sequences for each repeat cluster and
    collates metadata about these new clusters.

    Args:
    cluster_table_path: The path to the cluster table file.
    metadata_path: The path to the metadata file.

    Returns:
    None
    """
    
    cluster_df = pd.read_csv(cluster_table_path,
                            sep='\t',
                            header=None,
                            na_values=["", "NA"])
    cluster_df = cluster_df.loc[cluster_df[0] != "C"]
    cluster_df = cluster_df.groupby(1)
    cluster_df = cluster_df.filter(lambda x: len(x) > 1)
    cluster_df = cluster_df.sort_values(1)

    # Check that each accession is only present in one cluster
    assert cluster_df[8].nunique() == len(cluster_df)

    # Compile FASTA sequences for each cluster
    for cluster in set(cluster_df[1].tolist()):
        fasta_seq = list(Bio.SeqIO.parse(f"meta-cluster-seqs{cluster}", "fasta"))
        with open(f"repeat-lineage-{cluster}.fasta", "w", encoding="utf-8") as outfile:
            Bio.SeqIO.write(fasta_seq, outfile, "fasta")

    # Create new table to store accessions and repeat cluster number
    repeat_clusts = pd.DataFrame({"Accession": cluster_df[8],
                                "Repeat Cluster Number": cluster_df[1]})

    # Collate metadata about these new clusters
    metadata = pd.read_csv(metadata_path,
                        delimiter='\t',
                        na_values=["", "NA"])
    merged_metadata = pd.merge(metadata, repeat_clusts, on="Accession", how="inner")
    merged_metadata = merged_metadata.loc[~merged_metadata["Repeat Cluster Number"].isna()]
    merged_metadata = merged_metadata.sort_values("Repeat Cluster Number")
    merged_metadata = merged_metadata.drop(["Distance_Score", "Cluster_Size"], axis=1)

    # Write repeat lineage metadata
    merged_metadata.to_csv("repeat-lineage-metadata.tsv",
                        na_rep="",
                        index=False,
                        sep="\t")
# end def main

# Made a function to handle argparse. Also allows assigning in terminal to hopefully imporve readability
# python report-repeat-lineages.py -c 'clusterfile.cluster'

def arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--cluster_table_path', required=True, type=str, default=None,
	    metavar='<str>',
	    help="The path to the cluster table file.")

    parser.add_argument('-m', '--metadata_path', required=True, type=str, default=None,
        metavar='<str>',
        help="The path to the metadata file.")
    
    args = parser.parse_args()
    
    return args

if __name__ == "__main__":
    args = arguments()
    main(args.cluster_table_path, args.metadata_path)
