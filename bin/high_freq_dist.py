#!/bin/python3
import argparse


def arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--cluster', required=True, type=str, default=None,
	    metavar='<str>',
	    help="The path to the cluster table file.")

    parser.add_argument('-m', '--metadata', required=True, type=str, default=None,
        metavar='<str>',
        help="The path to the metadata file.")
    
    args = parser.parse_args()
    
    return args

args = arguments()
print(args.cluster)