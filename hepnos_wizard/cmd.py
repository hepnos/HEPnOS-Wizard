# (C) 2021 The University of Chicago
# See COPYRIGHT in top-level directory.


"""
.. module:: cmd
  :synopsis: This package provides an entry for
  calling hepnos-gen-config via command line.

.. moduleauthor:: Matthieu Dorier <mdorier@anl.gov>


"""


import argparse
import sys
from .specs import gen_config

def cmd_gen_config():
    parser = argparse.ArgumentParser(
        prog='hepnos-gen-config',
        description='Generate a configuration file for HEPnOS')
    parser.add_argument('--output', type=str, default='',
                        help='Output file name for JSON configuration')
    parser.add_argument('--address', type=str, required=True,
                        help='Mercury protocol or address (e.g. na+sm)')
    parser.add_argument('--use-progress-xstream', action='store_true',
                        help='Have mercury use a dedicated progress ES')
    parser.add_argument('--num-rpc-xstreams', type=int, default=0,
                        help='Number of ES for RPC handling')
    parser.add_argument('--num-rpc-pools', type=int, default=0,
                        help='Number of RPC pools')
    parser.add_argument('--num-providers', type=int, default=1,
                        help='Number of providers for databases')
    parser.add_argument('--num-queue-providers', type=int, default=0,
                        help='Number of providers for queues')
    parser.add_argument('--num-dataset-databases', type=int, default=1,
                        help='Number of databases for datasets')
    parser.add_argument('--num-run-databases', type=int, default=1,
                        help='Number of databases for runs')
    parser.add_argument('--num-subrun-databases', type=int, default=1,
                        help='Number of databases for subruns')
    parser.add_argument('--num-event-databases', type=int, default=1,
                        help='Number of databases for events')
    parser.add_argument('--num-product-databases', type=int, default=1,
                        help='Number of databases for products')
    parser.add_argument('--database-type', type=str, default='map',
                        help='Type of database')
    parser.add_argument('--database-path-prefix', type=str, default='',
                        help='Path to databases, if required by the backend')
    parser.add_argument('--ssg-group-file', type=str, default='hepnos.ssg',
                        help='SSG file name')
    parser.add_argument('--jx9', action='store_true',
                        help='Produce a jx9 output instead of json')
    args = parser.parse_args()
    gen_config(**vars(args))

if __name__ == '__main__':
    cmd_gen_config()
