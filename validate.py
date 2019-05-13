#!/usr/bin/env python

import argparse

import complib


def main(args=None):

    if args is None:
        pass
    parser = argparse.ArgumentParser(description="Automatic comparison IFS GRIB and XIOS NetCDF output")
    parser.add_argument("--grb", required=True, metavar="FILES", type=argparse.FileType('r'), nargs='+',
                        help="IFS output grib files (concatenated)")
    parser.add_argument("--nc", required=True, metavar="FILES", type=argparse.FileType('r'), nargs='+',
                        help="XIOS output nc files")

    args = parser.parse_args()

    complib.compare_vars(args.nc, args.grb, 1)


if __name__ == "__main__":
    main()
