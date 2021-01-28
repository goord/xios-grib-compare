#!/usr/bin/env python

import argparse

import complib


def main(args=None):
    if args is None:
        pass
    parser = argparse.ArgumentParser(description="Automatic comparison IFS GRIB and XIOS NetCDF output")
    parser.add_argument("--grb", required=True, type=str, metavar="FILE", help="IFS output grib files (concatenated)")
    parser.add_argument("--nc", required=True, type=str, metavar="FILE.nc", help="XIOS output nc files")

    args = parser.parse_args()
    d = 2
    if args.nc[:-3].endswith("_ml"):
        d = 3

    complib.compare_data(args.grb, args.nc, d)


if __name__ == "__main__":
    main()
