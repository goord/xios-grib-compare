#!/usr/bin/env python

import argparse

import complib


def main(args=None):
    if args is None:
        pass
    parser = argparse.ArgumentParser(description="Automatic comparison IFS GRIB and XIOS NetCDF output")
    parser.add_argument("--grb", required=True, type=str, metavar="FILE", help="IFS output grib files (concatenated)")
    parser.add_argument("--nc", required=True, type=str, metavar="FILE.nc", help="XIOS output nc files")
#    parser.add_argument("--var", required=True, type=str, help="variable (short)name")

    args = parser.parse_args()

    complib.compare_data(args.grb, args.nc)


if __name__ == "__main__":
    main()
