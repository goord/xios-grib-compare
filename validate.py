#!/usr/bin/env python

import argparse

import complib


def main(args=None):

    if args is None:
        pass
    parser = argparse.ArgumentParser(description="Automatic comparison IFS GRIB and XIOS NetCDF output")
    parser.add_argument("--grb", required=True, metavar="FILES", nargs='+', help="IFS output grib files (concatenated)")
    parser.add_argument("--nc", required=True, metavar="FILES", nargs='+', help="XIOS output nc files")
    parser.add_argument("--tmpdir", metavar="DIR", default="./tmp", help="Temporary directory")

    args = parser.parse_args()

    complib.temp_dir=args.tmpdir
    complib.compare_vars(args.nc, args.grb, num_threads=1)


if __name__ == "__main__":
    main()
