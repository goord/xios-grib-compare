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
    parser.add_argument("--np", metavar="N", default=1, help="Number of parallel threads")

    args = parser.parse_args()

    complib.temp_dir = args.tmpdir
    complib.compare_vars(args.nc, args.grb, num_threads=int(args.np))


if __name__ == "__main__":
    main()
