#!/usr/bin/env python

import glob
import complib
import argparse


def main(args=None):

    if args is None:
        pass
    parser = argparse.ArgumentParser(description="Automatic comparison IFS GRIB and XIOS NetCDF output")
    parser.add_argument("--grb", required=True, metavar="FILES", type=str, help="IFS output grib files (concatenated)")
    parser.add_argument("--nc", required=True, metavar="FILES", type=str, help="XIOS output nc files")

    args = parser.parse_args()

    grbfiles = glob.glob(args.grb)
    ncfiles = glob.glob(args.nc)

    complib.compare_vars(ncfiles, grbfiles, 1)
