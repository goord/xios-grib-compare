import os
import multiprocessing
import resource
import gribapi
import cdo

import netCDF4

shvars = ["pt", "mont", "pres", "etadot", "z", "t", "u", "v", "w", "vo", "d", "r"]

level_types = {"surface": "sfc",
               "isobaricInhPa": "pl",
               "isobaricInPa": "pl",
               "hybrid": "ml",
               "depthBelowLandLayer": "sfc"}

temp_dir = os.path.join(os.getcwd(), "tmp")


def extract_variables(ncfiles):
    result = []
    for ncfile in ncfiles:
        ds = netCDF4.Dataset(ncfile)
        for var in ds.variables.keys():
            if var in ["lat", "lon"] or var.startswith("bounds_") or var.startswith("time_") or "levels" in var:
                continue
            result.append((str(var), ncfile))
        ds.close()
    return result


def compare_vars(nc_files, grib_files, num_threads):
    ncvars = extract_variables(nc_files)
    tmp_grbs = {}
#    if os.path.exists(temp_dir):
#        if any(os.listdir(temp_dir)):
#            raise Exception("Temporary working directory exists and is not empty")
#    else:
#        os.makedirs(temp_dir)
    grbvars = []
    for v in ncvars:
        ncfile = v[1]
        file_atts = os.path.basename(ncfile)[:-3].split('_')
        freq, grid_type, lev_type = file_atts[1], file_atts[2], file_atts[3]
        key = (v[0], lev_type)
        fpath = os.path.join(temp_dir, "_".join([v[0], lev_type]) + ".grib")
#        f = open(fpath, 'w')
#        tmp_grbs[key] = f
        grbvars.append((v[0], v[1], fpath))
#    for grib_file in grib_files:
#        print "Splitting input file", grib_file
#        with open(grib_file, 'r') as grib_in:
#            while True:
#                record = gribapi.grib_new_from_file(grib_in)
#                if record is None:
#                    break
#                varname = str(gribapi.grib_get(record, "shortName"))
#                ltype = level_types.get(str(gribapi.grib_get(record, "typeOfLevel")), "none")
#                if (varname, ltype) in tmp_grbs:
#                    gribapi.grib_write(record, tmp_grbs[(varname, ltype)])
#                gribapi.grib_release(record)
#    for grb in tmp_grbs.values():
#        grb.close()
    pool = multiprocessing.Pool(processes=num_threads)
    pool.map(postproc_worker, grbvars)



def postproc_worker(vartuple):
    varname, ncfile, grbfile = vartuple[0], vartuple[1], vartuple[2]
    file_atts = os.path.basename(ncfile)[:-3].split('_')
    freq, grid_type, lev_type = file_atts[1], file_atts[2], file_atts[3]
    app = cdo.Cdo()
    print "Processing", grbfile
    if varname in shvars:
        app.sp2gpl(input=grbfile, output=grbfile.replace(".grib", ".nc"), options="-f nc")
    else:
        app.copy(input=grbfile, output=grbfile.replace(".grib", ".nc"), options="-f nc -R")
