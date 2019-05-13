import os
import multiprocessing
import gribapi

import netCDF4

shvars = ["pt", "mont", "pres", "etadot", "z", "t", "u", "v", "w", "vo", "d", "r"]

level_types = {"sfc": [1, 111], "ml": [109], "pl": [100, 210], "pv": [117]}

temp_dir = os.path.join(os.getcwd(), "tmp")


def extract_variables(ncfiles):
    result = []
    for ncfile in ncfiles:
        ds = netCDF4.Dataset(ncfile)
        for var in ds.variables.keys():
            if var in ["lat", "lon"] or var.startswith("bounds_") or var.startswith("time_"):
                continue
            result.append((var, ncfile))
        ds.close()
    return result


def compare_vars(nc_files, grib_files, num_threads):
    ncvars = extract_variables(nc_files)
    shgrbs = [g for g in grib_files if os.path.basename(g).startswith("ICMSH")]
    shgrb = shgrbs[0] if any(shgrbs) else None
    uagrbs = [g for g in grib_files if os.path.basename(g).startswith("ICMUA")]
    uagrb = uagrbs[0] if any(uagrbs) else None
    sfgrbs = [g for g in grib_files if os.path.basename(g).startswith("ICMGG")]
    sfgrb = sfgrbs[0] if any(sfgrbs) else None
    grbvars = []
    for v in ncvars:
        ncfile = v[1]
        file_atts = os.path.basename(ncfile)[:-3].split('_')
        freq, grid_type, lev_type = file_atts[1], file_atts[2], file_atts[3]
        if v[0] in shvars:
            grbf = shgrb
        elif lev_type != "sfc":
            grbf = uagrb
        else:
            grbf = sfgrb
        grbvars.append(v + (grbf,))
    if os.path.exists(temp_dir):
        if any(os.path.listdir(temp_dir)):
            raise Exception("Temporary working directory exists and is not empty")
    else:
        os.makedirs(temp_dir)
    pool = multiprocessing.Pool(processes=num_threads)
    pool.map(postproc_worker, grbvars)


def postproc_worker(vartuple):
    varname, ncfile, grbfile = vartuple[0], vartuple[1], vartuple[2]
    file_atts = os.path.basename(ncfile)[:-3].split('_')
    freq, grid_type, lev_type = file_atts[1], file_atts[2], file_atts[3]
    filter_levels = level_types[lev_type]
    newfile = os.path.join(temp_dir, "_".join([varname, lev_type]) + ".grib")
    with open(grbfile, 'r') as grib_in, open(newfile, 'w') as grib_out:
        while True:
            record = gribapi.grib_new_from_file(grib_in)
            if record is None:
                break
            if str(gribapi.grib_get_long(record, "shortName")) == varname and \
                    int(gribapi.grib_get_long(record, "indicatorOfTypeOfLevel")) in filter_levels:
                gribapi.write(record, grib_out)
    print "wrote", newfile
