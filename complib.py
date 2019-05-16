import multiprocessing
import logging
import os
import time

import cdo
import gribapi
import netCDF4

logger = multiprocessing.log_to_stderr(logging.DEBUG)

shvars = ["pt", "mont", "pres", "etadot", "z", "t", "u", "v", "w", "vo", "d", "r"]

level_types = {"surface": "sfc",
               "isobaricInhPa": "pl",
               "isobaricInPa": "pl",
               "hybrid": "ml",
               "depthBelowLandLayer": "sfc"}

temp_dir = os.path.join(os.getcwd(), "tmp")

split_grib = False
post_proc_grib = True


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


def create_nc_diffs(grbvars):
    datasets = {}
    for v in grbvars:
        if v[1] in datasets:
            datasets[v[1]].append(v)
        else:
            datasets[v[1]] = [v]
    for ncpath, vlist in datasets.items():
        dstpath = os.path.join(temp_dir, os.path.basename(ncpath))
        logger.info("Writing validation file %s..." % dstpath)
        with netCDF4.Dataset(ncpath, 'r') as src, netCDF4.Dataset(dstpath, 'w') as dst:
            #dst.setncatts(src.__dict__)
            for name, dimension in src.dimensions.items():
                dst.createDimension(
                    name, (len(dimension) if not dimension.isunlimited() else None))
            for name, variable in src.variables.items():
                dst.createVariable(name, variable.datatype, variable.dimensions)
                #dst.createVariable(ifsname, variable.datatype, variable.dimensions)
                dst[name][:] = src[name][:]
                #dst[name].setncatts(src[name].__dict__)
                for v in vlist:
                    print v
                    if v[0] == name:
                        logger.info("Opening post-processed nc file %s" % v[-1])
                        with netCDF4.Dataset(v[-1], 'r') as orig:
                            ifsname = name + "_ifs"
                            if name in orig.variables.keys():
                                logger.info("dst shape: %s orig shape: %s" % (str(dst[name].shape),
                                                                              str(orig[name].shape)))
                            else:
                                logger.info("variables %s not found in %s" % (name, v[-1]))


def compare_vars(nc_files, grib_files, num_threads):
    ncvars = extract_variables(nc_files)
    tmp_grbs = {}
    if split_grib and post_proc_grib:
        if os.path.exists(temp_dir):
            if any(os.listdir(temp_dir)):
                raise Exception("Temporary working directory exists and is not empty")
        else:
            os.makedirs(temp_dir)
    grbvars = []
    for v in ncvars:
        ncfile = v[1]
        file_atts = os.path.basename(ncfile)[:-3].split('_')
        freq, grid_type, lev_type = file_atts[1], file_atts[2], file_atts[3]
        if grid_type != "regular":
            logger.info("Dismissing variable %s on reduced grid in %s" % (v[0], v[1]))
            continue
        key = (v[0], lev_type)
        fpath = os.path.join(temp_dir, "_".join([v[0], lev_type]) + ".grib")
        if split_grib:
            tmp_grbs[key] = open(fpath, 'w')
        grbvars.append((v[0], v[1], fpath))
    start = time.time()
    for grib_file in grib_files:
        logger.info("Splitting input file %s" % grib_file)
        if split_grib:
            with open(grib_file, 'r') as grib_in:
                while True:
                    record = gribapi.grib_new_from_file(grib_in)
                    if record is None:
                        break
                    varname = str(gribapi.grib_get(record, "shortName"))
                    typel = str(gribapi.grib_get(record, "typeOfLevel"))
                    ltype = level_types.get(typel, "none")
                    if typel == "isobaricInPa":
                        gribapi.grib_release(record)
                        continue
                    ofile = tmp_grbs.get((varname, ltype), None)
                    if ofile is not None:
                        gribapi.grib_write(record, ofile)
                    gribapi.grib_release(record)
    if split_grib:
        for grb in tmp_grbs.values():
            grb.close()
    pool = multiprocessing.Pool(processes=(num_threads if post_proc_grib else 1))
    ncfiles = pool.map(postproc_worker, grbvars)
    end = time.time()
    logger.info("The post-processing loop took %d seconds" % (end - start))
    create_nc_diffs([grbvars[i] + (ncfiles[i],) for i in range(len(grbvars))])


def postproc_worker(vartuple):
    varname, ncfile, grbfile = vartuple[0], vartuple[1], vartuple[2]
    file_atts = os.path.basename(ncfile)[:-3].split('_')
    freq, grid_type, lev_type = file_atts[1], file_atts[2], file_atts[3]
    app = cdo.Cdo()
    logger.info("Processing %s" % grbfile)
    freqopt = None
    if freq == "3h":
        freqopt = "-selhour,0,3,6,9,12,15,18,21"
    elif freq == "6h":
        freqopt = "-selhour,0,6,12,18"
    elif freq == "1d":
        freqopt = "-daymean"
    elif freq == "1m":
        freqopt = "-monmean"
    else:
        logger.warning("Frequency %s not recognized, skipping variable %s from file %s" % (freqopt, varname, ncfile))
        return
    output = grbfile.replace(".grib", "_" + freq + ".nc")
    if post_proc_grib:
        if varname in shvars:
            app.sp2gpl(input=" ".join([freqopt, grbfile]), output=output, options="-f nc ")
        else:
            app.copy(input=" ".join(["-setgridtype, regular", freqopt, grbfile]), output=output, options="-f nc")
    return output
