import logging
import os

import gribapi
import netCDF4 as nc
import numpy as np
import pickle as pkl
import json
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
import cartopy.crs as ccrs


logging.basicConfig(level=logging.INFO)

log = logging.getLogger(__name__)

plot_vars = ["2t", "10u", "10v", "msl", "sp", "tclv", "tcww", "tcc", "cp", "lsp", "ci"]
map_vars = ["10v", "sp", "cp"]
prof_vars = ["q", "clwc", "cc"]


def get_xios_step(itim, dims):
    if dims == 2:
        return -1 if (itim < 2) else (itim - 2)
    if dims == 3:
        return -1 if (itim % 2 == 0) else ((itim - 3)/2)
    return -1


def get_cached_results(directory, plotvars, mapvars):
    plotcache = {}
    for v in plotvars:
        filepath = os.path.join(directory, v + "_errs.json")
        if os.path.isfile(filepath):
            with open(filepath) as ifile:
                plotcache[v] = json.load(ifile)
    mapcache = {}
    for v in mapvars:
        filepath = os.path.join(directory, v + "_interp.pkl")
        if os.path.isfile(filepath):
            with open(filepath, "rb") as ifile:
                mapcache[v] = pkl.load(ifile)
    return plotcache, mapcache



def write_cache(directory, dims, errbars, mapvars):
    if dims == 3:
        for v in set([k[0] for k in errbars.keys()]):
            keys = sorted([k for k in errbars.keys() if k[0] == v])
            result = {"levels": [k[1] for k in keys], 
                      "absdiff": [errbars[k][0] for k in keys], 
                      "resol": [errbars[k][1] for k in keys], 
                      "refval": [errbars[k][2] for k in keys]}
            filepath = os.path.join(directory, v + "_errs.json")
            with open(filepath, 'w') as ofile:
                json.dump(result, ofile)
    else:
        for v in errbars.keys():
            filepath = os.path.join(directory, v + "_errs.json")
            result = {"absdiff": errbars[v][0], "resol": errbars[v][1], "refval": errbars[v][2]}
            with open(filepath, 'w') as ofile:
                json.dump(result, ofile)
        for v in mapvars.keys():
            filepath = os.path.join(directory, v + "_interp.pkl")
            with open(filepath, "wb") as ofile:
                pkl.dump(mapvars[v], ofile)



def compare_data(gribfile, ncfile, dims=2):
    errorbar_result = {}
    map_result = {}

    if dims == 3:
        plot_cache, map_cache = get_cached_results(os.path.dirname(ncfile), prof_vars, [])
        recvars = [v for v in prof_vars if v not in plot_cache.keys()]
    else:
        plot_cache, map_cache = get_cached_results(os.path.dirname(ncfile), plot_vars, map_vars)
        vars2d = [v for v in plot_vars if v not in plot_cache.keys()]
        vars2dmap = [v for v in map_vars if v not in map_cache.keys()]
        recvars = set(vars2d + vars2dmap)

    read_data(recvars, gribfile, ncfile, errorbar_result, map_result, dims)

    write_cache(os.path.dirname(ncfile), dims, errorbar_result, map_result)
    
    if dims == 3:
        for v, vals in plot_cache.items():
            levels, absdiff, resol, refval = vals["levels"], vals["absdiff"], vals["resol"], vals["refval"]
            for i in range(len(levels)):
                errorbar_result[(v, levels[i])] = (absdiff[i], resol[i], refval[i])
    else:
        for v, vals in plot_cache.items():
            absdiff, resol, refval = vals["absdiff"], vals["resol"], vals["refval"]
            errorbar_result[v] = (absdiff, resol, refval)
        map_result.update(map_cache)

    dsxios = nc.Dataset(ncfile, 'r')
    fmt = "{:>20}" * 5 if dims == 3 else "{:>20}" * 4
    if dims == 3:
        log.info(fmt.format("variable", "level", "abs. diff.", "grb. err.", "ref. val."))
        for key, item in sorted(errorbar_result.iteritems()):
            log.info(fmt.format(key[0], key[1], *item))
        plot_error_profs(errorbar_result, dsxios)
    else:
        log.info(fmt.format("variable", "abs. diff.", "grb. err.", "ref. val."))
        for key, item in errorbar_result.items():
            log.info(fmt.format(key, *item))
        plot_error_bars(errorbar_result)
        plot_error_maps(map_result, dsxios)



def read_data(recvars, gribfile, ncfile, errorbar_result, map_result, dims):
    if not any(recvars):
        return
    dsxios = nc.Dataset(ncfile, 'r')
    step, prevstep, itim, pl = 0, -1, 0, None
    with open(gribfile) as grb:
        while True:
            record = gribapi.grib_new_from_file(grb)
            if record is None:
                break
            step = gribapi.grib_get(record, "stepRange")
            if '-' in step:
                step = int(step.split('-')[1])
            else:
                step = int(step)
            varname = str(gribapi.grib_get(record, "shortName"))
            if prevstep != step:
                itim += 1
            jtim = get_xios_step(itim, dims)
            if jtim > 0 and varname in dsxios.variables and varname in recvars:
                #print "using",varname,"at step",step,"comparing to entry",jtim
                if pl is None:
                    pl = gribapi.grib_get_array(record, "pl")
                fldgrib = gribapi.grib_get_values(record)
                refval = abs(float(gribapi.grib_get(record, "referenceValue")))
                if refval == 0.:
                    refval = 1
                nbits = int(gribapi.grib_get(record, "bitsPerValue"))
                if jtim < dsxios.variables[varname].shape[0]:
                    if dims == 3:
                        lev = int(gribapi.grib_get(record, "level"))
                        fldxios = dsxios.variables[varname][jtim, lev - 1, ...]
                        key = (varname, lev)
                    else:
                        lev = -1
                        fldxios = dsxios.variables[varname][jtim, ...]
                        key = varname
                    absdiffindex = np.argmax(np.abs(fldgrib - fldxios))
                    #print "mean rel. diff is....", np.median(np.abs(fldgrib - fldxios)/fldgrib),"...level:",lev
                    absdiff = fldgrib[absdiffindex] - fldxios[absdiffindex]
                    resgrib = (gribapi.grib_get(record, "maximum") - gribapi.grib_get(record, "minimum")) / (2 ** nbits)
                    if key not in errorbar_result:
                        errorbar_result[key] = (absdiff, resgrib, refval)
                    else:
                        a, r, res = errorbar_result[key]
                        if abs(absdiff) > abs(a):
                            errorbar_result[key] = (absdiff, resgrib, refval)
                    if key in map_vars:
                        if key not in map_result or abs(absdiff) > abs(errorbar_result[key][0]):
                            map_result[key] = fldgrib - fldxios
            prevstep = step
            gribapi.grib_release(record)
    if any(map_result):
        lats = dsxios.variables["lat"][...]
        lons = dsxios.variables["lon"][...]
        newlats = np.array(list(sorted(set(lats))))
        newlons = np.linspace(0, 360, 2 * len(newlats))
        xi, yi = np.meshgrid(newlons, newlats)
        for key in map_result.keys():
            zi = griddata((lons, lats), map_result[key], (xi, yi), method='nearest')
            map_result[key] = zi
    dsxios.close()
            


def plot_error_bars(errorbar_result):
    values = np.array([v[0]/v[2] for v in errorbar_result.values()])
    errors = np.array([v[1]/v[2] for v in errorbar_result.values()])
    plt.style.use("ggplot")
    markers, caps, bars = plt.errorbar(x=list(range(0, len(values))), y=values, yerr=errors, ecolor="darkred",
                                       capsize=8, elinewidth=10, ls='none', marker='_', markeredgecolor="black",
                                       markersize=8)
    [bar.set_alpha(0.5) for bar in bars]
    [cap.set_alpha(0.5) for cap in caps]
    plt.axhline(0, color="black", linestyle=":")
    plt.yscale("symlog", linthreshy=0.00001)
    plt.ylabel("relative error")
    plt.xticks(list(range(0, len(values))), errorbar_result.keys())
    #    plt.show()
    plt.savefig("boxplot.png", dpi=300)
    plt.clf()
    plt.close()


def plot_error_maps(map_result, dsxios):
    lats = dsxios.variables["lat"][...]
    lons = dsxios.variables["lon"][...]
    newlats = np.array(list(sorted(set(lats))))
    newlons = np.linspace(0, 360, 2 * len(newlats))
    xi, yi = np.meshgrid(newlons, newlats)
    for varname, zi in map_result.items():
        longname = dsxios.variables[varname].long_name
        units = dsxios.variables[varname].units
        ax = plt.axes(projection=ccrs.PlateCarree())
        plt.gcf().set_size_inches(10, 6)
        plt.pcolormesh(xi, yi, zi, cmap=plt.cm.bwr, transform=ccrs.PlateCarree())
        plt.title(longname + " difference [" + units + "]")
        ax.coastlines()
        plt.colorbar(fraction=0.036, pad=0.04)  # draw colorbar
    #    plt.show()
        plt.savefig(varname + "_map.png", dpi=300)
        plt.clf()
        plt.close()


def plot_error_profs(errorbar_result, dsxios):
    varnames = set([k[0] for k in errorbar_result.keys()])
    for varname in varnames:
        longname = dsxios.variables[varname].long_name
        units = dsxios.variables[varname].units
        levs = sorted([k[1] for k in errorbar_result.keys() if k[0] == varname])
        vals = [errorbar_result[(varname, lv)] for lv in levs]
        values = np.array([v[0] for v in vals])
        errors = np.array([v[1] for v in vals])
        plt.style.use("ggplot")
        plt.errorbar(y=levs, x=values, xerr=errors, ls='none', elinewidth=2.5, color="darkred", capsize=1, capthick=1, alpha=0.5)
        plt.axvline(0, color="black", linestyle=":")
        plt.xscale("symlog", linthreshx=1.e-11)
        plt.xlabel(longname + " difference [" + units + "]")
        plt.ylabel("model level")
        plt.gca().invert_yaxis()
#        plt.show()
        plt.savefig("prof_" + varname + ".png", dpi=300)
        plt.clf()
        plt.close()
