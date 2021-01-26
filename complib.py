import logging
import os

import gribapi
import netCDF4 as nc
import numpy as np
import pickle as pkl
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
import cartopy.crs as ccrs


logging.basicConfig(level=logging.INFO)

log = logging.getLogger(__name__)

plot_vars = ["2t", "10u", "10v", "msl", "sp", "tclv", "tcww", "tcc", "cp", "lsp", "ci"]
map_vars = ["10v", "sp", "cp"]
prof_vars = ["q"]


def get_xios_step(itim, dims):
    if dims == 2:
        return -1 if itim < 2 else itim - 2
    if dims == 3:
        return (itim - 1)/2
    return -1


def compare_data(gribfile, ncfile, dims=2):
    recvars = set(plot_vars + map_vars)
    dsxios = nc.Dataset(ncfile, 'r')
    step, prevstep, itim, pl = 0, -1, 0, None
    errorbar_result = {}
    map_result = {}
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
                if pl is None:
                    pl = gribapi.grib_get_array(record, "pl")
                fldgrib = gribapi.grib_get_values(record)
                refval = abs(float(gribapi.grib_get(record, "referenceValue")))
                if refval == 0.:
                    refval = 1
                nbits = int(gribapi.grib_get(record, "bitsPerValue"))
                if dims == 3:
                    lev = str(gribapi.grib_get(record, "level"))
                    fldxios = dsxios.variables[varname][jtim, lev, ...]
                    key = (varname, lev)
                else:
                    lev = -1
                    fldxios = dsxios.variables[varname][jtim, ...]
                    key = varname
                absdiffindex = np.argmax(np.abs(fldgrib - fldxios))
                absdiff = fldgrib[absdiffindex] - fldxios[absdiffindex]
                reldiff = absdiff / refval
                resgrib = (gribapi.grib_get(record, "maximum") - gribapi.grib_get(record, "minimum")) / (2 ** nbits * refval)
                if key not in errorbar_result:
                    errorbar_result[key] = (absdiff, reldiff, resgrib, step)
                else:
                    a, r, res, stp = errorbar_result[key]
                    if abs(absdiff) > abs(a):
                        errorbar_result[key] = (absdiff, reldiff, resgrib, step)
                if key in map_vars:
                    if key not in map_result or abs(absdiff) > abs(errorbar_result[key][0]):
                        map_result[key] = fldgrib - fldxios
            prevstep = step
            gribapi.grib_release(record)
    fmt = "{:>20}" * 6 if dims == 3 else "{:>20}" * 5
    if dims == 3:
        log.info(fmt.format("variable", "level", "abs. diff.", "rel. diff.", "grb. res.", "step"))
        for key, item in errorbar_result.items():
            log.info(fmt.format(key[0], key[1], *item))
        plot_error_profs(errorbar_result)
    else:
        log.info(fmt.format("variable", "abs. diff.", "rel. diff.", "grb. res.", "step"))
        for key, item in errorbar_result.items():
            log.info(fmt.format(key, *item))
        plot_error_bars(errorbar_result)
        plot_error_maps(map_result, dsxios)


def plot_error_bars(errorbar_result):
    values = np.array([v[1] for v in errorbar_result.values()])
    errors = np.array([v[2] for v in errorbar_result.values()])
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
    for varname in map_result.keys():
        longname = dsxios.variables[varname].long_name
        units = dsxios.variables[varname].units
        fname = varname + "_interp.pkl"
        if os.path.isfile(fname):
            with open(fname, "rb") as ifile:
                zi = pkl.load(ifile)
        else:
            zi = griddata((lons, lats), map_result[varname], (xi, yi), method='nearest')
            with open(fname, "wb") as ofile:
                pkl.dump(zi, ofile)
        ax = plt.axes(projection=ccrs.PlateCarree())
        plt.gcf().set_size_inches(10, 6)
        plt.pcolormesh(xi, yi, zi, cmap=plt.cm.bwr, transform=ccrs.PlateCarree())
        plt.title(longname + " difference [" + units + "]")
        ax.coastlines()
        plt.colorbar(fraction=0.036, pad=0.04)  # draw colorbar
        plt.savefig(varname + "_map.png", dpi=300)
        plt.clf()
        plt.close()


def plot_error_profs(errorbar_result):
    varnames = set([k[0] for k in errorbar_result.keys()])
    for varname in varnames:
        levs = sorted([k[1] for k in errorbar_result.keys() if k[0] == varname])
        vals = [errorbar_result[(varname, lv)] for lv in levs]
        values = np.array([v[1] for v in vals])
        errors = np.array([v[2] for v in vals])
        plt.style.use("ggplot")
        markers, caps, bars = plt.errorbar(y=list(range(0, len(values))), x=values, xerr=errors, ecolor="darkred",
                                           capsize=8, elinewidth=10, ls='none', marker='_', markeredgecolor="black",
                                           markersize=8)
        [bar.set_alpha(0.5) for bar in bars]
        [cap.set_alpha(0.5) for cap in caps]
        plt.axvline(0, color="black", linestyle=":")
        plt.xscale("symlog", linthreshy=0.00001)
        plt.xlabel("relative error")
#        plt.yticks(list(range(0, len(values))), errorbar_result.keys())
        #    plt.show()
        plt.savefig("profplot.png", dpi=300)
        plt.clf()
        plt.close()
