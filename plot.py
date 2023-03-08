import os
import datetime as dt

import numpy
import h5py
from matplotlib import pyplot as plt
import matplotlib as mpl
import click

"""
Example plotting script
"""

@click.command()
@click.argument('pvname')
def main(pvname):
    filename = 'data/{}.h5'.format(pvname)

    mpl.use('Agg')
    f = h5py.File(filename, 'r')
    v = f['value'][()][1:]  # Skip first element, it's zero
    t = f['time'][()][1:]
    t = [dt.datetime.fromtimestamp(_t) for _t in t]
    print('Data file size: {} bytes, {} MB'.format(os.path.getsize(filename), os.path.getsize(filename) >> 20))
    print('Number of data points: {}'.format(len(v)))
    # print(v)
    # print(t)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(t, v)
    plt.savefig('plots/{}.png'.format(pvname), dpi=300)

if __name__ == "__main__":
    main()
