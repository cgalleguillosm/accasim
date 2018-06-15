import os
from scipy.stats.stats import describe
from math import sqrt

if __name__ == '__main__':
    systems = ('seth', 'ricc', 'mc',)
    
    times = {system: [] for system in systems}
    
    with open('results/times') as f:
        for line in f.readlines():
            values = line.split()
            times[values[0]].append(float(values[2]))
    
    for system in systems:
        folder = 'results/{}/'.format(system)
            
        totals = []
        maxs = []
            
        for file in os.listdir(folder):
            values = []
            with open(os.path.join(folder, file)) as f:
                values = [ float(v) for v in f.readlines()[1:] if float(v) != 0]
            totals.append(describe(values).mean)
            maxs.append(max(values))    
        ddata = describe(totals)
        dmaxs = describe(maxs)
        print('Simulation time for {}'.format(system))
        print('\tAvg: {:.0f}'.format(describe(times[system]).mean))
        print('\tMax (stdev): {:.1f}'.format(sqrt(describe(times[system]).variance)))
        print('Memory Consumption for {}'.format(system))
        print('\tAvg: {:.0f}'.format(ddata.mean))
        print('\tstdev: {:.1f}'.format(sqrt(ddata.variance)))
        print('\tMax (avg): {:.0f}'.format(dmaxs.mean))
        print('\tMax (stdev): {:.1f}'.format(sqrt(dmaxs.variance)))
        print()
