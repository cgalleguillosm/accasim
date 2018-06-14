import numpy as np


def getTotalTime(filepath):
    f = open(filepath)
    simtime = 0
    disptime = 0
    mems = []
    for line in f:
        attrs = line.split(';')
        simtime += float(attrs[2])
        disptime += float(attrs[3])
        mems.append(float(attrs[5]))
    f.close()

    data = {}
    data['simtime'] = simtime / 1000.0
    data['schedtime'] = disptime / 1000.0
    data['avgmem'] = np.average(np.array(mems))
    data['maxmem'] = np.max(np.array(mems))

    return data


if __name__ == '__main__':
    dir = 'results/'
    workloadName = 'bench-HPC2N-2002-2.2.1-cln.swf'
    numRuns = 10

    disp_names = ['FIFO-FF', 'FIFO-BF', 'LJF-FF', 'LJF-BF', 'SJF-FF', 'SJF-BF', 'EBF-FF', 'EBF-BF']

    for disp in disp_names:
        data = []
        for i in range(numRuns):
            filepath = dir + 'Run_' + str(i) + '/' + disp + '/' + workloadName
            data.append(getTotalTime(filepath))
        print('----- %s -----' % disp)
        simData = [d['simtime'] for d in data]
        print('- Sim Time: %s (std %s)' % (np.average(simData), np.std(simData)))
        dispData = [d['schedtime'] for d in data]
        print('- Dispatching Time: %s (std %s)' % (np.average(dispData), np.std(dispData)))
        manData = [d['simtime'] - d['schedtime'] for d in data]
        print('- Management Time: %s (std %s)' % (np.average(manData), np.std(manData)))
        avgmemData = [d['avgmem'] for d in data]
        print('- Average Memory: %s (std %s)' % (np.average(avgmemData), np.std(avgmemData)))
        maxmemData = [d['maxmem'] for d in data]
        print('- Maximum memory: %s (std %s)' % (np.average(maxmemData), np.std(maxmemData)))

    exit(0)
