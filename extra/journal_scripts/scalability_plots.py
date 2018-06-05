from subprocess import Popen
from accasim.utils.plot_factory import PlotFactory
from os.path import exists


if __name__ == '__main__':
    dir = 'results/'
    workloadName = 'bench-HPC2N-2002-2.2.1-cln.swf'
    numRuns = 10

    disp_names = ['FIFO-FF', 'FIFO-BF', 'LJF-FF', 'LJF-BF', 'SJF-FF', 'SJF-FF', 'EBF-FF', 'EBF-BF']

    paths = []
    for disp in disp_names:
        curPath = dir + disp + '.csv'
        paths.append(curPath)
        if not exists(curPath):
            for i in range(numRuns):
                filepath = dir + 'Run_' + str(i) + '/' + disp + '/' + workloadName
                command = 'cat ' + filepath + ' >> ' + curPath
                p = Popen(args=command, shell=True)
                p.wait()

    fac = PlotFactory(plot_class='benchmark')
    fac.set_files(paths=paths, labels=disp_names)
    fac.produce_plot(type='scalability', output='Scalability.pdf', smooth=50)
    fac.produce_plot(type='sim_time', output='Simtime.pdf')

    exit(0)
