from subprocess import Popen
from accasim.utils.plot_factory import PlotFactory
from os.path import exists


if __name__ == '__main__':
    dir = 'results/Run_0/'
    workloadName = 'sched-HPC2N-2002-2.2.1-cln.swf'

    disp_names = ['FIFO-FF', 'FIFO-BF', 'LJF-FF', 'LJF-BF', 'SJF-FF', 'SJF-BF', 'EBF-FF', 'EBF-BF']

    paths = []
    for disp in disp_names:
        curPath = dir + disp + '/' + workloadName
        paths.append(curPath)

    fac = PlotFactory(plot_class='schedule', config='config/HPC2N.config')
    fac.set_files(paths=paths, labels=disp_names)
    fac.produce_plot(type='slowdown', output='slowdown.pdf', scale='log', groups=2)
    fac.produce_plot(type='queue_size', output='queuesize.pdf', scale='log', groups=2)

    exit(0)
