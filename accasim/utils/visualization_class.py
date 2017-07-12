"""
MIT License

Copyright (c) 2017 cgalleguillosm

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.cm as mplcm
import matplotlib.colors as colors
import time
import matplotlib.animation as Animation
from accasim.utils.misc import sorted_object_list, str_datetime
from collections import namedtuple
from threading import Thread

class scheduling_visualization:
    
    def __init__(self, datasource, system_info, min_time=10 * 60, max_time=30 * 60):
        self.datasource = datasource
        self.system_info = system_info
        self.min_time = min_time
        self.max_time = max_time
        self.job_patches = {}
        self.job_object = {}
        self.job_resouce_type = {}
        self.sorted_jobs = {}
        self.job_color = {}
        for key in self.system_info.keys():
            self.job_resouce_type[key] = namedtuple('Resource_{}'.format(key), ['id', key, 'expected_duration'])
            self.sorted_jobs[key] = sorted_object_list({'main': key, 'break_tie':'expected_duration'}, None)
        self.figure = None
        self.animation = None
        self.axis_mapper = None
        self.color_pos = None
        self.last_line = {'time': None}
        self.color_blacklist = ['red']
        self.canvas = None
        
        # Create initial structure for ploting
        self.set_colors()
        self.running = False

    def start(self):
        self.running = True
        self.create_empty_map()

    def stop(self):
        if self.running:
            self.running = False
            try:
                self.animation._stop()
                self.animation.repeat = False
            except AttributeError as e:
                print(e)
        plt.close()        
        
    def load_new_jobs(self, current_time, running_jobs, _resources):
        for _id, _job in running_jobs.items():
            if _id in self.job_patches:
                continue
            self.job_object[_id] = _job
            for res in _resources:
                # self.sorted_jobs[res].add(self.job_resouce_type[res](_id, _job.get_total_requested_resource()[res], _job.expected_duration))
                self.sorted_jobs[res].add(self.job_resouce_type[res](_id, getattr(_job, res), _job.expected_duration))
        
    def job_draw(self, i):
        running_jobs = self.datasource['running_jobs']
        current_time = self.datasource['current_time']
        if current_time is None:
            self.stop()
            return
        self.xlimit(current_time)
        if not running_jobs:
            _finished = list(self.job_patches.keys())
            if _finished:
                self.update_finished_jobs(current_time, _finished)
            for res, ax in self.axis_mapper.items():
                self.modify_legend(ax, res) 
                self.update_ylabel(ax, res, 0)            
            
            return 
            
        _resources = self.system_info.keys()
        self.start_line_draw(current_time)
        
        # Load new jobs 
        self.load_new_jobs(current_time, running_jobs, _resources)
                
        # Remove jobs already finished
        _finished = [_id for _id in self.job_patches.keys() if _id not in running_jobs ]  # filter(lambda x: x not in running_jobs, list(self.job_patches.keys()))
        if _finished:
            self.update_finished_jobs(current_time, _finished)
            
        # Create new Rectangles or update its position (By increasing consumption)
        _stop_f = False
        for res in _resources:
            ax = self.axis_mapper[res]
            # Sorted by consumption
            _sorted_jobs = self.sorted_jobs[res].get_reversed_object_list()
            # Start from 0
            self.system_info[res]['used'] = 0
            
            for sjob in _sorted_jobs:
                _id = sjob.id
                job = self.job_object[_id]
                j_used = getattr(sjob, res)
                if j_used == 0:
                    # If 0 does not exists the patch
                    continue
                # Check if already exists
                if _id in self.job_patches:
                    _patches = self.job_patches[_id]
                    # Check if the patch exists
                    if res in _patches:
                        self.update_running_job(_patches, res, j_used)
                        continue
                else:
                    self.job_patches[_id] = {}
                    self.job_color[_id] = self.next_color()                
                
                a_used = self.system_info[res]['used']  # / self.system_info[res]['total']
                # _min, _max = self.job_normalization(current_time, sjob)
                r = patches.Rectangle(
                    (job.start_time, a_used / self.system_info[res]['total']), sjob.expected_duration, j_used / self.system_info[res]['total'],
                    fill=True, linewidth=0, edgecolor='black', facecolor=self.job_color[_id], linestyle='dashed', label=_id
                )                 
                self.job_patches[_id][res] = r 
                self.system_info[res]['used'] += j_used
                ax.add_patch(r)        
                 
            self.modify_legend(ax, res)
            self.update_ylabel(ax, res, self.system_info[res]['used'])
        plt.rcParams.update({'font.size': 16})
        self.figure.canvas.draw()
        self.figure.canvas.flush_events()             
        
    def update_ylabel(self, _ax, _res, _used):
        _res_usage = _used / self.system_info[_res]['total']
        _ax.set_ylabel('{} {:.2%}'.format(_res, _res_usage), rotation=90, size=20)
        
    def modify_legend(self, ax, res, legend_size=10):
        h, l = ax.get_legend_handles_labels()
        # L.get_texts()[0].set_text('make it short')
        if len(l) <= legend_size:
            ax.legend(h, l, prop={'size':10}, loc='upper right', title='Job id')
            return
        _generated_legend = {l: h for h, l  in zip(h, l)}
        _modified_legend = []
        for _idx, _job in enumerate(self.sorted_jobs[res].get_reversed_object_list()):
            if _idx == legend_size:
                break
            _modified_legend.append((str(_job.id), _generated_legend[str(_job.id)]))
        l, h = zip(*_modified_legend)
        ax.legend(h, l, prop={'size':12}, loc='upper left')
        
    def update_running_job(self, _patches, _res, _used):
        _patch = _patches[_res]
        _patch.set_y(self.system_info[_res]['used'] / self.system_info[_res]['total'])
        self.system_info[_res]['used'] += _used 
        # _patch.set_height(self.system_info[_res]['used'] / self.system_info[_res]['total'])
        
    def update_finished_jobs(self, current_time, _finished_jobs):
        for _id in _finished_jobs:
            self.remove_job(_id)
        
    def remove_job(self, id):
        _dict_patches = self.job_patches.pop(id, {})
        
        for res, _list in self.sorted_jobs.items(): 
            _job = _list.pop(id=id)
            self.system_info[res]['used'] -= getattr(_job, res)
            if res in _dict_patches:
                _dict_patches[res].remove()

        del self.job_color[id]
        del self.job_object[id]
        
    def job_normalization(self, actual_time, job):
        _min = self.min_time
        _max = self.max_time
        _job_start = (actual_time - job.start_time)
        _job_end = (job.start_time + job.expected_duration - actual_time)
        if _job_start > _min:
             _min = _job_start
        if _job_end < _max:
            _max = _job_end
        return (_min, _max)
    
    def start_line_draw(self, actual_time):
        if self.last_line['time'] == actual_time:
            return
        self.last_line['time'] = actual_time
        _text = self.last_line.pop('text', None)
        if _text is not None:
            _text.remove()
        self.last_line['text'] = self.figure.text(0.62, 0.05, 'Actual time: {}'.format(str_datetime(actual_time)), fontsize=12, backgroundcolor='white')
        for name, ax in self.axis_mapper.items():
            _old = self.last_line.pop(name, None)
            if _old is not None:
                _old.remove()
            self.last_line[name] = ax.axvline(x=actual_time, color='red')
        
            
    def create_empty_map(self):
        sys_config = self.system_info
        for _k in sys_config:
            sys_config[_k]['used'] = 0
        types = len(sys_config)
        self.figure, axis = plt.subplots(types, 1, sharex=True, sharey=True, figsize=(10, 8))
        sys_plot = {}
        for (ax, _sys) in zip(axis, sys_config):
            self.xlimit(0, ax)
            ax.set_ylabel(_sys, rotation=90, size=12)  # , size='large')
            ax.grid()
            xticks = ax.yaxis.get_major_ticks() 
            xticks[0].label1.set_visible(False)
            xticks[-1].label1.set_visible(False)
            sys_plot[_sys] = ax            

            for label in (ax.get_xticklabels() + ax.get_yticklabels()):
                label.set_fontsize(16)
        self.axis_mapper = sys_plot
        self.figure.subplots_adjust(hspace=0)
        plt.setp([a.get_xticklabels() for a in self.figure.axes[:-1]], visible=False)
        self.animation = Animation.FuncAnimation(self.figure, self.job_draw, interval=1000)
        
        plt.rcParams.update({'font.size': 16})
        plt.show()
                    
    def xlimit(self, actual_time, ax=None):
        if ax is None:         
            for _, ax in self.axis_mapper.items():
                self._set_xlimit(ax, actual_time)
        else: 
            self._set_xlimit(ax, actual_time)
            
    def _set_xlimit(self, ax, actual_time):
        if not actual_time:
            return
        self.xlim = (actual_time - self.min_time, actual_time + self.max_time)
        ax.set_xlim(self.xlim)
        labels = ['-600', '-{}'.format(actual_time % 600), '+600', '+1200', '+1800' ]
        ax.set_xticklabels(labels)
            
    
    def next_color(self):
        if self.color_pos is None: 
            self.color_pos = len(self.colors) // 2
        while True:
            _next_pos = self.color_pos
            self.color_pos += 1
            if self.color_pos == len(self.colors):
                self.color_pos = 0 
            if self.colors[_next_pos] not in self.color_blacklist:
                break
        return self.colors[_next_pos] 
        
    def set_colors(self):
        self.colors = [name for name, hex in colors.cnames.items()]
