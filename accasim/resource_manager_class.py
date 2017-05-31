import logging
from copy import deepcopy
from accasim.utils.misc import CONSTANT
import itertools


class resources_class:
    """
        resources class: Stablish the resources, allocate and release their use.
        
    """
    def __init__(self, groups, resources, **kwargs):
        """
            :param groups: define the groups of resources. i.e: {'group_0': {'core': 4, 'mem': 10}, .. }
            :param resources: Stablish the available resources of the system, in terms of number of previous groups. i.e: {'group_0': 32}, This will set 32 nodes of the group_0
            :param **kwargs:
                - node_prefix: This will set the prefix of the node name. The default name is 'node', this name is followed by _(i) where i corresponds to the ith loaded node.
                - available_prefix: This will set the prefix of the available resources. Internal use
                - used_prefix: This will set the prefix of the used resources. Internal use
        """
        self.constants = CONSTANT()
        self.groups = {}
        self.resources = {}
        self.system_resource_types = []
        # self.resources_tree = resource_map(kwargs['groups'])
        self.allocated = {}
        self.node_prefix = kwargs['node_prefix'] if 'node_prefix' in kwargs else 'node_' 
        self.available_prefix = kwargs['available_prefix'] if 'available_prefix' in kwargs else 'a_'
        self.used_prefix = kwargs['used_prefix'] if 'available_prefix' in kwargs else 'u_'

        for group_name, group_values in groups.items():
            self.system_resource_types += filter(lambda x: x not in self.system_resource_types, list(group_values.keys()))
            resource_group = { '%s%s' % (p, attr): q if p == self.available_prefix else 0
                for attr, q in group_values.items() for p in [self.available_prefix, self.used_prefix]
            }
            self.define_group(group_name, resource_group)

        j = 0
        for group_name, q in resources.items():
            for i in range(q):
                _node_name = '%s%i' % (self.node_prefix, j + 1)
                _attrs_values = self.groups[group_name]
                self.resources[_node_name] = deepcopy(_attrs_values)
                # self.resources_tree.add(_node_name, kwargs['groups'][group_name])
                j += 1              

    def define_group(self, name, group):
        assert(isinstance(group, dict))
        assert(name not in self.groups), ('Repreated name group: %s. Select another one.' % (name))
        self.groups[name] = group

    def allocate(self, node_name, **kwargs):
        # TODO: Update using self.system_resource_types
        assert(self.resources), 'The resources must be setted before jobs allocation'
        _resources = self.resources[node_name]
        _used = {}
        for k, v in kwargs.items():
            _rem_attr = _resources['%s%s' % (self.available_prefix, k)] - _resources['%s%s' % (self.used_prefix, k)]
            assert(v <= _rem_attr), 'The event was request {} {}, but there is only {} available.'.format(v, k, _rem_attr)
            _resources['%s%s' % (self.used_prefix, k)] += v
            _used[k] = _rem_attr - v
        # self.resources_tree.update(node_name, _used)

    def release(self, node_name, **kwargs):
        # TODO: Update using self.system_resource_types
        assert(self.resources), 'The resources must be setted before release resources'
        _resources = self.resources[node_name]
        for k, v in kwargs.items():
            _resources['%s%s' % (self.used_prefix, k)] -= v
            assert(_resources['%s%s' % (self.used_prefix, k)] >= 0), 'The event was request to release %i %s, but there is only %i available. It is impossible less than 0 resources' % (v, k, _resources['%s%s' % (self.used_prefix, k)])
        #=======================================================================
        # self.resources_tree.update(node_name, {
        #     attr: (_resources['%s_%s' % (self.available_prefix, attr)] - _resources['%s_%s' % (self.used_prefix, attr)]) for attr in set([attr.split('_')[1] for attr in _resources.keys()])})
        #=======================================================================

    def availability(self):
        # TODO: Update using self.system_resource_types
        assert(self.resources)
        _a = {}
        for node, attrs in self.resources.items():
            _a[node] = {
                attr: (attrs['%s%s' % (self.available_prefix, attr)] - attrs['%s%s' % (self.used_prefix, attr)]) for attr in set([attr.split('_')[1] for attr in attrs])
            }
        return _a

    def usage(self):
        # TODO: Update using self.system_resource_types
        _str = "System usage:\n"
        _str_usage = []
        usage = {k: 0 for k in list(self.resources.values())[0]}
        for attrs in self.resources.values():
            for k, v in attrs.items():
                usage[k] += v
        for _attr in set([attr.split('_')[1] for attr in usage]):
            if usage['%s%s' % (self.available_prefix, _attr)] > 0:
                _str_usage.append("%s: %.2f%%" % (_attr, usage['%s%s' % (self.used_prefix, _attr)] / usage['%s%s' % (self.available_prefix, _attr)] * 100))
        return (_str + ', '.join(_str_usage))

    def system_capacity(self):
        _capacity = {
            r: {'total':
                sum([attrs[self.available_prefix + r] for _, attrs in self.resources.items()]) 
            } 
            for r in self.system_resource_types
        }
        return _capacity        

    def __str__(self):
        _str = "Resources:\n"
        for node, attrs in self.resources.items():
            formatted_attrs = ""
            for attr in set([attr.split('_')[1] for attr in attrs]):
               formatted_attrs += '%s: %i/%i, ' % (attr, attrs['%s%s' % (self.used_prefix, attr)], attrs['%s%s' % (self.available_prefix, attr)])
            _str += '- %s: %s\n' % (node, formatted_attrs)
        return _str

class resource_manager:

    def __init__(self, _resource):
        assert(isinstance(_resource, resources_class)), ('Only %s class is acepted for resources' % resources_class.__name__)
        self.resources = _resource
        self.actual_events = {}

    def allocate_event(self, event, node_names):
        logging.debug('Allocating %s event in nodes %s' % (event.id, ', '.join([node for node in node_names])))
        _resources = event.requested_resources
        _attrs = event.requested_resources.keys()

        unique_nodes = [(t, node_names.count(t)) for t in set(node_names)]

        self.actual_events[event.id] = {
            node_name: { _attr:_resources[_attr] * q for _attr in _attrs} for (node_name, q) in unique_nodes
        }
        for node_name, values in self.actual_events[event.id].items():
            self.resources.allocate(node_name, **values)

    def remove_event(self, id):
        for node_name, values in self.actual_events.pop(id).items():
            self.resources.release(node_name, **values)

    def node_resources(self, *args):
        for arg in args:
            print(arg, self.resources.resources[arg])

    def availability(self):
        return self.resources.availability()

    def resource_types(self):
        return list(set([ resource.split('_')[1]  for group, group_resources in self.resources.groups.items() for resource in group_resources ]))

    def get_nodes(self):
        return list(self.resources.resources.keys())

    def get_total_cores(self):
        return sum([attrs['a_core'] for attrs in self.resources.resources.values()])

    def get_total_gpu(self):
        return sum([attrs['a_gpu'] for attrs in self.resources.resources.values()])

    def get_total_mic(self):
        return sum([attrs['a_mic'] for attrs in self.resources.resources.values()])

    def get_total_mem(self):
        return sum([attrs['a_mem'] for attrs in self.resources.resources.values()])

    def get_base_resources(self):
        prf = 'a'
        base = {}
        for k in self.resource_types():
            s = '_'.join([prf, k])
            base[k] = sum([attrs[s] for attrs in self.resources.resources.values()])
        return base

    def get_used_resources(self):
        prf = 'u'
        used = {}
        for k in self.resource_types():
            s = '_'.join([prf, k])
            used[k] = sum([attrs[s] for attrs in self.resources.resources.values()])
        return used

if __name__ == '__main__':

    class event:
        def __init__(self, id):
            self.id = id
            self.core = 3
            self.gpu = 2
            self.mic = 0
            self.mem = 14
        def update_core(self):
            self.core = 6

        def __str__(self):
            return 'ID: %s, core: %i, gpu: %i, mic: %i and mem: %i' % (self.id, self.core, self.gpu, self.mic, self.mem)

    groups = {
        'g0': {'core': 8, 'gpu':2, 'mic': 0, 'mem': 14},
        'g1': {'core': 8, 'gpu':2, 'mic': 0, 'mem': 14}
    }
    resources = {
        'g0': 2,
        'g1': 3,
    }
    _resource = resources_class(groups=groups, resources=resources)
    print(_resource)
    rm = resource_manager(_resource)
    nodes = ['node_1', 'node_4']
    e = event('e.1')
    rm.allocate_event(e, nodes)
    print(rm.availability())
    rm.remove_event(e.id)
