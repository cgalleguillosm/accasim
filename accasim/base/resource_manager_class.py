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
import logging
from copy import deepcopy
from accasim.utils.misc import CONSTANT, FrozenDict

class resources_class:
    """
    
        resources class: Stablish the resources, allocate and release their use.
        
    """
    ON = 1
    OFF = 0
    
    def __init__(self, groups, resources, **kwargs):
        """
        
        :param groups: define the groups of resources. i.e: {'group_0': {'core': 4, 'mem': 10}, .. }
        :param resources: Stablish the available resources of the system, in terms of number of previous groups. i.e: {'group_0': 32}, This will set 32 nodes of the group_0
        :param \*\*kwargs:
            - node_prefix: This will set the prefix of the node name. The default name is 'node', this name is followed by _(i) where i corresponds to the ith loaded node.
            - available_prefix: This will set the prefix of the available resources. Internal use
            - used_prefix: This will set the prefix of the used resources. Internal use
        
        """
        self.constants = CONSTANT()
        self.groups = {}
        self.resources = {}
        self.resources_status = {}
        self.system_resource_types = []
        self.system_total_resources = None
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
                self.resources_status[_node_name] = self.ON
                j += 1              

    def total_resources(self):
        """
        Total system resources
        
        :return: A dictionary with the resources and its values.
            
        """
        if self.system_total_resources:
            return self.system_total_resources
        avl_types = {_type: 0 for _type in self.system_resource_types}
        for _node, _node_values in self.resources.items():
            for _type in avl_types.keys():
                avl_types[_type] += _node_values[self.available_resource_key(_type)]
        self.system_total_resources = FrozenDict(**avl_types)
        return avl_types

    def define_group(self, name, group):
        """
        
         Internal method for defining groups of resources.
         
         :param name: Name of the group
         :param group: Values of the group. As defined in the system config.  
        
        """
        assert(isinstance(group, dict))
        assert(name not in self.groups), ('Repreated name group: %s. Select another one.' % (name))
        self.groups[name] = group

    def allocate(self, node_name, **kwargs):
        """
        
        Method for job allocation. It receives the node name and the resources to be used.
        
        :param node_name: Name of the node to be updated.
        :param \*\*kwargs: Dictionary of the system resources and its values to be used. 
        
        """
        # TODO: Update using self.system_resource_types
        assert(self.resources), 'The resources must be setted before jobs allocation'
        assert(self.resources_status[node_name] == self.ON), 'The Node {} is {}, it is impossible to allocate any job'
        _resources = self.resources[node_name]
        _used = {}
        for k, v in kwargs.items():
            _rem_attr = _resources['%s%s' % (self.available_prefix, k)] - _resources['%s%s' % (self.used_prefix, k)]
            assert(v <= _rem_attr), 'The event was request {} {}, but there is only {} available.'.format(v, k, _rem_attr)
            _resources['%s%s' % (self.used_prefix, k)] += v
            _used[k] = _rem_attr - v

    def release(self, node_name, **kwargs):
        """
        
        Method for allocation release. It receives the node name and the resources to be released.
        
        :param node_name: Name of the node to be updated.
        :param \*\*kwargs: Dictionary of the system resources and its values to be released. 
        
        """
        # TODO: Update using self.system_resource_types
        assert(self.resources), 'The resources must be setted before release resources'
        assert(self.resources_status[node_name] == self.ON), 'The Node {} is {}.'
        _resources = self.resources[node_name]
        for k, v in kwargs.items():
            _resources['%s%s' % (self.used_prefix, k)] -= v
            assert(_resources['%s%s' % (self.used_prefix, k)] >= 0), 'The event was request to release %i %s, but there is only %i available. It is impossible less than 0 resources' % (v, k, _resources['%s%s' % (self.used_prefix, k)])

    def availability(self):
        """
        
        System availablity calculation
        
        :return: Return a dictionary with the system availability. In terms of {node: {resource: value}}
        
        """
        # TODO: Update using self.system_resource_types
        assert(self.resources)
        _a = {}
        for node, attrs in self.resources.items():
            if self.resources_status[node] == self.OFF:
                continue 
            _a[node] = {
                # attr: (attrs['%s%s' % (self.available_prefix, attr)] - attrs['%s%s' % (self.used_prefix, attr)]) for attr in set([attr.split('_')[1] for attr in attrs])
                attr: (attrs['%s%s' % (self.available_prefix, attr)] - attrs['%s%s' % (self.used_prefix, attr)]) for attr in self.system_resource_types
            }
        return _a

    def usage(self):
        """
        
        System usage calculation
        
        :return: Return a string of the system usage 
        
        """
        # TODO: Update using self.system_resource_types
        _str = "System usage: "
        _str_usage = []
        usage = {k: 0 for k in list(self.resources.values())[0]}
        for attrs in self.resources.values():
            for k, v in attrs.items():
                usage[k] += v
        # for _attr in set([attr.split('_')[1] for attr in usage]):
        for _attr in self.system_resource_types:
            if usage['%s%s' % (self.available_prefix, _attr)] > 0:
                _str_usage.append("%s: %.2f%%" % (_attr, usage['%s%s' % (self.used_prefix, _attr)] / usage['%s%s' % (self.available_prefix, _attr)] * 100))
        return (_str + ', '.join(_str_usage))

    def system_capacity(self):
        """
        
        :return: Return total system capacity 
        
        """
        _capacity = {
            r: {'total':
                sum([attrs[self.available_prefix + r] for _, attrs in self.resources.items()]) 
            } 
            for r in self.system_resource_types
        }
        return _capacity
    
    def resource_manager(self):
        """
        
        Instantiation of the resource manager object
        
        :return: Resource manager object. 
        
        """
        return resource_manager(self)
    
    def available_resource_key(self, _key):
        """
        
        Generate the resource key names
        
        :param _key: Name of the resource
            
        :return: Return the Resource key name. 
        
        """
        assert(_key in self.system_resource_types), '{} is not a resource type'.format(_key)
        return '{}{}'.format(self.available_prefix, _key)        

    def __str__(self):
        _str = "Resources:\n"
        for node, attrs in self.resources.items():
            formatted_attrs = ""
            # for attr in set([attr.split('_')[1] for attr in attrs]):
            for attr in self.system_resource_types:
               formatted_attrs += '%s: %i/%i, ' % (attr, attrs['%s%s' % (self.used_prefix, attr)], attrs['%s%s' % (self.available_prefix, attr)])
            _str += '- %s: %s\n' % (node, formatted_attrs)
        return _str

class resource_manager:

    def __init__(self, _resource):
        """
        
        Constructor for Resource Manager.
        This class handles the resources through Allocation and Release methods.
        
        :param _resource: An instance of the resources class. It defines the system capacity.  
        
        """
        assert(isinstance(_resource, resources_class)), ('Only %s class is acepted for resources' % resources_class.__name__)
        self.resources = _resource
        self.actual_events = {}

    def allocate_event(self, event, node_names):
        """
        
        Method for job allocation. It uses the event request to determine the resources to be allocated.
        
        :param event: Job event object.
        :param node_names: List of nodes where the job will be allocated.  
        
        """
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
        """
        
        Method for job release. It release the allocated resources on the specific nodes.
        
        :param id: Job Id 
        
        """
        for node_name, values in self.actual_events.pop(id).items():
            self.resources.release(node_name, **values)

    def node_resources(self, *args):
        """
        
        :param \*args: list of node names 
        
        Print nodes and its resources 
        
        """
        for arg in args:
            print(arg, self.resources.resources[arg])

    def availability(self):
        """
        
        :return: Return system availability
        
        """        
        return self.resources.availability()

    def resource_types(self):
        """
        
        :return: Return resource types of the system
        
        """
        return self.resources.system_resource_types

    def get_nodes(self):
        """
        
        :return: Return node names
        
        """
        return list(self.resources.resources.keys())
    
    def get_total_resources(self, *args):
        """
        
        Return the total system resource for the required argument. The resource have to exist in the system. 
        If no arguments is proportioned all resources are returned.
        
        :param \*args: Depends on the system configuration. But at least it must have ('core', 'mem') resources.
            
        :return: Dictionary of the resources and its values.          
        
        """
        _resources = self.resources.total_resources()
        if not args or len(args) == 0:
            return {k: v for k, v in _resources.items()}
        avl_types = {}
        for arg in args:
            assert(arg in _resources), '{} is not a resource of the system. Available resource are {}'.format(arg, self.resource_types()) 
            avl_types[arg] = _resources[arg]
        return avl_types

    def groups_available_resource(self, _key=None):
        """
        
        :param _key: None for values of all types for all groups. Giving a specific key will return the resource for the specific type
        
        :return: Dictionary of {group{type: value}}   
        
        """
        if not _key:
            _group = {}
            for k, v in self.resources.groups.items():
                _group[k] = {_type: v[self.resources.available_resource_key(_type)] for _type in self.resources.system_resource_types} 
            return _group
        _group_key = self.resources.available_resource_key(_key)
        return {_group:_v[_group_key]  for _group, _v in self.resources.groups.items()} 
