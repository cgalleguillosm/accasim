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
from datetime import datetime


class Resources:
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
        self.SYSTEM_CAPACITY_TOTAL = None
        self.SYSTEM_CAPACITY_NODES = None
        self.GROUPS = None
        self.NODE_LIST = []
        
        self._definition = tuple([{'nodes': q, 'resources':groups[k]} for k, q in resources.items()])
        self._resources = {}
        self._current_capacity = {}
        self._resources_status = {}
        self._system_resource_types = []
        
        self._node_prefix = kwargs.pop('node_prefix', 'node_') 
        
        # List all attributes of all groups
        for group_name, group_values in groups.items():
            self._system_resource_types += filter(lambda x: x not in self._system_resource_types, list(group_values.keys()))
        
        # Here the current usage is maintained
        self._total_resources = {r: 0 for r in self._system_resource_types}
        
        #=======================================================================
        # Create the corresponding group attributes and add 0 to absent attributes.
        # This is performed in case that the user doesn't assign an absent attribute in the system config.
        # For instance when a group has gpu and an another group hasn't and that attribute must be 0. 
        #=======================================================================
        _groups = {}
        for group_name, group_values in groups.items():
            resource_group = { attr: group_values.get(attr, 0) for attr in self._system_resource_types }
            self._define_group(_groups, group_name, resource_group)
        self.GROUPS = FrozenDict(**_groups)

        #=======================================================================
        # Define system capacity and usage of the system
        #=======================================================================
        _node_capacity = {res:0 for res in self._system_resource_types}
        _nodes_capacity = {}
        _system_capacity = deepcopy(_node_capacity)

        j = 0        
        for group_name, q in resources.items():
            for res, value in self.GROUPS[group_name].items():
                _system_capacity[res] += value * q
                
            for i in range(q):
                _node_name = '{}{}'.format(self._node_prefix, j + 1)
                _attrs_values = self.GROUPS[group_name]
                
                _nodes_capacity[_node_name] = deepcopy(_attrs_values)
                self._current_capacity[_node_name] = deepcopy(_attrs_values)
                self._resources[_node_name] = deepcopy(_node_capacity)
                self._resources_status[_node_name] = self.ON
                self.NODE_LIST.append(_node_name)   
                j += 1

        self.SYSTEM_CAPACITY_NODES = FrozenDict(**_nodes_capacity)
        self.SYSTEM_CAPACITY_TOTAL = FrozenDict(**_system_capacity)

    def _define_group(self, _groups, name, group):
        """
        
         Internal method for defining groups of resources.
         
         :param name: Name of the group
         :param group: Values of the group. As defined in the system config.  
        
        """
        assert(isinstance(group, dict))
        assert(name not in _groups), 'Repreated name group: {}. Select another one.'.format(name)
        _groups[name] = group

    def allocate(self, node_name, **kwargs):
        """
        
        Method for job allocation. It receives the node name and the resources to be used.
        
        :param node_name: Name of the node to be updated.
        :param \*\*kwargs: Dictionary of the system resources and its values to be used. 
        
        """
        assert(self._resources), 'The resources must be setted before jobs allocation'
        assert(self._resources_status[node_name] == self.ON), 'The Node {} is {}, it is impossible to allocate any job'
        _capacity = self.SYSTEM_CAPACITY_NODES[node_name]
        _used_resources = self._resources[node_name]
        _done = []
        _full_usage = {}
        for res, v in kwargs.items():
            _rem_res = _capacity[res] - _used_resources[res]  # _used_resources[avl_key] - _used_resources[used_key]
            try:
                assert(v <= _rem_res), 'The event requested {} {}, but there are only {} available.'.format(v, res, _rem_res)
                _used_resources[res] += v
                # Update totals
                self._total_resources[res] += v
                self._current_capacity[node_name][res] -= v
                _done.append((res, v))
            except AssertionError as e:
                while _done:
                    key, req = _done.pop()
                    _used_resources[key] -= req
                    # Update totals
                    self._total_resources[key] -= req
                    self._current_capacity[node_name][key] += req
                return False, e
        return True, 'OK'

    def release(self, node_name, **kwargs):
        """
        
        Method for allocation release. It receives the node name and the resources to be released.
        
        :param node_name: Name of the node to be updated.
        :param \*\*kwargs: Dictionary of the system resources and its values to be released. 
        
        """
        assert(self._resources), 'The resources must be setted before release resources'
        assert(self._resources_status[node_name] == self.ON), 'The Node {} is {}.'
        _resources = self._resources[node_name]
        for _res, v in kwargs.items():
            _resources[_res] -= v
            # Update totals
            self._total_resources[_res] -= v
            self._current_capacity[node_name][_res] += v
            assert(_resources[_res] >= 0), 'The event was request to release {} {}, but there is only {} available. It is impossible less than 0 resources'.format(v, _res, _resources['%s%s' % (self.used_prefix, _res)])
        
    def availability(self):
        """
        
        Returns the current system availablity. It just return nodes that are ON.
        
        :return: Return a dictionary with the system availability. In terms of {node: {resource: value}}
        
        """
        return {node: {k:v for k, v in self._current_capacity[node].items()} for node in self.NODE_LIST if self._resources_status[node] == self.ON}

    def usage(self, type=None):
        """
        
        System usage calculation
        @todo: Use NODE_LIST instead items
        
        :return: Return a string of the system usage 
        
        """
        _str = "System usage: "
        _str_usage = []
        
        if not type:
            for res, value in self.SYSTEM_CAPACITY_TOTAL.items():
                if value > 0:
                    _str_usage.append("{}: {:.2%}".format(res, self._total_resources[res] / value))
                else:
                    _str_usage.append("{}: -".format(res))
            return (_str + ', '.join(_str_usage))
        elif type == 'dict':
            return {
                res: (self._total_resources[res] / value) * 100                
                for res, value in self.SYSTEM_CAPACITY_TOTAL.items()
            }
        else:
            raise NotImplementedError()

    def system_capacity(self, type='total'):
        """
        
        :param type: 
            'total' to return the total per resource type
            'nodes' to return the capacity of nodes
                        
        
        :return: Return system capacity 
        
        """
        if type == 'total':
            return self.SYSTEM_CAPACITY_TOTAL
        elif type == 'nodes':
            return self.SYSTEM_CAPACITY_NODES
        raise ResourceError('System Capacity: \'{}\' type not defined'.format(type))
    
    def resource_manager(self):
        """
        
        Instantiation of the resource manager object
        
        :return: Resource manager object. 
        
        """
        return ResourceManager(self)

    def __str__(self):
        """
        @todo: Use NODE_LIST instead items
        """
        _str = "Resources:\n"
        for node in self.NODE_LIST:
            attrs = self._resources[node]
            formatted_attrs = ""
            for attr in self._system_resource_types:
               formatted_attrs += '{}: {}/{}, '.format(attr, attrs[attr], self.SYSTEM_CAPACITY_NODES[node][attr])
            _str += '- {}: {}\n'.format(node, formatted_attrs)
        return _str
    
    def system_groups(self):
        """
            Returns the available system groups.
        """
        return self.GROUPS


class ResourceManager:

    def __init__(self, _resource):
        """
        
        Constructor for Resource Manager.
        This class handles the resources through Allocation and Release methods.
        
        :param _resource: An instance of the resources class. It defines the system capacity.  
        
        """
        assert(isinstance(_resource, Resources)), ('Only {} class is acepted for resources'.format(Resources.__name__))
        self._resources = _resource
        self._running_jobs = {}
        self._logger = logging.getLogger('accasim')

    def allocate_event(self, event, node_names):
        """
        
        Method for job allocation. It uses the event request to determine the resources to be allocated.
        
        :param event: Job event object.
        :param node_names: List of nodes where the job will be allocated.  
        
        :return: Tuple: First element True if the event was allocated, False otherwise. Second element a message. 
        """
        if hasattr(self._logger, 'trace'):
            self._logger.trace('Allocating {} in nodes {}'.format(event.id, ', '.join([node for node in node_names])))
        _requested_res = event.requested_resources
        _attrs = _requested_res.keys()
        
        _allocation = {}
        for node in node_names:
            if not(node in _allocation):
                _allocation[node] = {_attr: _requested_res[_attr] for _attr in _attrs}
                continue
            for _attr in _attrs:
                _allocation[node][_attr] += _requested_res[_attr]
        
        _allocated = True
        _rollback = []
        message = 'OK'
        
        for node_name, values in _allocation.items():
            done, message = self._resources.allocate(node_name, **values)
            if done:
                _rollback.append((node_name, values))
            else:
                self._logger.error('Rollback for {}: {}'.format(event.id, _rollback + [(node_name, values)]))
                _allocated = False
                break
        
        if _allocated:
            self._running_jobs[event.id] = _allocation
        else:
            while _rollback:
                node_name, values = _rollback.pop()
                self._resources.release(node_name, **values)
               
        return _allocated, message

    def remove_event(self, id):
        """
        
        Method for job release. It release the allocated resources on the specific nodes.
        
        :param id: Job Id 
        
        """
        for node_name, values in self._running_jobs.pop(id).items():
            self._resources.release(node_name, **values)

    def node_resources(self, *args):
        """
        
        :param \*args: list of node names 
        
        Print nodes and its resources 
        
        """
        for arg in args:
            print(arg, self._resources._resources[arg])
    
    @property
    def current_availability(self):
        """
        
        :return: Return system availability
        
        """        
        return self._resources.availability()

    @property
    def resource_types(self):
        """
        
        :return: Return resource types of the system
        
        """
        return self._resources._system_resource_types

    @property
    def node_names(self):
        """
        
        :return: Return node names
        
        """
        return self._resources.NODE_LIST
    
    def total_resources(self, *args):
        """
        
        Return the total system resource for the required argument. The resource has to exist in the system. 
        If no arguments is proportioned all resources are returned.
        @todo: Use NODE_LIST instead items
        
        :param \*args: Depends on the system configuration. But at least it must have ('core', 'mem') resources.
            
        :return: Dictionary of the resources and its values.          
        
        """
        _resources = self.system_capacity('total')
        if not args or len(args) == 0:
            return {k: v for k, v in _resources.items()}
        avl_types = {}
        for arg in args:
            assert(arg in _resources), '{} is not a resource of the system. Available resource are {}'.format(arg, self.resource_types) 
            avl_types[arg] = _resources[arg]
        return avl_types

    def groups_available_resource(self, _key=None):
        """
        
        :param _key: None for values of all types for all groups. Giving a specific key will return the resource for the specific type
        :todo: Use NODE_LIST instead items
        
        :return: Dictionary of {group{type: value}}   
        
        """
        _resources = self._resources.system_groups()
        if not _key:
            return _resources
        _resource_key = self._resources.available_resource_key(_key)
        return {_group:_v[_resource_key]  for _group, _v in self._resources.items()} 
    
    def system_resource_types(self):
        return self._resources._system_resource_types
    
    def system_capacity(self, type):
        """        
        :param type: 
            'total' to return the total per resource type
            'nodes' to return the capacity of nodes            
        
        :return: Return system capacity 
        """
        return self._resources.system_capacity(type)
    
    def system_resources(self):
        return self._resources 
    
    @property    
    def current_usage(self):
        return self._resources.usage()
    
    @property
    def current_allocations(self):
        return self._running_jobs


class ResourceError(Exception):
    pass  
