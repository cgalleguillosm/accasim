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
from accasim.utils.misc import FrozenDict


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
        """
        self.GROUPS = None
        self.SYSTEM_CAPACITY_TOTAL = None
        self.SYSTEM_CAPACITY_NODES = None
        self.NODE_LIST = None        
        
        self.definition = [{'nodes': q, 'resources':groups[k]} for k, q in resources.items() ]
        self.resources = {}
        self.resources_status = {}
        self.system_resource_types = []
        self.system_total_resources = None
        self.node_prefix = kwargs['node_prefix'] if 'node_prefix' in kwargs else 'node_' 

        # List all attributes of all groups
        for group_name, group_values in groups.items():
            self.system_resource_types += filter(lambda x: x not in self.system_resource_types, list(group_values.keys()))
        #=======================================================================
        # Create the corresponding group attributes and add 0 to absent attributes.
        # This is performed in case that the user doesn't assign an absent attribute in the system config.
        # For instance when a group has gpu and an another group hasn't and that attribute must be 0. 
        #=======================================================================
        _groups = {}
        for group_name, group_values in groups.items():
            resource_group = { attr: group_values.get(attr, 0) for attr in self.system_resource_types }
            self.define_group(_groups, group_name, resource_group)
        self.GROUPS = FrozenDict(**_groups)

        #=======================================================================
        # Define system capacity and usage of the system
        #=======================================================================
        _nodes_capacity = {}
        _system_capacity = {res:0 for res in self.system_resource_types}

        j = 0
        self.NODE_LIST = []
        for group_name, q in resources.items():
            for res, value in self.GROUPS[group_name].items():
                _system_capacity[res] += value * q
                
            for i in range(q):
                _node_name = '{}{}'.format(self.node_prefix, j + 1)
                _attrs_values = self.GROUPS[group_name]
                
                _nodes_capacity[_node_name] = {res:_attrs_values[res] for res in self.system_resource_types}
                self.resources[_node_name] = {res:0 for res in self.system_resource_types}
                self.resources_status[_node_name] = self.ON      
                self.NODE_LIST.append(_node_name)       
                j += 1
        
        self.SYSTEM_CAPACITY_NODES = FrozenDict(**_nodes_capacity)
        self.SYSTEM_CAPACITY_TOTAL = FrozenDict(**_system_capacity)
        
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

    def define_group(self, _groups, name, group):
        """
        
         Internal method for defining groups of resources.
         
         :param name: Name of the group
         :param group: Values of the group. As defined in the system config.  
        
        """
        assert(isinstance(group, dict))
        assert(name not in _groups), ('Repreated name group: %s. Select another one.' % (name))
        _groups[name] = group

    def allocate(self, node_name, **kwargs):
        """
        
        Method for job allocation. It receives the node name and the resources to be used.
        
        :param node_name: Name of the node to be updated.
        :param \*\*kwargs: Dictionary of the system resources and its values to be used. 
        
        """
        # TODO: Update using self.system_resource_types
        assert(self.resources), 'The resources must be setted before jobs allocation'
        assert(self.resources_status[node_name] == self.ON), 'The Node {} is {}, it is impossible to allocate any job'
        _capacity = self.SYSTEM_CAPACITY_NODES[node_name]
        _used_resources = self.resources[node_name]
        _done = []
        for res, v in kwargs.items():
            _rem_attr = _capacity[res] - _used_resources[res]
            try:
                assert(v <= _rem_attr), 'The event requested {} {}, but there are only {} available.'.format(v, res, _rem_attr)
                _used_resources[res] += v
                _done.append((res, v))
            except AssertionError as e:
                while _done:
                    key, req = _done.pop()
                    _used_resources[key] -= req
                return False, e
        return True, 'OK'

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
        for res, v in kwargs.items():
            _resources[res] -= v
            assert(_resources[res] >= 0), 'The event was request to release %i %s, but there is only %i available. \
                It is impossible less than 0 resources' % (v, res, _resources[res])
        
    def availability(self):
        """
        
        System availablity calculation
        
        :return: Return a dictionary with the system availability. In terms of {node: {resource: value}}
        
        """
        assert(self.resources)
        _a = {}
        for node, node_resources in self.resources.items():
            if self.resources_status[node] == self.OFF:
                continue 
            _a[node] = {
                res: (self.SYSTEM_CAPACITY_NODES[node][res] - node_resources[res]) for res in self.system_resource_types
            }
        return _a

    def usage(self, type=None):
        """
        
        System usage calculation
        
        :return: Return a string of the system usage 
        
        """
        _str = "System usage: "
        _str_usage = []
        usage = {res: sum([node_resources[res] for node_resources in self.resources.values()]) for res in self.system_resource_types}
        if not type:
            for res, value in self.SYSTEM_CAPACITY_TOTAL.items():
                _str_usage.append("{}: {:.2%}".format(res, usage[res] / value))
            return (_str + ', '.join(_str_usage))
        elif type == 'dict':
            return {
                res: (usage[res] / value) * 100                
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
            for attr in self.system_resource_types:
               formatted_attrs += '%s: %i/%i, ' % (attr, attrs['%s%s' % (self.used_prefix, attr)], attrs['%s%s' % (self.available_prefix, attr)])
            _str += '- %s: %s\n' % (node, formatted_attrs)
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
        assert(isinstance(_resource, Resources)), ('Only %s class is acepted for resources' % Resources.__name__)
        self.resources = _resource
        self.actual_events = {}

    def allocate_event(self, event, node_names):
        """
        
        Method for job allocation. It uses the event request to determine the resources to be allocated.
        
        :param event: Job event object.
        :param node_names: List of nodes where the job will be allocated.  
        
        :return: Tuple: First element True if the event was allocated, False otherwise. Second element a message. 
        """
        _resources = event.requested_resources
        _attrs = event.requested_resources.keys()

        unique_nodes = [(t, node_names.count(t)) for t in set(node_names)]

        self.actual_events[event.id] = {
            node_name: { _attr:_resources[_attr] * q for _attr in _attrs} for (node_name, q) in unique_nodes
        }
        _allocated = True
        _rollback = []
        for node_name, values in self.actual_events[event.id].items():
            done, message = self.resources.allocate(node_name, **values)
            if done:
                _rollback.append((node_name, values))
            else:
                _allocated = False
                self.actual_events.pop(event.id)
                break
            
        while not _allocated and _rollback:
            node_name, values = _rollback.pop()
            self.resources.release(node_name, **values)
               
        return _allocated, message

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
        _resources = self.system_capacity('total')
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
        _resources = self.resources.system_groups()
        if not _key:
            return _resources
        _resource_key = self._resources.available_resource_key(_key)
        return {_group:_v[_resource_key] for _group, _v in self.resources.items()} 
    
    def system_resources(self):
        return self.resources 
    
    def system_capacity(self, type):
        """        
        :param type: 
            'total' to return the total per resource type
            'nodes' to return the capacity of nodes            
        
        :return: Return system capacity 
        """
        return self.resources.system_capacity(type)
    
    def node_names(self):
        """
        
        :return: Return node names
        
        """
        return self.resources.NODE_LIST
