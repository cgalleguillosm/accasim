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
from abc import abstractmethod, ABC
from accasim.base.event_class import event_mapper
from builtins import int

class additional_data_type:
    """
    
    Specific object type for variables added through additional data implementations 
    
    """
    def __init__(self, value):
        self.value = value

    def get(self):
        """
        :return: Returns the internal value
        
        """
        return self.value
    
    def update(self, value):
        """
        Updates the internal value
        
        :param value: New value
         
        """
        self.data = value
        
class additional_data(ABC):
    """
    Additional data class enables to add new behavior to the system, by executing a custom process which can use the current state of the system to create new data
    The current state of the system is maintained in the :class:`accasim.base.event_class.event_mapper` object.  
    """
    
    def __init__(self, event_manager=None):
        """
        
        Constructor. 
        The event mapper (:class:`accasim.base.event_class.event_mapper`) must be defined at the instantiation or later, but it is mandatory for working.
        
        :param event_manager: Event manager object.
        
        """
        self.event_mapper = event_manager
    
    @abstractmethod
    def execute(self):
        """
        This method is called at every simulation step by the simulator to update the aditional data. 
        The user must ensure that any current existent variable will be overwritten by using the add_data function to update/create a single variable.
        """
        pass
    
    def add_timepoint(self, timepoint):
        """
        
        Add a new time point for the events that activate the simulation steps.
        
        :param timepoint: New time point, it must be equal or later than the current time.         
        
        """
        assert(isinstance(timepoint, int)), 'The time point must be an int'
        current_time = self.event_mapper.current_time
        if timepoint > current_time:
            tpoints = getattr(self.event_mapper, 'time_points')
            tpoints.add(timepoint)     
        
    def add_data(self, name, value):
        """
        
        Adds a new attribute and the respective value to the event manager.
        The new variable corresponds to a :class:`.additional_data_type`. If the variable already exists, it is updated through its update method.
        
        :param name: The name of the new attribute
        :param value: The value for the new attribute
        
        """
        if hasattr(self.event_mapper, name):
            var = getattr(self.event_mapper, name)
            assert(isinstance(var, additional_data_type)), 'Only additional_data_type class can be modified. Ensure that the {} is not already used.'.format(name)
            var.update(value)
            return
        var = additional_data_type(value)   
        setattr(self.event_mapper, name, var)        
        
    def set_event_manager(self, event_manager):
        """
            Set the system event manager
            
            :param event_manager: An instantiation of a :class:`accasim.base.event_class.event_mapper` class or None
             
        """       
        if self.event_mapper:
            return
         
        self.allocator.set_resource_manager(event_manager)
        assert isinstance(event_manager, event_mapper), 'Event Mapper not valid for scheduler'
        self.event_mapper = event_manager
