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

from accasim.base.event_class import EventManager
from accasim.utils.misc import CONSTANT

class AdditionalDataType:
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
        
class AdditionalData(ABC):
    """

    Additional data class enables to add new behavior to the system, by executing a custom process which can use the current state of the system to create new data
    The current state of the system is maintained in the :class:`accasim.base.event_class.EventManager` object.

    """
          
    def __init__(self, event_manager=None):
        """
        
        Constructor. 
        The event mapper (:class:`accasim.base.event_class.EventManager`) must be defined at the instantiation or later, but it is mandatory for working.
        
        :param event_manager: Event manager object.
        
        """
        self.constant = CONSTANT()
        self.event_mapper = event_manager
        
    @abstractmethod
    def exec_before_dispatching(self, job_dict, queued_jobs):
        """
            Executed before the dispatcher call.
            
            :param job_dict: Dictionary containing all the loaded jobs.
            :param queued_jobs: A list containing the ids of the queued jobs.

            :return: Nothing to return. All modifications must be performed directly to the job object.
        """
        pass
    
    @abstractmethod
    def exec_after_dispatching(self, job_dict, to_dispatch_jobs, rejected_jobs):
        """
            Executed after the dispatcher call.
            
            :param job_dict: Dictionary containing all the loaded jobs.
            :param to_dispatch_jobs: A list containing the dispatching tuples [(start_time, job_id, node_allocation), ...]
            :param rejected_jobs: A list of job ids with rejected jobs by the dispatcher.
            
            :return: Nothing to return if there is no modification to the to_dispatch_jobs and rejected_jobs. 
                Otherwise these lists must be returned as tuple (to_dispatch_jobs, rejected_jobs,)  
        """
        pass
    
    @abstractmethod
    def exec_before_submission(self, **kwargs):
        pass

    @abstractmethod
    def exec_after_submission(self, **kwargs):
        pass

    @abstractmethod
    def exec_before_completion(self, **kwargs):
        pass

    @abstractmethod
    def exec_after_completion(self, removed_jobs):
        """
            Executed after releasing the resources of the jobs which have to finish at the current time. 
            
            :param removed_jobs: List of removed job object with their updated attributes.
            
            :return: Nothing 
        """
        pass

    @abstractmethod
    def stop(self):
        """
        Executed after the simulation ends.
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
            assert(isinstance(var, AdditionalDataType)), 'Only additional_data_type class can be modified. Ensure that the {} is not already used.'.format(name)
            var.update(value)
            return
        var = AdditionalDataType(value)   
        setattr(self.event_mapper, name, var)        
        
    def set_event_manager(self, event_manager):
        """

        Set the system event manager
            
        :param event_manager: An instantiation of a :class:`accasim.base.event_class.EventManager` class or None

        """       
        if self.event_mapper:
            return
        assert isinstance(event_manager, EventManager), 'Event Mapper not valid for scheduler'
        self.event_mapper = event_manager

class AdditionalDataError(Exception):
    pass
