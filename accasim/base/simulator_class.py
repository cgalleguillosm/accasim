from events_simulator.utils.reader_class import reader
from datetime import datetime
from abc import abstractmethod, ABC
from events_simulator.utils.misc import CONSTANT

class simulator_base(ABC):
	
	def __init__(self, _reader):
		self.constants = CONSTANT()
		self.real_init_time = datetime.now()
		assert(isinstance(_reader, reader))
		self.reader = _reader		 	
		self.n_lines = self.constants.N_LINES
			
	@abstractmethod
	def start_simulation(self):
		raise NotImplementedError('Must be implemented!')
	
	@abstractmethod
	def load_events(self):
		raise NotImplementedError('Must be implemented!')
