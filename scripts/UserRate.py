from datetime import datetime

class UserRate:

	def __init__(self, user_id="", from_ts="", to_ts="", queries=0):
		self.user_id = user_id
		self.from_ts = from_ts
		self.to_ts = to_ts
		self.queries = queries

	@property
	def from_ts(self):
		return self._from_ts
	
	@from_ts.setter
	def from_ts(self, start_ts):
		self._from_ts = datetime.strptime(start_ts, "%Y-%m-%d %H:%M:%S")

	@property
	def to_ts(self):
		return self._to_ts

	@to_ts.setter
	def from_ts(self, end_ts):
		self.to_ts = datetime.strptime(end_ts, "%Y-%m-%d %H:%M:%S")

	@property
	def queries(self):
		return self. queries
	
	@queries.setter
	def queries(self, queries):
		self.queries = queries
	
	def __str__(self):    
        return "({0}, {1}, {2}, {3})".format(self.user_id, self.from_ts, self.to_ts, self.queries)

