class RequestInfo:
	def __init__(self, groupId, projectId, dataStatus):
		self.groupId = str(groupId)
		self.projectId = str(projectId)
		if isinstance(dataStatus, list):
			self.dataStatus = dataStatus # list
			print('* set list')
		else:
			self.dataStatus = str(dataStatus)
			print('* set str')