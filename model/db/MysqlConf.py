import pymysql

class MysqlConf:
	conn = None
	def __init__(self, host, user, pw, dbName):
		self.host = str(host)
		self.user = str(user)
		self.pw = str(pw)
		self.dbName = str(dbName)

	def connect(self):
		self.conn = pymysql.connect(host=self.host, user=self.user, password=self.pw, db=self.dbName)

	def disconnect(self):
		self.conn.close()
		self.conn = None

	def getRows(self, sql):
		self.cur = self.conn.cursor(pymysql.cursors.DictCursor)
		self.cur.execute(sql)
		print('query is ///////////////////////////////////////')
		print(sql)
		print('////////////////////////////////////////////////')
		return self.cur.fetchall()
