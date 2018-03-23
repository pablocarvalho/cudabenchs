from db import DBConnection

db2 = 'kernels.db'
db1 = 'kernels-gtx_980-30its.db'
conn1 = DBConnection(db1)
conn2 = DBConnection(db2)

cursor1 = conn1.connection.cursor()
cursor2 = conn2.connection.cursor()

cursor1.execute("""select * from kernels;""")
result1 = cursor1.fetchall()

print "Running on " + str(len(result1)) + ' kernels...'

for row1 in result1:
	print row1['codeName']
	cursor2.execute("""update kernels set codeName = ? where mangledName = ?""",(row1['codeName'],row1['mangledName']));

conn2.connection.commit()
