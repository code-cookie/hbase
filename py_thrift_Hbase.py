#coding:utf8
'''参考：
1,官网：https://wiki.apache.org/hadoop/Hbase/ThriftApi
2,网上搜python hbase
结合官网教程能轻松看懂
'''
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *

transport = TSocket.TSocket('localhost', 9090);
transport = TTransport.TBufferedTransport(transport)
protocol = TBinaryProtocol.TBinaryProtocol(transport);
client = Hbase.Client(protocol)
transport.open()

def create_hbase_table():
    contents = ColumnDescriptor(name='cf:', maxVersions=1)
    client.createTable('testpy', [contents])
    print client.getTableNames()
def getColDesc():
    print client.getColumnDescriptors('test')
def getTableRegions():
    print client.getTableRegions('test')

def insert():
    row = 'row-key1'
    mutations = [Mutation(column="cf:a", value="100")]
    client.mutateRow('testpy', row, mutations, None)
def get_one_row():
    tableName = 'testpy'
    rowKey = 'row-key1'
    result = client.getRow(tableName, rowKey, None)
    print result
    for r in result:
        print 'the row is ' , r.row
        print 'the values is ' , r.columns.get('cf:a').value
def get_muti_row():
    scan = TScan()
    tableName = 'test'
    id = client.scannerOpenWithScan(tableName, scan, None)
    result2 = client.scannerGetList(id, 2)
    print result2

if __name__ == "__main__":
    get_one_row()
