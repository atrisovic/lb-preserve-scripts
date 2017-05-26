import json
from datetime import datetime
import cPickle
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )
from DIRAC.Core.DISET.RPCClient      import RPCClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
import re
bk  = BookkeepingClient()

def json_serial(obj):
   """JSON serializer for objects not serializable by default json code"""
   if isinstance(obj, datetime):
           serial = obj.isoformat()
           return serial
   raise TypeError ("Type not serializable")

serviceFields = [ 'RequestID', 'HasSubrequest', 'ParentID', 'MasterID',
                    'RequestName', 'RequestType', 'RequestState',
                    'RequestPriority', 'RequestAuthor', 'RequestPDG', 'RequestWG',
                    'SimCondition', 'SimCondID',
                    'ProPath', 'ProID',
                    'EventType', 'NumberOfEvents', 'Comments', 'Description', 'Inform', 'IsModel',
                    'bk', 'bkTotal', 'rqTotal', 'crTime', 'upTime']
localFields = [ 'ID', '_is_leaf', '_parent', '_master',
                  'reqName', 'reqType', 'reqState',
                  'reqPrio', 'reqAuthor', 'reqPDG', 'reqWG',
                  'simDesc', 'simCondID',
                  'pDsc', 'pID',
                  'eventType', 'eventNumber', 'reqComment', 'reqDesc', 'reqInform', 'IsModel',
                  'eventBK', 'eventBKTotal', 'EventNumberTotal',
                  'creationTime', 'lastUpdateTime']

def converta( req ):
    result = {}
    for x, y in zip( localFields, serviceFields ):
      result[x] = req[y]
    result['_is_leaf'] = not result['_is_leaf']
    if req['bkTotal'] is not None and req['rqTotal'] is not None and req['rqTotal']:
      result['progress'] = long( req['bkTotal'] ) * 100 / long( req['rqTotal'] )
    else:
      result['progress'] = None
    if req['SimCondDetail']:
      result.update( cPickle.loads( req['SimCondDetail'] ) )
    if req['ProDetail']:
      result.update( cPickle.loads( req['ProDetail'] ) )
    result['creationTime'] = str( result['creationTime'] )
    result['lastUpdateTime'] = str( result['lastUpdateTime'] )
    if req['Extra']:
      result.update( cPickle.loads( req['Extra'] ) )
    for x in result:
      if x != '_parent' and result[x] == None:
        result[x] = ''
    return result

pr = RPCClient( "ProductionManagement/ProductionRequest" )
#res = pr.getProductionRequestList( 0, '', 'ASC', 0, 1, {"RequestState":"Done"} )['Value']
#op = pr.getFilterOptions()['Value']

filter = {"RequestState":"Done","RequestType":'Simulation'}
#res = pr.getProductionRequestList( 0, '', 'ASC', 0, 0, {"RequestType":'Simulation','IsModel':1} )['Value']
#res = pr.getProductionRequest([38608])
res = pr.getProductionRequestList( 0, '', 'ASC', 0, 0, {'RequestID':[38608, 27133]})['Value']

############################################
#print res
#print res.keys
text = res['Rows'][0]["ProDetail"]
#print json.dumps(text, default=json_serial, indent=4, separators=(',', ': '))
# Count steps
stepno = 0
temp = re.findall(r"p\dStep", text)
stepno = len(temp)
#print temp, stepno
# Create production dict

prod = dict()
# Take all the useful stuff
prod["SimCondition"] = res['Rows'][0]["SimCondition"]
prod["RequestType"] = res['Rows'][0]["RequestType"]
prod["EventType"]= res['Rows'][0]["EventType"]
prod["NumberOfEvents"]= res['Rows'][0]["NumberOfEvents"]
prod["ProPath"]= res['Rows'][0]["ProPath"]
prod["RequestID"]= res['Rows'][0]["RequestID"]
prod["upTime"]= res['Rows'][0]["upTime"]
prod["crTime"]= res['Rows'][0]["crTime"]
# Ditch the rest
rows = [converta( x ) for x in res['Rows']]
# print json.dumps(rows, default=json_serial, indent=4, separators=(',', ': '))


current = {}
for i in range(1, stepno):
  stepid = 'p%dStep' % i
  if stepid in rows[0]:
    if rows[0][stepid]:
      a = bk.getAvailableSteps({'StepId':rows[0][stepid]})
      if not a['OK']:
        print a['Message']
        exit(1)
      values = a["Value"]["Records"][0]
      keys = values[-1]["ParameterNames"]
      step = {}
      for i in range(len(keys)):
              step[keys[i]] = values[i]
      prod[stepid] = step

#passes = []
#for i in range(1, stepno):
#   stepid = 'p%dUse' % i
#   procid = 'p%dPass' % i
#   if stepid in rows[0]:
#     if rows[0][stepid] == 'Yes':
#        passes.append(rows[0][procid])
#
passes = []
if prod["RequestType"] == 'Simulation':
    passes.append('MC')
else:
    passes.append('LHCb')
#passes[0] = 'MC' if prod["RequestType"] == 'Simulation' else 'LHCb'
temp_ = re.findall(r'-20\d\d-', prod["SimCondition"])[0]
passes.append(temp_.strip('-'))
passes.append(prod["SimCondition"]) 
passes.append(prod["ProPath"])
passes.append(prod["EventType"])
prod["bkkpath"] = "/".join(passes)
prod["year"] = temp_.strip('-')
print json.dumps(prod, default=json_serial, indent=4, separators=(',', ': '))
#res = pr.getProductionRequestList( 0, '', 'ASC', 0, 0, {'RequestID':[38608,38610,38609,38603]} )['Value']

#print rows[0]


