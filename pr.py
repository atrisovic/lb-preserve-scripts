import cPickle
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )
from DIRAC.Core.DISET.RPCClient      import RPCClient

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk  = BookkeepingClient()

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
res = pr.getProductionRequestList( 0, '', 'ASC', 0, 0, {"RequestType":'Simulation','IsModel':1} )['Value']
rows = [converta( x ) for x in res['Rows']]
############################################
for i in range(1,10):
  stepid = 'p%dStep' % i
  print stepid
  if stepid in rows[1]:
    if rows[0][stepid]:
      res = bk.getAvailableSteps({'StepId':rows[0][stepid]})
      if not res['OK']:
        print res['Message']
        exit(1)
      print res['Value']
passes = []
for i in range(1,10):
   stepid = 'p%dUse' % i
   procid = 'p%dPass' % i
   if stepid in rows[0]:
     if rows[0][stepid] == 'Yes':
        passes.append(rows[0][procid])

print "/".join(passes)

res = pr.getProductionRequestList( 0, '', 'ASC', 0, 0, {'RequestID':[38608,38610,38609,38603]} )['Value']




