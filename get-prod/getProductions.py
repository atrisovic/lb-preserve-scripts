import json
from datetime import datetime
import cPickle
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )
from DIRAC.Core.DISET.RPCClient      import RPCClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
import re
bk  = BookkeepingClient()

localFields = [ 'ID', '_is_leaf', '_parent', '_master',
                  'reqName', 'reqType', 'reqState',
                  'reqPrio', 'reqAuthor', 'reqPDG', 'reqWG',
                  'simDesc', 'simCondID',
                  'pDsc', 'pID',
                  'eventType', 'eventNumber', 'reqComment', 'reqDesc', 'reqInform', 'IsModel',
                  'eventBK', 'eventBKTotal', 'EventNumberTotal',
                  'creationTime', 'lastUpdateTime']

serviceFields = [ 'RequestID', 'HasSubrequest', 'ParentID', 'MasterID',
                    'RequestName', 'RequestType', 'RequestState',
                    'RequestPriority', 'RequestAuthor', 'RequestPDG', 'RequestWG',
                    'SimCondition', 'SimCondID',
                    'ProPath', 'ProID',
                    'EventType', 'NumberOfEvents', 'Comments', 'Description', 'Inform', 'IsModel',
                    'bk', 'bkTotal', 'rqTotal', 'crTime', 'upTime']

def json_serial(obj):
   """JSON serializer for objects not serializable by default json code"""
   if isinstance(obj, datetime):
           serial = obj.isoformat()
           return serial
   raise TypeError ("Type not serializable")

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

def getAllProd ():
    pr = RPCClient( "ProductionManagement/ProductionRequest" )
    filter = {"RequestState":"Done","RequestType":'Simulation', 'RequestID':[39044,39043,7644,7587,7539]}
    #res = pr.getProductionRequestList( 0, '', 'ASC', 1, 1, filter)['Value']
    #filter = {'RequestID':[38608,38609,38610,38611,38612,38613,38603]}
    res = pr.getProductionRequestList( 0, '', 'ASC', 0, 0, filter)['Value']
    #res = pr.getProductionRequestList( 0, '', 'ASC', 0, 0, {'RequestID':[38608,38610,38609,38603]} )['Value']
    ############################################
    #print json.dumps(res['Rows'][0], default=json_serial, indent=4, separators=(',', ': '))

    for r in res['Rows']:
        prod = dict()
        prod["EventType"]= r["EventType"]  #if "eventType" in pr else ""
        prod["ProPath"]= r["ProPath"]
        prod["SimCondition"] = r["SimCondition"]
        prod["RequestType"] = r["RequestType"]
        prod["NumberOfEvents"]= r["NumberOfEvents"]
        prod["RequestID"]= r["RequestID"]
        prod["upTime"]= r["upTime"]
        prod["crTime"]= r["crTime"]

        pr = converta(r)
        temp = re.findall(r"p\dStep", str(pr))
        stepno = len(temp)
        print stepno
        
        # Ditch the rest
        current = {}
        for i in range(1, stepno):
          stepid = 'p%dStep' % i
          if stepid in pr:
            if pr[stepid]:
              a = bk.getAvailableSteps({'StepId':pr[stepid]})
              if not a['OK']:
                print a['Message']
                exit(1)
              values = a["Value"]["Records"][0]
              
              keys = values[-1]["ParameterNames"]
              step = {}
              for j in range(len(keys)):
                      step[keys[j]] = values[j]
              prod[stepid] = step
              #prod[stepid + "ID"] = conv[0][stepid]
        #print json.dumps(prod, default=json_serial, indent=4, separators=(',', ': '))
        # BKK path
        passes = []
        if prod["RequestType"] == 'Simulation':
            passes.append('MC')
        else:
            passes.append('LHCb')
        #passes[0] = 'MC' if prod["RequestType"] == 'Simulation' else 'LHCb'
        findall = re.findall(r'-20\d\d-', prod["SimCondition"])
        temp_ = ""
        if len(findall) > 0:
            temp_ = findall[0]
            passes.append(temp_.strip('-'))
        else:
            findall = re.findall(r'[a-z]20\d\d-', prod["SimCondition"])
            if len(findall) > 0:
                temp_ = findall[0]
                passes.append(temp_[1:5])
            else:
                print prod["SimCondition"]
                #break
        passes.append(prod["SimCondition"]) 
        passes.append(prod["ProPath"])
        passes.append(prod["EventType"])
        passes = [x or '' for x in passes]
        if len(passes) > 0:
            prod["bkkpath"] = "/".join(passes)
            #print prod["bkkpath"]
        prod["year"] = temp_[1:5]
        print json.dumps(prod, default=json_serial, indent=4, separators=(',', ': '))

if __name__ == "__main__":
    getAllProd()
