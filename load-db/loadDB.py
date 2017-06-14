from py2neo import Graph, Node, Relationship, NodeSelector
import json
import re

graph = Graph()
selector = NodeSelector(graph)

if __name__ == "__main__":
    f = open("parttwo.txt")
    content = f.read()
    f.close()
    rel = json.loads(content)
    for el in rel:
        # no events - continue
        #if el["NumberOfEvents"] == None:
	#    print "Skipping production %s because of 0 events"%el["RequestID"]
        #    continue
        # get applications
        apps = []
	param = dict()
        for keys in el:
            if re.match(r'p\dStep', keys):
                 apps.append(el[keys]["ApplicationName"].upper()+" "+el[keys]["ApplicationVersion"].lower())
	    param[keys] = str(el[keys])
    	# make production
	labels = [ 'Production' ]
        node = Node(*labels, **param)
	graph.create(node)
	print "Production %s successfully saved." % el["RequestID"]
	# link with apps
	for a in apps:
		try:
        		p = list(selector.select("Project", name = a.split()[0], version = a.split()[1]))[0]
        		graph.create(Relationship(node, "USES", p))
			#print("Found: {0} {1}".format(a.split()[0], a.split()[1]))
		except IndexError:
		        print("Not found: {0} {1}".format(a.split()[0], a.split()[1]))
	param.clear()
	del apps[:]
	#break
