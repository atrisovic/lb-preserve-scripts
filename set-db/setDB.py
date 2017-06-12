from py2neo import Graph, Node, Relationship, NodeSelector

class softConfDB(object):

    def __init__(self):
        self.graph = Graph()
        self.selector = NodeSelector(self.graph)

    def createProject(self, name, version):
        ''' Create new Project node '''
        self.graph.create(Node("Project", name=name.upper(), version=version.lower()))
    
    def createPlatform(self, platform):
        ''' Create new Platform node '''
        self.graph.create(Node("Platform", platform=platform.lower()))

    def createProjectPlatformRelationship(self, p, v, plat):
        ''' Create PLATFORM relationship between two a project and a platform '''
        try:
            project = list(self.selector.select("Project", name = p.upper(), version = v.lower()))[0]
            platform = list(self.selector.select("Platform", platform = plat.lower()))[0]
            self.graph.create(Relationship(project, "PLATFORM", platform))
        except IndexError:
            print("Node not found")

    def createProjectProjectRelationshio(self, p1, v1, p2, v2):
        ''' Create REQUIRES relationship between two projects '''
        try:
            a = list(self.selector.select("Project", name = p1.upper(), version = v1.lower()))[0]
            b = list(self.selector.select("Project", name = p2.upper(), version = v2.lower()))[0]
            self.graph.create(Relationship(a, "REQUIRES", b))
        except IndexError:
            print("Node not found")

    def projectExists(self, p, v):
        project = list(self.selector.select("Project", name = p.upper(), version = v.lower()))
        if len(project) == 0:
            return False
        return True

    def platformExists(self, plat):
        platform = list(self.selector.select("Platform", platform = plat.lower()))
        if len(platform) == 0:
            return False
        return True

    def listPlatforms(self, p, v):
        ''' List platforms released for the project version '''
        query = "MATCH (a:Project)--(p:Platform) WHERE a.name ='"+p.upper()+"' AND a.version = '"+v.lower()+"' RETURN p.platform"
        return self.graph.run(query).data()
   
    def listProjects(self):
        ''' List all projects '''
        return list(self.selector.select("Project"))

    def deleteAll(self):
        ''' Delete everything '''
        self.graph.delete_all()

def main():
    scdb = softConfDB()
    scdb.deleteAll()

    f = open("softconfdb.json")
    content = f.read()
    f.close()
    rel = eval(content)

    for el in rel:
        n1 = el[0]
        r = el[1]
        n2 = el[2]
        if not scdb.projectExists(n1[0], n1[1]):
                scdb.createProject(n1[0], n1[1])
                print("project created "+n1[0]+" "+n1[1]+"\n")
        if r == "REQUIRES":
            if not scdb.projectExists(n2[0], n2[1]):
                    scdb.createProject(n2[0], n2[1])
            scdb.createProjectProjectRelationshio(n1[0], n1[1], n2[0], n2[1])
            print("requires created "+n1[0]+" "+n1[1]+" "+n2[0]+" "+n2[1]+"\n")
        elif r == "PLATFORM":
            if not scdb.platformExists(n2):
                    scdb.createPlatform(n2)
            scdb.createProjectPlatformRelationship(n1[0], n1[1], n2)
            print("platform created "+n2+" "+n1[0]+" "+n1[1]+"\n")
        else:
            print("shouldn't be here: " + r)

if __name__ == "__main__":
    main()
