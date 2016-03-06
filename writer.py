import csv
from py2neo import authenticate, Graph, Node, Relationship
import time
import os

authenticate("http://localhost:7474/browser/", "test", "test")
start = time.time()

shapes = "./mapatonGTFS/shapes.txt"
path =  os.path.join(os.path.dirname(__file__), shapes)
graph = Graph()
print graph
with open(shapes , 'r+') as in_file:
    reader = csv.reader(in_file, delimiter=',')
    next(reader, None)
    batch = graph.cypher.begin()

    try:
        i = 0;
        j = 0;
        for row in reader:
            if row:
                shape_id = strip(row[0])
                latitude = strip(row[1])
                longitude = strip(row[2])
                sequence = strip(row[3])

                query = "merge (routeNode:Node {latitude:{a}, longitude:{b},shape_id:{c}, sequence:{d})"
                batch.append(query, {"latitude": latitude,"longitude":longitude,"shape_id":shape_id,"sequence":sequence})
                i += 1
                j += 1
            batch.process()

            if (i == 1000): #submits a batch every 1000 lines read
                batch.commit()
                print j, "lines processed"
                i = 0
                batch = graph.cypher.begin()
        else: batch.commit() #submits remainder of lines read
        print j, "lines processed"

    except Exception as e:
        print e, row, reader.line_num

def strip(string): return''.join([c if 0 < ord(c) < 128 else ' ' for c in string]) #removes non utf-8 chars from string within cell


end = time.time() - start
print "Time to complete:", end