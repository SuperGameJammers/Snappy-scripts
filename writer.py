import csv
from py2neo import authenticate, Graph, Node, Relationship
import time
import os

start = time.time()
def main():
    authenticate("http://localhost:7474/browser/", "test", "test")
    shapes = "./mapatonGTFS/shapes.txt"
    path =  os.path.join(os.path.dirname(__file__), shapes)
    graph = Graph()
    authenticate("localhost:7474", "neo4j", "test")
    print graph
    with open(path , 'r+') as in_file:
        reader = csv.reader(in_file, delimiter=',')
        next(reader, None)
        batch = graph.cypher.begin()
        last = " "
        try:
            i = 0;
            j = 0;
            for row in reader:

                if row:
                    # shape_id,shape_pt_lat,shape_pt_lon,shape_pt_shape_pt_sequence
                    shape_id = strip(row[0])
                    shape_pt_lat = strip(row[1])
                    shape_pt_long = strip(row[2])
                    shape_pt_sequence = strip(row[3])

                    if int(shape_pt_sequence) > 0 and last == shape_id:
                        # latterNode = batch.append(graph.find_one("Node","shape_pt_sequence", str(int(shape_pt_sequence)-1)))
                        # # query = "({shape_pt_lat: %s, shape_pt_long: %s, shape_id: \"%s\", shape_pt_sequence: %s, id: %i})" % (shape_pt_lat, shape_pt_long, shape_id, shape_pt_sequence,j)
                        # newNode = batch.append(Node.cast({"shape_pt_lat": shape_pt_lat, "shape_pt_long": shape_pt_long, "shape_id": shape_id, "shape_pt_sequence": shape_pt_sequence}))
                        # newNodeQuery = "merge(routeNode:Node {shape_pt_lat: %s, shape_pt_long: %s, shape_id: \"%s\", shape_pt_sequence: %s, id: %i})" % (shape_pt_lat, shape_pt_long, shape_id, shape_pt_sequence,j)
                        # # newNode = graph.cypher.execute(newNodeQuery)
                        # batch.append(Relationship(latterNode, "INTERSECTS WITH", newNode))

                        latterQuery = "match(n:Node { shape_pt_sequence: %s})" % (str(int(shape_pt_sequence) - 1))
                        relationship = "MERGE (n)-[:IS_PART_OF]-(routeNode)"
                        query = "%s merge(routeNode:Node {shape_pt_lat: %s, shape_pt_long: %s, shape_id: \"%s\", shape_pt_sequence: %s}) %s" % (latterQuery, shape_pt_lat, shape_pt_long, shape_id, shape_pt_sequence,relationship)
                        batch.append(query)
                    else:
                        query = "merge(routeNode:Node {shape_pt_lat: %s, shape_pt_long: %s, shape_id: \"%s\", shape_pt_sequence: %s})" % (shape_pt_lat, shape_pt_long, shape_id, shape_pt_sequence)
                        batch.append(query)
                    i += 1
                    j += 1
                    last = shape_id
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

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time() - start
    print "Time to complete:", end