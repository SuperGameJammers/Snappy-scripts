import csv
from py2neo import authenticate, Graph, Node, Relationship
import time
import os

start = time.time()


def main():
    authenticate("http://localhost:7474/browser/", "test", "test")
    shapes = "./intersections.txt"
    path = os.path.join(os.path.dirname(__file__), shapes)
    graph = Graph()
    authenticate("localhost:7474", "neo4j", "test")
    print graph
    with open(path, 'r+') as in_file:
        reader = csv.reader(in_file, delimiter=',')
        next(reader, None)
        batch = graph.cypher.begin()
        try:
            i = 0;
            j = 0;
            for row in reader:

                if row:
                    # trip1, sequence1, lat, long, trip2, sequence2, lat2,long2
                    trip1 = strip(row[0])
                    sequence1 = strip(row[1])
                    lat = strip(row[2])
                    long = strip(row[3])
                    trip2 = strip(row[4])
                    sequence2 = strip(row[5])

                    latterQuery = "match(n:Node { shape_pt_sequence: %s})" % (sequence1)
                    query = "match(routeNode:Node { shape_pt_sequence: %s})" % (sequence2)
                    # print "sequence " + sequence1
                    # print "sequence2 " + sequence2

                    relationship = "MERGE( (n)-[:INTERSECTS_WITH]-(routeNode))"
                    query = "%s %s %s" % (latterQuery, query, relationship)
                    batch.append(query)

                    i += 1
                    j += 1
                batch.process()

                if (i == 1000):  # submits a batch every 1000 lines read
                    batch.commit()
                    print j, "lines processed"
                    i = 0
                    batch = graph.cypher.begin()
            else:
                batch.commit()  # submits remainder of lines read
            print j, "lines processed"

        except Exception as e:
            print e, row, reader.line_num


def strip(string): return ''.join(
        [c if 0 < ord(c) < 128 else ' ' for c in string])  # removes non utf-8 chars from string within cell


if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time() - start
    print "Time to complete:", end
