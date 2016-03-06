import csv
from py2neo import authenticate, Graph, Node, Relationship
import time
import os
import math

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
                        latterQuery = "match(n:Node { shape_pt_sequence: %s})" % (str(int(shape_pt_sequence) - 1))
                        relationship = "CREATE (n)-[:ROUTE_OF {distance: %d}]->(routeNode)" % (points2distance([float(lastLat),float(lastLong)],[float(shape_pt_lat),float(shape_pt_long)]))
                        query = "%s merge(routeNode:Node {shape_pt_lat: %s, shape_pt_long: %s, shape_id: \"%s\", shape_pt_sequence: %s}) %s" % (latterQuery, shape_pt_lat, shape_pt_long, shape_id, shape_pt_sequence,relationship)
                        batch.append(query)
                        relationship = "CREATE (routeNode)-[:ROUTE_OF {distance: %d}]-(n)" % (points2distance([float(lastLat),float(lastLong)],[float(shape_pt_lat),float(shape_pt_long)]))
                        query = "%s merge(routeNode:Node {shape_pt_lat: %s, shape_pt_long: %s, shape_id: \"%s\", shape_pt_sequence: %s}) %s" % (latterQuery, shape_pt_lat, shape_pt_long, shape_id, shape_pt_sequence,relationship)

                        batch.append(query)
                    else:
                        query = "merge(routeNode:Node {shape_pt_lat: %s, shape_pt_long: %s, shape_id: \"%s\", shape_pt_sequence: %s})" % (shape_pt_lat, shape_pt_long, shape_id, shape_pt_sequence)
                        batch.append(query)
                    i += 1
                    j += 1
                lastLat, lastLong = shape_pt_lat, shape_pt_long
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

def points2distance(start,  end):
    """
      Calculate distance (in kilometers) between two points given as (long, latt) pairs
      based on Haversine formula (http://en.wikipedia.org/wiki/Haversine_formula).
      Implementation inspired by JavaScript implementation from
      http://www.movable-type.co.uk/scripts/latlong.html
      Accepts coordinates as tuples (deg, min, sec), but coordinates can be given
      in any form - e.g. can specify only minutes:
      (0, 3133.9333, 0)
      is interpreted as
      (52.0, 13.0, 55.998000000008687)
      which, not accidentally, is the lattitude of Warsaw, Poland.
    """
    start_long = math.radians(start[0])
    start_latt = math.radians(start[1])
    end_long = math.radians(end[0])
    end_latt = math.radians(end[1])
    d_latt = end_latt - start_latt
    d_long = end_long - start_long
    a = math.sin(d_latt/2)**2 + math.cos(start_latt) * math.cos(end_latt) * math.sin(d_long/2)**2
    c = 2 * math.atan2(math.sqrt(a),  math.sqrt(1-a))
    return  math.floor(6371000 * c)

def strip(string): return''.join([c if 0 < ord(c) < 128 else ' ' for c in string]) #removes non utf-8 chars from string within cell

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time() - start
    print "Time to complete:", end