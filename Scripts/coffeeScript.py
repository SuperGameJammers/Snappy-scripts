import csv
from py2neo import authenticate, Graph, Node, Relationship
import time
import os
import math
import numpy as np


def main():
    authenticate("http://localhost:7474/browser/", "test", "test")
    coffee = "./CoffeeSeparatedValues.csv"

    path = os.path.join(os.path.dirname(__file__), coffee)
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
                    lat2 = strip(row[6])
                    long2 = strip(row[7])
                    if(trip1!= trip2):
                        latterQuery = "match(n:Node { shape_pt_sequence: %s})" % (sequence1)
                        query = "match(routeNode:Node { shape_pt_sequence: %s})" % (sequence2)
                        relationship = "CREATE ( (n)-[:INTERSECTS_WITH {distance: %d}]->(routeNode))" % (points2distance([float(lat),float(long)],[float(lat2), float(long2)]))

                        query = "%s %s %s" % (latterQuery, query, relationship)
                        batch.append(query)

                        relationship = "CREATE ( (routeNode)-[:INTERSECTS_WITH {distance: %d}]->(n))" % (points2distance([float(lat),float(long)],[float(lat2), float(long2)]))
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


def points2distance(start, end):
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
    a = math.sin(d_latt / 2) ** 2 + math.cos(start_latt) * math.cos(end_latt) * math.sin(d_long / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return math.floor(6371000 * c)


if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time() - start
    print "Time to complete:", end
