import json
import os
import re

from eventseries.src.main.import2Neo4j.Neo4jDriver import Driver
from eventseries.src.main.import2Neo4j.neo_4j_connection import Neo4jConnection


# from import2Neo4j.Neo4jConnection import Neo4jConnection


class ImportData(object):
    def fetch_data_and_import(self):
        ## Create connection
        conn = Neo4jConnection(
            uri="bolt://localhost:7687", user="neo4j", pwd="Neelima@17"
        )
        # resources_path = os.path.abspath("resources")
        # events = os.path.join(resources_path, "events_with_ordinal.json")
        query_string = """
        CALL apoc.load.json("file:///C:\\Users\\Vinay Shah\\Desktop\\All_Files\\CEUR-WS-Event-Series--SS23\\eventseries\\src\\main\\resources\\person.json")
    
        YIELD value
        MERGE (p:Person {name: value.name})
        SET p.age = value.age
        WITH p, value
        UNWIND value.children AS child
        MERGE (c:Person {name: child})
        MERGE (c)-[:CHILD_OF]->(p);
        """
        print(conn.query(query_string, db="neo4j"))

    def new_func(self):
        resources_path = os.path.abspath("resources")
        events = os.path.join(resources_path, "events_with_ordinal.json")

        with open(events, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)
        return data

    def import_data_to_neo4j(self, data):
        driver = Driver()
        driver_1 = driver.connect()
        with driver_1.session() as session:
            # Extract the necessary data from the JSON object
            # and construct your Cypher query accordingly
            cypher_query = f"CREATE (n:Person {{name: '{data['name']}'}})"
            cypher_query = f"FOREACH (node in $data | CREATE (m:Events) SET m = node)"
            for node in data:
                string = node["event"]
                match = re.search(r"Q(\d+)", string)
                if match:
                    qid = match.group(0)
                    cypher_query = (
                        "MERGE (m:Events {event: $node.event}) "
                        "SET m += $node "
                        "SET m.qid = $qid"
                    )
                    # cypher_query = f"MATCH (n:Events) WHERE n.event = $string SET n.qid = $qid"
                    session.run(cypher_query, node=node, qid=qid)
            # graph = Graph()
            #
            # # Iterate over the JSON data and add triples to the RDF graph
            # for triple in data:
            #     subject = URIRef(triple['subject'])
            #     predicate = URIRef(triple['predicate'])
            #     if triple['object'].startswith('http://'):
            #         obj = URIRef(triple['object'])
            #     elif triple['object'].startswith('https://'):
            #         obj = URIRef(triple['object'])
            #     else:
            #         if isinstance(triple['object'], Literal) and triple['object'].datatype == XSD.dateTime:
            #             obj = Literal(triple['object'], datatype=XSD.dateTime)
            #         else:
            #             obj = Literal(triple['object'])
            #             print(obj)
            #             # if re.match(r"'", Literal(triple['object'])):
            #             #     obj = re.sub(r"'", r"\\'", Literal(triple['object']))
            #
            #     graph.add((subject, predicate, obj))
            # cypher_queries = []
            # for s, p, o in graph:
            #     cypher_query = (
            #         f"MERGE (subject:Entity {{uri: `{s}`}}) "
            #         f"MERGE (object:Entity {{uri: `{o}`}}) "
            #         f"MERGE (subject)-[:`{p}`]->(object)"
            #     )
            #     cypher_queries.append(cypher_query)
            #
            # # Execute the Cypher queries
            # for query in cypher_queries:
            #     session.run(query)

    def new_func_series(self):
        notebooks_path = os.path.abspath("../../../notebooks")
        events_series = os.path.join(notebooks_path, "event_series_flattened.json")

        with open(events_series, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)
        return data

    def import_event_series_to_neo4j(self, data):
        driver = Driver()
        driver_1 = driver.connect()
        with driver_1.session() as session:
            for node in data:
                my_dict = dict()
                for i in node:
                    # print(node[i]['value'])
                    if i.lower() == "series":
                        match = re.search(r"Q(\d+)", node[i]["value"])
                        if match:
                            qid = match.group(0)
                    my_dict[i] = node[i]["value"]
                print(my_dict)
                cypher_query = (
                    "MERGE (m:Events_Series {series: $my_dict.series}) "
                    "SET m += $my_dict "
                    "SET m.qid = $qid"
                )
                session.run(cypher_query, my_dict=my_dict, qid=qid)

    # def counter_func(self):
    #     data_path = os.path.abspath("../../../data")
    #     data_json = os.path.join(data_path, "EventsWithoutSeries.json")
    #
    #     with open(data_json, "r", encoding='utf-8') as json_file:
    #         data = json.load(json_file)
    #         count = [0,0,0,0]
    #         for i in data:
    #             if 'description' in i:
    #                 if 'workshop' in i['description'].lower():
    #                     count[0]+=1
    #
    #                 elif 'conference' in i['description'].lower():
    #                     count[1]+=1
    #
    #                 else:
    #                     count[2]+=1
    #
    #             else:
    #                 count[3] += 1
    #         print("Workshops: ", count[0])
    #         print("Conferences: ", count[1])
    #         print("Others: ", count[2])
    #         print("No desc attr: ", count[3])

    def check_event_with_series(self, events_data, series_data):
        driver = Driver()
        driver_1 = driver.connect()
        with driver_1.session() as session:
            my_dict = dict()
            for series in series_data:
                my_dict[series["series"]["value"]] = list()
            for series in series_data:
                for event in events_data:
                    if "series" in event:
                        if series["series"]["value"] == event["series"]:
                            my_dict[series["series"]["value"]].append(event["event"])

            for key, values in my_dict.items():
                for value in values:
                    cypher_query = (
                        "MATCH (m:Events_Series {series: $key}), (n:Events {event: $value}) "
                        "CREATE (n)-[:`Part-of-the-Series`]->(m)"
                    )
                    session.run(cypher_query, key=key, value=value)
