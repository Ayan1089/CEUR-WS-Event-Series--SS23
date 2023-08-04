from neo4j import GraphDatabase


class Driver:
    def connect(self):
        uri = "bolt://localhost:7687"  # Replace with your Neo4j URI
        user = "neo4j"  # Replace with your Neo4j username
        password = "Neelima@17"  # Replace with your Neo4j password

        driver = GraphDatabase.driver(uri, auth=(user, password))
        return driver
