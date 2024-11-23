from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
from tqdm import tqdm

load_dotenv()

NEO4J_URI = os.environ.get("NEO4J_URI")
NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")
AURA_INSTANCEID = os.environ.get("AURA_INSTANCEID")
AURA_INSTANCENAME = os.environ.get("AURA_INSTANCENAME")

def insert_data_from_json(data, uri, auth):
    """
    Inserts nodes and relationships into the Neo4j database based on a specified JSON schema.
    
    Args:
        data (dict): Data
        uri (str): URI for the Neo4j database.
        auth (tuple): A tuple of (username, password) for database authentication.
    """
    try:
        
        with GraphDatabase.driver(uri, auth=auth) as driver:
            with driver.session() as session:
                # Insert nodes
                print("Inserting nodes...")
                for node in tqdm(data.get("nodes", [])):
                    session.execute_write(create_node, node)
                
                # Insert relationships
                print("Inserting relationships...")
                for relationship in tqdm(data.get("relationships", [])):
                    session.execute_write(create_relationship, relationship)
    except Exception as e:
        print(f"An error occurred: {e}")

def create_node(tx, node_data):
    """
    Creates a node in the database.
    
    Args:
        tx: Neo4j transaction.
        node_data (dict): Dictionary containing node data with 'label' and 'properties'.
    """
    try:
        label = node_data.get("label")
        properties = node_data.get("properties", {})
        
        prop_str = ", ".join(f"{key}: ${key}" for key in properties.keys())
        query = f"MERGE (n:{label} {{{prop_str}}})"
        tx.run(query, **properties)
    except Exception as e:
        print(f"An error occurred while creating node: {e}")

def create_relationship(tx, rel_data):
    """
    Creates a relationship in the database.
    
    Args:
        tx: Neo4j transaction.
        rel_data (dict): Dictionary containing relationship data.
    """
    try:
        start_node = rel_data.get("start_node")
        end_node = rel_data.get("end_node")
        rel_type = rel_data.get("type")
        properties = rel_data.get("properties", {})
        
        start_label = start_node["label"]
        end_label = end_node["label"]
        start_criteria = " AND ".join(f"a.{key} = ${key}" for key in start_node["match_criteria"].keys())
        end_criteria = " AND ".join(f"b.{key} = ${key}" for key in end_node["match_criteria"].keys())
        prop_str = ", ".join(f"{key}: ${key}" for key in properties.keys())
        
        query = (
            f"MATCH (a:{start_label}), (b:{end_label}) "
            f"WHERE {start_criteria} AND {end_criteria} "
            f"MERGE (a)-[r:{rel_type} {{{prop_str}}}]->(b)"
        )
        
        params = {**start_node["match_criteria"], **end_node["match_criteria"], **properties}
        tx.run(query, **params)
    except Exception as e:
        print(f"An error occurred while creating relationship: {e}")


if __name__ == "__main__":
    import json

    with open("data.json") as f:
        data = json.load(f)

    insert_data_from_json(data, NEO4J_URI, (NEO4J_USERNAME, NEO4J_PASSWORD))

