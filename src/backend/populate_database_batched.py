from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
from tqdm import tqdm
from itertools import islice

load_dotenv()

NEO4J_URI = os.environ.get("NEO4J_URI")
NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")

BATCH_SIZE = 100  # Define the batch size for insertion


def insert_data_from_json(data, uri, auth):
    """
    Inserts nodes and relationships into the Neo4j database in batches.
    
    Args:
        data (dict): Data
        uri (str): URI for the Neo4j database.
        auth (tuple): A tuple of (username, password) for database authentication.
    """
    try:
        with GraphDatabase.driver(uri, auth=auth) as driver:
            with driver.session() as session:
                # Insert nodes in batches
                print("Inserting nodes in batches...")
                for batch in tqdm(batch_data(data.get("nodes", []), BATCH_SIZE), total=len(data.get("nodes", [])) // BATCH_SIZE):
                    session.execute_write(create_nodes_batch, batch)

                # Insert relationships in batches
                print("Inserting relationships in batches...")
                for batch in tqdm(batch_data(data.get("relationships", []), BATCH_SIZE), total=len(data.get("relationships", [])) // BATCH_SIZE):
                    session.execute_write(create_relationships_batch, batch)
    except Exception as e:
        print(f"An error occurred: {e}")


def batch_data(iterable, batch_size):
    """
    Splits data into batches of a given size.
    
    Args:
        iterable (iterable): The data to batch.
        batch_size (int): The size of each batch.
    Yields:
        List of items in each batch.
    """
    it = iter(iterable)
    while batch := list(islice(it, batch_size)):
        yield batch


def create_nodes_batch(tx, nodes_batch):
    """
    Creates a batch of nodes in the database.
    
    Args:
        tx: Neo4j transaction.
        nodes_batch (list): List of node data dictionaries.
    """
    try:
        for node_data in nodes_batch:
            label = node_data.get("label")
            properties = node_data.get("properties", {})
            prop_str = ", ".join(f"{key}: ${key}" for key in properties.keys())
            query = f"MERGE (n:{label} {{{prop_str}}})"
            tx.run(query, **properties)
    except Exception as e:
        print(f"An error occurred while creating nodes batch: {e}")


def create_relationships_batch(tx, relationships_batch):
    """
    Creates a batch of relationships in the database.
    
    Args:
        tx: Neo4j transaction.
        relationships_batch (list): List of relationship data dictionaries.
    """
    try:
        for rel_data in relationships_batch:
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
        print(f"An error occurred while creating relationships batch: {e}")


if __name__ == "__main__":
    import json

    with open("data.json") as f:
        data = json.load(f)

    insert_data_from_json(data, NEO4J_URI, (NEO4J_USERNAME, NEO4J_PASSWORD))
