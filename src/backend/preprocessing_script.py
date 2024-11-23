import pandas as pd
import json
import ast
from tqdm import tqdm


def parse_list_string(list_string):
    """
    Parses a string that represents a list into an actual Python list.
    
    Args:
        list_string (str): A string representing a list (e.g., "['item1', 'item2']").
        
    Returns:
        list: Parsed Python list of strings.
    """
    try:
        if type(list_string) is not str:
            # print("The provided string is not a string : {}".format(list_string))
            return []
        # Handle potential quotes inconsistencies and parse the string
        cleaned_string = list_string.replace('""', '"')
        parsed_list = ast.literal_eval(cleaned_string)
        
        # Ensure the result is a list
        if isinstance(parsed_list, list):
            return parsed_list
        else:
            raise ValueError("The provided string does not represent a list.")
    except (SyntaxError, ValueError) as e:
        print(f"Error parsing list string: {e}")
        return []
    


def csv_to_json(csv_file_path, json_file_path):
    """
    Converts a CSV file into a JSON format suitable for Neo4j insertion.
    
    Args:
        csv_file_path (str): Path to the CSV file.
        json_file_path (str): Path where the JSON file will be saved.
    """
    nodes = []
    relationships = []

    # Read CSV file
    dataframe = pd.read_csv(csv_file_path)
    # for column in dataframe.columns:
    #     dataframe[column] = dataframe[column].astype(str)
    records = dataframe.to_dict(orient='records')

    companies_counter = 0
    sectors_counter = 0
    controversies_counter = 0

    for row in tqdm(records):
        # try:
        # Parse companies, sectors, controversies, and articles
        companies = parse_list_string(row.get('companies', '[]'))
        sectors = parse_list_string(row.get('sectors', '[]'))
        controversies = parse_list_string(row.get('controverts', '[]'))
        article_name = row.get('label')
        link = row.get('link')
        # except Exception as e:
        #     counter += 1
        #     print(f"An error occurred: {e}")
        #     print(f"Row: {row}")
        
        # Add nodes for companies
        for company in companies:
            nodes.append({
                "label": "Company",
                "properties": {
                    "name": company
                }
            })
            companies_counter += 1
        
        # Add nodes for sectors
        for sector in sectors:
            nodes.append({
                "label": "Sector",
                "properties": {
                    "sector_name": sector
                }
            })
            sectors_counter += 1
        
        # Add nodes for controversies
        for controversy in controversies:
            nodes.append({
                "label": "Controversy",
                "properties": {
                    "name": controversy
                }
            })
            controversies_counter += 1
        
        # Add node for article
        nodes.append({
            "label": "Article",
            "properties": {
                "name": article_name,
                "url": link
            }
        })

        # Create relationships
        for company in companies:
            for sector in sectors:
                relationships.append({
                    "start_node": {
                        "label": "Company",
                        "match_criteria": {
                            "name": company
                        }
                    },
                    "end_node": {
                        "label": "Sector",
                        "match_criteria": {
                            "sector_name": sector
                        }
                    },
                    "type": "BELONGS_TO",
                    "properties": {}
                })

            relationships.append({
                "start_node": {
                    "label": "Article",
                    "match_criteria": {
                        "url": link
                    }
                },
                "end_node":{
                    "label": "Company",
                    "match_criteria": {
                        "name": company
                    }
                },
                "type": "MENTIONS",
                "properties": {}
            })
        
        for controversy in controversies:
            relationships.append({
                "start_node": {
                    "label": "Article",
                    "match_criteria": {
                        "url": link
                    }
                },
                "end_node": {
                    "label": "Controversy",
                    "match_criteria": {
                        "name": controversy
                    }
                },
                "type": "LINKED_TO",
                "properties": {}
            })

    # Remove duplicates in nodes and relationships
    unique_nodes = {json.dumps(node, sort_keys=True) for node in nodes}
    unique_nodes = [json.loads(node) for node in unique_nodes]
    unique_relationships = {json.dumps(rel, sort_keys=True) for rel in relationships}
    unique_relationships = [json.loads(rel) for rel in unique_relationships]


    # Create final JSON structure
    final_json = {
        "nodes": list(unique_nodes),
        "relationships": list(unique_relationships)
    }

    # Write to output JSON file
    with open(json_file_path, 'w', encoding='utf-8') as jsonfile:
        json.dump(final_json, jsonfile, indent=4, ensure_ascii=False)

    print(f"Total articles: {len(records)}")
    print(f"Total companies: {companies_counter}")
    print(f"Total sectors: {sectors_counter}")
    print(f"Total controversies: {controversies_counter}")
    print(f"Total relationships: {len(unique_relationships)}")
    # print(f"Successfully converted CSV file to JSON. {counter} errors occurred over {len(records)} records.")

if __name__ == "__main__":
    csv_to_json("llm_output.csv", "data.json")


