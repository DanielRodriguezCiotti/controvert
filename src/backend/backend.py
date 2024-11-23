from neo4j import GraphDatabase,Session
from dotenv import load_dotenv
import os
import pandas as pd
import json

load_dotenv()

NEO4J_URI = os.environ.get("NEO4J_URI","")
NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")

with open("src/data_backend/mapping_controversies.json", "r") as f:
    mapping_controversies = json.load(f)
with open("src/data_backend/sectors_mapping.json", "r") as f:
    mapping_sectors = json.load(f)
with open("src/data_backend/sectors.json", "r") as f:
    sectors_list = json.load(f)

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

search_bar ="""
WITH "{sector}" AS targetSector
MATCH (s:Sector)
WITH s.sector_name AS sectorName, apoc.text.levenshteinSimilarity(targetSector, s.sector_name) AS similarity
ORDER BY similarity DESC
LIMIT 10
RETURN sectorName, similarity
"""

nb_controversies_distribution = """
MATCH (sector:Sector)<-[:BELONGS_TO]-(article:Article)-[:LINKED_TO]->(controversy:Controversy)
RETURN sector.sector_name AS sector_name, 
       COUNT(article) AS number_of_articles
"""
overview_data = """
MATCH (sector:Sector)<-[:BELONGS_TO]-(article:Article)
OPTIONAL MATCH (article)-[:LINKED_TO]->(controversy:Controversy)
OPTIONAL MATCH (article)-[:LEADS_TO]->(perf:Company_Performance)
RETURN 
    sector.sector_name AS sector_name, 
    COUNT(DISTINCT article) AS number_of_articles,
    MIN(perf.diff_2_months) AS min_perf_diff_2_months
"""

controversy_repartition = """
MATCH (sector:Sector)<-[:BELONGS_TO]-(article:Article)-[:LINKED_TO]->(controversy:Controversy)
WHERE sector.sector_name = "{sector}"
RETURN controversy.name AS controversy_name, 
       COUNT(article) AS number_of_articles
"""

financial_impact_by_controversy_per_sector = """
MATCH (sector:Sector)<-[:BELONGS_TO]-(article:Article)-[:LINKED_TO]->(controversy:Controversy)
MATCH (article)-[:LEADS_TO]->(perf:Company_Performance)
WHERE sector.sector_name = "{sector}"
RETURN perf.diff_2_months AS perf, controversy.name AS controversy, sector.sector_name AS sector
"""

articles_for_sector_controversy = """
MATCH (sector:Sector)<-[:BELONGS_TO]-(article:Article)-[:LINKED_TO]->(controversy:Controversy)
WHERE sector.sector_name = '{sector}'
OPTIONAL MATCH (article)-[:MENTIONS]->(company:Company)
OPTIONAL MATCH (article)-[:LEADS_TO]->(perf:Company_Performance)
RETURN article, perf.diff_2_months AS perf_2, perf.diff_1_month AS perf_1 , controversy.name AS controversy, company.company_name AS company
"""

def get_nb_controversies_per_activity(session:Session):
        
    result = session.run(query = overview_data)
    records = result.data()
    if len(records) == 0:
        return None
    data = pd.DataFrame(records)
    data["activity"] = data["sector_name"].apply(lambda x: mapping_sectors.get(x, x))
    data= data[data["activity"] != ""]
    # Group by activity summing the number of articles and min of min_perf_diff_2_months
    agg_data = data.groupby("activity",as_index=False).agg({"number_of_articles": "sum", "min_perf_diff_2_months": "min"}).reset_index()
    agg_data["percentage"] = agg_data["number_of_articles"] / agg_data["number_of_articles"].sum() * 100
    agg_data.loc[agg_data["percentage"] <= 1, "activity"] = "Other"
    agg_data = agg_data.sort_values("percentage", ascending=False)
    agg_data = agg_data.groupby("activity").agg({"number_of_articles": "sum", "percentage": "sum", "min_perf_diff_2_months": "min"}).reset_index()
    return agg_data[["activity","number_of_articles","percentage","min_perf_diff_2_months"]]


def get_data_for_risk_repartition(session:Session,sector:str):
    

    result = session.run(query = controversy_repartition.format(sector=sector))
    records = result.data()
    if len(records) == 0:
        return None
    data = pd.DataFrame(records)
    data["controversy_name"] = data["controversy_name"].apply(lambda x: mapping_controversies.get(x, x))
    data = data.groupby("controversy_name").sum().reset_index()
    data["percentage"] = data["number_of_articles"] / data["number_of_articles"].sum() * 100
    data = data.sort_values("percentage", ascending=False)
    data.loc[data["percentage"] <= 5, "controversy_name"] = "Other"
    data = data.groupby("controversy_name").sum().reset_index()
    return data

def get_data_nb_controverties_distrib(session:Session):
    
    result = session.run(query = nb_controversies_distribution)
    records = result.data()
    if len(records) == 0:
        return None
    data = pd.DataFrame(records)
    return data

def get_data_financial_impact_by_controversy_per_sector(session:Session,sector:str):
    
    result = session.run(query = financial_impact_by_controversy_per_sector.format(sector=sector))
    records = result.data()
    if len(records) == 0:
        return None
    data = pd.DataFrame(records)
    data["controversy"] = data["controversy"].apply(lambda x: mapping_controversies.get(x, x))
    return data.groupby(["controversy","sector"]).min().reset_index()

def get_articles_for_sector_controversy(session:Session,sector:str):
    
    try:
        result = session.run(query = articles_for_sector_controversy.format(sector=sector))
        records = result.data()

        if len(records) == 0:
            return None
        data = pd.DataFrame(records)
        data["controversy"] = data["controversy"].apply(lambda x: mapping_controversies.get(x, x))
        data["date"] = data["article"].apply(lambda x: x.get("date"))
        data["url"] = data["article"].apply(lambda x: x.get("url"))
        # Remove duplicates
        data.drop_duplicates(subset=["url","controversy"], inplace=True)
        data.sort_values(["date"], inplace=True, ascending=False)
        return data
    except Exception as e:
        print(e)
        return None
        
# get_articles_for_sector_controversy(driver.session(),sector="Extraction de minerais métalliques",controversy = "Environmental Controversies").to_csv("result_backend_articles.csv",index=False)
print(get_nb_controversies_per_activity(driver.session()))
