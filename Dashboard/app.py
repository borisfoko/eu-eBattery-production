import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from dbManager import Neo4jConnection

############ init db connection ##################
conn = Neo4jConnection(uri="bolt://localhost:7687", user="EBatteryDB_user", pwd="PostSome_XplomTwhj_76")


def init_db():
    # Establish connection to neo4j db
    conn.query("CREATE OR REPLACE DATABASE EBatteryDB")
    # Create nodes and relations
    counter = 1
    query_string2 = ""
    query_string3 = ""
    dfEUBattery = pd.read_csv('data/EUBatteryGenData.csv')
    query_part_1 = '''({}:Country '''
    query_part_2 = '''name:"{}",ref:"{}"'''
    query_part_3 = '''({})-[:import_from '''
    query_part_4 = ''']->({}'''
    query_part_5 = '''year:{},mineral:'cobalt',quantity:{}'''
    query_part_6 = '''year:{},mineral:'graphite',quantity:{}'''
    query_part_7 = '''year:{},mineral:'nickel',quantity:{}'''
    query_part_8 = '''year:{},mineral:'lithium',quantity:{}'''
    query_part_9 = '''year:{},mineral:'manganese',quantity:{}'''
    for index, row in dfEUBattery.iterrows():
        if index == 0:
            query_string2 = "CREATE " + query_part_1.format(row["import_country"])
            query_string2 = query_string2 + "{" + query_part_2.format(row["import_country"], "import country") + "}),"
        if 0 < index <= 6:
            query_string2 = query_string2 + "\n" + query_part_1.format(row["import_country"])
            query_string2 = query_string2 + "{" + query_part_2.format(row["import_country"], "import country") + "}),"
        if counter == 7 and 0 < index <= 56:
            query_string2 = query_string2 + "\n" + query_part_1.format(row["eu_country"])
            query_string2 = query_string2 + "{" + query_part_2.format(row["eu_country"], "europe") + "}),"
            counter = 0
        if row["cobalt"] >= 100:
            query_string3 = query_string3 + "\n" + query_part_3.format(row["import_country"])
            query_string3 = query_string3 + "{" + query_part_5.format(row["year"], row["cobalt"]) + "}"
            query_string3 = query_string3 + query_part_4.format(row["eu_country"]) + "),"
        if row["graphite"] >= 100:
            query_string3 = query_string3 + "\n" + query_part_3.format(row["import_country"])
            query_string3 = query_string3 + "{" + query_part_6.format(row["year"], row["graphite"]) + "}"
            query_string3 = query_string3 + query_part_4.format(row["eu_country"]) + "),"
        if row["nickel"] >= 100:
            query_string3 = query_string3 + "\n" + query_part_3.format(row["import_country"])
            query_string3 = query_string3 + "{" + query_part_7.format(row["year"], row["nickel"]) + "}"
            query_string3 = query_string3 + query_part_4.format(row["eu_country"]) + "),"
        if row["lithium"] >= 100:
            query_string3 = query_string3 + "\n" + query_part_3.format(row["import_country"])
            query_string3 = query_string3 + "{" + query_part_8.format(row["year"], row["lithium"]) + "}"
            query_string3 = query_string3 + query_part_4.format(row["eu_country"]) + "),"
        if row["manganese"] >= 100:
            query_string3 = query_string3 + "\n" + query_part_3.format(row["import_country"])
            query_string3 = query_string3 + "{" + query_part_9.format(row["year"], row["manganese"]) + "}"
            query_string3 = query_string3 + query_part_4.format(row["eu_country"]) + "),"
        counter = counter + 1
    query_string1 = query_string2 + "\n" + query_string3
    query_string1 = query_string1[:-1]
    conn.query(query_string1, db='EBatteryDB')


def get_countries(ref):
    query_string = "MATCH (country:Country {ref: '" + ref + "'})\nRETURN country.name AS country"
    resp = conn.query(query_string, db='EBatteryDB')
    temp = pd.DataFrame(data=resp)
    temp.columns = ['country']
    return temp


def get_minerals():
    query_string = "MATCH (n) WHERE EXISTS(n.mineral) RETURN DISTINCT \"node\" as entity, n.mineral AS mineral UNION ALL MATCH ()-[r]-() WHERE EXISTS(r.mineral) RETURN DISTINCT \"relationship\" AS entity, r.mineral AS mineral"
    resp = conn.query(query_string, db='EBatteryDB')
    temp = pd.DataFrame(data=resp)
    temp.columns = ['entity', 'mineral']
    return temp


def get_years():
    query_string = "MATCH (n) WHERE EXISTS(n.year) RETURN DISTINCT \"node\" as entity, n.year AS year UNION ALL MATCH ()-[r]-() WHERE EXISTS(r.year) RETURN DISTINCT \"relationship\" AS entity, r.year AS year"
    resp = conn.query(query_string, db='EBatteryDB')
    temp = pd.DataFrame(data=resp)
    temp.columns = ['entity', 'year']
    return temp


def get_yearly_import(country, mineral):
    if country == "europe":
        query_string = "MATCH (c:Country {ref:'europe'})-[imp:import_from {mineral:'" + mineral + "'}]-(ic:Country {ref:'import country'}) WITH 'europe' AS country, SUM(imp.quantity) AS quantity, imp.year AS year ORDER BY year RETURN year, country, quantity"
    else:
        query_string = "MATCH (c:Country {ref:'europe', name:'" + country + "'})-[imp:import_from {mineral:'" + mineral + "'}]-(ic:Country {ref:'import country'}) WITH c.name AS country, SUM(imp.quantity) AS quantity, imp.year AS year ORDER BY year RETURN year, country, quantity"
    resp = conn.query(query_string, db='EBatteryDB')
    # convert data into dataframes for each division
    temp = pd.DataFrame(data=resp)
    temp.columns = ['year', 'country', 'quantity']
    return temp


############ Loading Datasets ##################
eu_countries = get_countries("europe")
import_countries = get_countries("import country")

if __name__ == '__main__':
    # init_db()
    print("European countries:")
    print(eu_countries)
    print("Export countries:")
    print(import_countries)
    # print("List of minerals:")
    # print(get_minerals())
    print("Yearly cobalt import germany")
    print(get_yearly_import("germany", "cobalt"))
    print("Year list")
    print(get_years())
    # app.run_server(debug=True)
