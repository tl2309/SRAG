# -*- encoding: utf-8 -*-
# @Time : 2024/8/1 20:13
# @Author: TLIN

from SPARQLWrapper import SPARQLWrapper, JSON

# Set the SPARQL endpoint
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

# Define the SPARQL query
query = """
SELECT (COUNT(?person) AS ?count)
WHERE 
{
  ?person p:P166 ?statement.
  ?statement ps:P166 wd:Q4178415. # ACM Fellowship
  ?statement pq:P585 ?date. # with a point in time
  BIND(YEAR(?date) AS ?year)
  FILTER(?year = 2023)
}
"""

sparql.setQuery(query)

# Set the return format to JSON
sparql.setReturnFormat(JSON)

# Execute the query and convert the response to JSON
results = sparql.query().convert()

# Extract and print the number of ACM Fellows in 2022
count = results["results"]["bindings"][0]["count"]["value"]
print(f"Number of ACM fellows in 2023: {count}")
