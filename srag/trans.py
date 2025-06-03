# -*- encoding: utf-8 -*-

import pandas
from txtai import LLM
import sqlite3
import csv
from tqdm import tqdm
import time
import json
import os
import requests
import os
import pandas as pd
import json

llm = LLM("/home/Mistral-7B-OpenOrca-AWQ")


def get_wikidata_id(search_term):
    """Fetch the Wikidata identifier for a given search term."""
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbsearchentities",
        "language": "en",
        "format": "json",
        "search": search_term
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if data['search']:
            return data['search'][0]['id']
        else:
            return None
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return None


def name_belongs_to_entity(wikidata_id, topicid):
    """Checks if the entity with the given Wikidata ID is an ACM Fellow."""
    query = """
    SELECT ?award WHERE {
      wd:%s p:P166 ?statement.
      ?statement ps:P166 ?award.
      FILTER(?award = wd:%s)
    }
    """ % (wikidata_id, topicid)

    url = "https://query.wikidata.org/sparql"
    headers = {"Accept": "application/sparql-results+json"}
    params = {"query": query, "format": "json"}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        return len(data['results']['bindings']) > 0
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return False


# Example usage
# name = "C. William Gear"  # Replace with the name you're interested in
# wikidata_id = get_wikidata_id(name)
# topic = "ACM fellow"
# topicid = get_wikidata_id(topic)
# print(topicid)
# if wikidata_id:
#     if name_belongs_to_entity(wikidata_id, topicid):
#         print(f"{name} (Wikidata ID: {wikidata_id}) is  {topic}.")
#     else:
#         print(f"{name} (Wikidata ID: {wikidata_id}) is not {topic}.")
# else:
#     print(f"No Wikidata ID found for '{name}'.")


def search_wikidata_property(keyword):
    """
    Search for a Wikidata property ID based on a keyword.
    Returns a list of matching properties with their IDs and descriptions.
    """
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbsearchentities",
        "language": "en",
        "type": "property",
        "format": "json",
        "search": keyword
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raises an HTTPError if the response status code is 4XX/5XX
        data = response.json()

        properties = []
        prop_id = ''
        for result in data.get("search", []):
            prop_id = result.get("id")
            label = result.get("label")
            description = result.get("description", "No description available.")
            properties.append((prop_id, label, description))

        return properties
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return []


def get_entities_wikipedia_titles(property_id, value_id):
    """Fetches Wikipedia titles for entities based on a specified property and its value."""
    query = f"""
      SELECT ?entity ?entityLabel ?article WHERE {{
      ?entity p:{property_id} ?statement.
      ?statement ps:{property_id} wd:{value_id}.
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
      OPTIONAL {{
        ?article schema:about ?entity;
                 schema:inLanguage "en";
                 schema:isPartOf <https://en.wikipedia.org/>.
      }}
    }}
    """
    url = "https://query.wikidata.org/sparql"
    headers = {"Accept": "application/sparql-results+json"}
    params = {"query": query}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        titles = []
        for item in data['results']['bindings']:
            if 'article' in item:
                article_url = item['article']['value']
                title = article_url.split('/')[-1]  # Extract Wikipedia title from URL
                titles.append(title)

        return titles
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return []


def get_wikipedia_first_paragraph(title):
    """Fetches the first paragraph of a Wikipedia page for a given title."""
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "titles": title,
        "prop": "extracts",
        "exintro": True,
        "explaintext": True,
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        page = next(iter(data['query']['pages'].values()))
        if 'extract' in page:
            content = page['extract']
            first_paragraph = content.split('\n')[0]  # Assuming the first paragraph is before the first newline
            return first_paragraph
        else:
            return "Content not available."
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return "Error retrieving content."

def count_jsonl_lines(file_path):
    with open(file_path, 'r') as file:
        line_count = 0
        for line in file:
            line_count += 1
        return line_count

def Transformation(path):

    total_iterations = count_jsonl_lines(os.path.join(path, 'entities_first_paragraph.jsonl'))
    progress_bar = tqdm(total=total_iterations)
    with open(os.path.join(path, 'entities_first_paragraph.jsonl'), 'r') as file:
        # Read each line in the file
        for line in file:
            # Convert the JSON string to a Python dictionary
            data = json.loads(line)

            # Process the data as needed
            # print(data)
            topic = data['topic']#table
            titles = data['entity']
            properties = data['properties']

            context = ""
            conn = sqlite3.connect('document_store_0.db')
            cursor = conn.cursor()
            propertys = properties
            property_i = ['title']
            for line in propertys.split(','):
                property_i.append(line.replace(' ', ''))

            topic = topic.replace(' ', '').replace('-', '')
            table_name = str(topic)
            cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
            columns_with_types = ', '.join([f'{col} TEXT' for col in property_i])
            print('columns_with_types= ', columns_with_types)
            cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
              {columns_with_types}
            )
            ''')

            # for node in list(g.scan()):
            #   gtext = g.attribute(node, "text")
            #   gname = g.attribute(node, "id")

            #   wikidata_id = get_wikidata_id(gname)

            for entity in titles:  # Limiting to first 5 results for demonstration
                # first_paragraph = get_wikipedia_first_paragraph(title)
                # print(f"{name} (Wikidata ID: {wikidata_id}) is an ACM Fellow.")
                title = entity['title']
                first_paragraph = entity['firstparagraph']
                prompt = f"""<|im_start|>system
                You are a friendly assistant. You answer questions from users.<|im_end|>
                <|im_start|>user
                Extracting property value using only the context below.If the property value is not mentioned in the context, using 'None'to fill.
                The output size must meet the size of property. Just output the property value in order one by one.
                property: {propertys}
                context: {first_paragraph} <|im_end|>
                <|im_start|>assistant
                """
                data = llm(prompt)
                # print(data)
                value_i = ['\'' + title + '\'']
                for line in data.split('\n'):
                    if len(line.split(':')) > 1:
                        # print(line.split(':')[1])
                        value_i.append('\'' + line.split(':')[1].replace(',', '').replace('\'', '') + '\'')
                # print(len(property_i))
                if len(value_i) == len(property_i):
                    # print(len(value_i))
                    # print(len(property_i))
                    sql = f'INSERT INTO {table_name} ({", ".join(property_i)}) VALUES ({", ".join(value_i)})'
                    # print(sql)
                    cursor.execute(sql)
                    # context.join(data)

            cursor.execute(f'SELECT * FROM {table_name}')
            # Fetch all rows from the query result
            rows = cursor.fetchall()
            # Close the cursor and connection to the database
            cursor.close()
            conn.close()

            # Write the fetched data to a CSV file
            with open(f'outputcsv/{topic}.csv', 'w', newline='') as file:
                csv_writer = csv.writer(file)
                # Write the header row if needed
                csv_writer.writerow([column[0] for column in cursor.description])
                # Write the data rows
                csv_writer.writerows(rows)

            print("CSV file has been created successfully.")

            progress_bar.update(1)

        progress_bar.close()


def gen_query():
    fout_queries = open(os.path.join('Stran/multi_entities_query.jsonl'), 'a')
    fout_entity = open(os.path.join('Stran/entities_fp.jsonl'), 'a')

    topics = [
        "Ivy League",
        "Russell Group",
        "Group of Eight",
        "U15",
        "C9 League",
        "Universitas 21",
        "Association of American Universities",
        "European University Association",
        "League of European Research Universities",
        "Coimbra Group",
        "Association of Commonwealth Universities",
        "Magna Charta Universitatum",
        "Association of Southeast Asian Institutions of Higher Learning",
        "The Guild of European Research-Intensive Universities",
        "Golden Triangle",
        "U15",
        "UNICA",
        "Ligue des Bibliothèques Européennes de Recherche",
        "Matariki Network of Universities",
        "Pacific Rim Universities",
        "Association of East Asian Research Universities",
        "Worldwide Universities Network",
        "Association of Pacific Rim Universities",
        "University of the Arctic",
        "Agence universitaire de la Francophonie",
        "International Alliance of Research Universities",
        "International Forum of Public Universities",
        "Association of Atlantic Universities",
        "Association of Jesuit Colleges and Universities",
        "Association of Catholic Colleges and Universities",
        "Council of Independent Colleges",
        "Association of Public and Land-grant Universities",
        "Universities Research Association",
        "Association of Commonwealth Universities",
        "International Association of Universities",
        "Association of Universities of Asia and the Pacific",
        "European Consortium of Innovative Universities",
        "European University Association",
        "Association of MBAs",
        "AACSB International",
        "AMBA",
        "EFMD Quality Improvement System",
        "Triple Crown",
        "Orbis",
        "Universitas 21 Global",
        "Global U8 Consortium",
        "Global Alliance in Management Education",
        "Partnership in International Management ",
        "Association to Advance Collegiate Schools of Business",
        "European Foundation for Management Development",
        "Association of African Universities",
        "Association of Indian Universities",
        "Association of Arab Universities",
        "Association of Universities of Latin America and the Caribbean",
        "ASEAN University Network",
        "Santander Network",
        "Mediterranean Universities Union",
        "Inter-University Council for East Africa",
        "Southern African Regional Universities Association",
        "Arab European Leadership Network in Higher Education",
        "Euro-Mediterranean University",
        "Network of International Business Schools ",
        "Academic Consortium 21",
        "Association of Christian Universities and Colleges in Asia",
        "Baltic Sea Region University Network",
        "Association of MBAs",
        "EFMD Global Network",
        "CHEA International Quality Group",
        "Global Network for Advanced Management",
        "International Association of University Presidents",
        "Association of Pacific Coast Universities",
        "Midwest Universities Consortium for International Activities",
        "African Research Universities Alliance",
        "Union of Mediterranean Universities",
        "Euroleague for Life Sciences",
        "Asia-Pacific Association for International Education",
        "Asia-Pacific Quality Network",
        "Asia University Federation",
        "Association of East Asian Research Universities",
        "European Association for International Education",
        "International Association of Universities and Colleges of Art, Design and Media",
        "Innovation Alliance of the China Universities",
        "China Education Association for International Exchange",
        "Higher Education Evaluation Center of China",
        "China Academic Degrees and Graduate Education Association",
        "National University Technology Network",
        "Consortium for North American Higher Education Collaboration",
        "Network of International Business and Economic Schools"
    ]  # , "Cities of the World", "Presidents of the US", "Chemical Elements", "Summer Olympic Games", "Nobel Prize in Chemistry"]
    properties = ["name, start year, president, location, staff number"]
    # ,
    #             "population, geographic coordinates, altitude, GDP",
    #             "term lengths, political parties, vice-presidents, birth states, previous occupations",
    #             "atomic number, atomic mass, boiling point, melting point, electron configuration",
    #             "host cities, number of participating countries, total number of events, medal tally, records broken",
    #             "categories , year of award, country of origin, field of contribution" ]

    types = ["Intercomparison", "Superlative", "Aggregation", "Distribution Compliance", "Correlation Analysis",
             "Variance Analysis"]
    keyword = ["member of"]
    que_txt = ''
    count = 0
    qtype = ''
    qsql = ''
    qq = ''
    total_iterations = len(topics)
    progress_bar = tqdm(total=total_iterations)
    for i, topic in enumerate(topics):

        prope = properties[0]
        topicid = get_wikidata_id(topic)
        # property_id = search_wikidata_property(keyword)[0][0]
        property_id = 'P463'
        print(topicid)
        print(property_id)
        # property_id = "P54"  # Example: Wikidata property for "award received" P166
        # value_id = "Q18748039"  # Example: Wikidata item for "ACM Fellow"
        titles = get_entities_wikipedia_titles(property_id, topicid)
        print(len(titles))
        for title in titles:  # Limiting to first 5 results for demonstration
            first_paragraph = get_wikipedia_first_paragraph(title)
            # print(f"Title: {title}\nFirst Paragraph: {first_paragraph}\n")
            entity_i = {'title': title, 'firstparagraph': first_paragraph}
        topic_i = {'topic': topic, 'edge': keyword[0],
                   'properties': prope, 'entity': entity_i}
        fout_entity.write(json.dumps(topic_i) + '\n')

        table_n = topic.replace(' ', '')
        gen_q_prompt = '''Questions types are {},  
        topic is {}, properties is {}, 
        using the type, topics , properties to ask 30 questions. The questions concern about only the listed properties. 
        Just output questions type， questions and SQL for each question in a format'Questions type:...\n Questions:...\n SQL:...\n', 
        For SQL, the table name is {}, the columns are the properties without ' '. no other word and symbol. remove the Serial number.'''.format(
            types, topic, prope, table_n)
        answer = GPT(gen_q_prompt, "GPT-4")
        answer = answer.replace('\n\n', '\n')
        for line in answer.split('\n'):
            # print(line)
            if "Questions type" in line:
                qtype = line.split(':')[1]
            if "Questions" in line:
                qq = line.split(':')[1]
            if "SQL" in line:
                qsql = line.split(':')[1]
                ques_i = {'qid': count, 'topic': topic, 'edge': keyword,
                          'properties': prope, 'type': qtype, 'question': qq, 'sql': qsql, 'answer': 'NOT YET'}
                count += 1
                fout_queries.write(json.dumps(ques_i) + '\n')
        que_txt = que_txt + answer

        progress_bar.update(1)

    progress_bar.close()
    fout_queries.close()
    fout_entity.close()
    # print(answer)

    # for que in ques:

    # file_path = "Stran/queries.txt"

    # # Open the file in write mode and save the content
    # with open(file_path, "w") as file:
    #     file.write(que_txt)


def sqltool(sql_query):
    # sql_table(file_path, header)
    conn = sqlite3.connect('document_store.db')
    cursor = conn.cursor()
    columns = []
    # sql_query = "SELECT * FROM your_table_name;"
    try:

        cursor.execute("SELECT name FROM IEEEFellow;")
        rows = cursor.fetchall()

        # Print the list of tables
        print("Tables in the database:")
        for table in rows:
            print(table[0])

        cursor.execute(sql_query)
        rows = cursor.fetchall()
        # print('answer is:', rows)
        if cursor.description != None:
            columns = [description[0] for description in cursor.description]
        conn.commit()
        conn.close()

        print(columns)

        return columns, rows

    except sqlite3.Error as e:
        print("An error occurred:", e)
        return []
    # with open('output.csv', 'w', newline='', encoding='utf-8') as csvfile:
    #     writer = csv.writer(csvfile)
    #     writer.writerow(columns)
    #     writer.writerows(rows)
    #
    # table_name = 'faculty'
    # cursor.execute(f'DROP TABLE IF EXISTS {table_name}')


def csv_sqltool(table, sql_query):
    try:
        csv_file_path = "outputcsv/{}.csv".format(table)
        df = pd.read_csv(csv_file_path)

        # Create a connection to a new in-memory SQLite database
        conn = sqlite3.connect(':memory:')

        # Copy the DataFrame data into a new SQLite table
        df.to_sql(table, conn, if_exists='replace', index=False)

        # Execute the SQL query
        result_df = pd.read_sql_query(sql_query, conn)

        # Print the result DataFrame
        # print('reults = ', result_df)
        columns = ', '.join(result_df.columns)
        rows = []
        for row in result_df.itertuples(index=False, name=None):
            # print('row = ',row)
            rows.append(row)
        # print(rows)
        return columns, rows
    except pandas.errors.DatabaseError as e:
        print("数据库操作出错:", e)
        print("An error occurred:", e)
        return 'Wrong'


def gen_answer(path):
    idx = 0
    fout_qa = open(os.path.join(path, 'multi_entities_QA.jsonl'), 'a')
    with open(os.path.join(path, 'multi_entities_query.jsonl'), 'r') as file:
        # Read each line in the file
        for line in file:
            # Convert the JSON string to a Python dictionary
            data = json.loads(line)
            sqll = data['sql'].split(';')[0]
            table_n = data['topic'].replace(' ', '').replace('-', '')

            # print(ques)
            # sql_query = "SELECT * FROM {};".format(table_n)
            answe = csv_sqltool(table_n, sqll)
            # print(sqltool(sql_query))
            print(answe)
            # if result_df != '[]':
            ques_i = {'qid': idx, 'topic': data['topic'], 'edge': data['edge'],
                      'properties': data['properties'], 'type': data['type'],
                      'question': data['question'], 'sql': data['sql'], 'answer': answe}
            idx += 1
            fout_qa.write(json.dumps(ques_i) + '\n')

    fout_qa.close()


def GPT(prompt: str, model: str, **kwargs):
    import requests
    import json
    url = "your API url"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Your key"
    }
    data = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    res = json.loads(response.text)

    if res["choices"][0]["message"]["content"] is not None:
        return res["choices"][0]["message"]["content"]
    else:
        return 'None'


def main():
    # Example usage
    # topic = "Nobel Prize in Chemistry"  # ACM fellow, city, President of the United States, Chemical Elements, Summer Olympic Games,  Nobel Prize in Chemistry
    # keyword = "award received"  # award received, instance of, position held, instance of, instance of,award received
    # propertys = ['categories', 'year of award', 'country of origin', 'field of contribution']
    #
    # topicid = get_wikidata_id(topic)
    # property_id = search_wikidata_property(keyword)[0][0]
    # print(topicid)
    # print(property_id)
    # # property_id = "P54"  # Example: Wikidata property for "award received" P166
    # # value_id = "Q18748039"  # Example: Wikidata item for "ACM Fellow"
    # titles = get_entities_wikipedia_titles(property_id, topicid)
    # print(len(titles))
    # for title in titles:  # Limiting to first 5 results for demonstration
    #     first_paragraph = get_wikipedia_first_paragraph(title)
    #     print(f"Title: {title}\nFirst Paragraph: {first_paragraph}\n")
    path = 'benchmarks_1'
    # Transformation(path)
    gen_answer(path)


if __name__ == '__main__':
    main()
