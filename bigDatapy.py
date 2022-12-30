import json
import pyodbc
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import numpy as np
from collections import defaultdict

def calculate_similarity(server_name, database_name):
    # Create a connection string
    conn_string = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={server_name};"
        f"DATABASE={database_name};"
    )

    # Establish a connection to the database
    conn = pyodbc.connect(conn_string)

    # Create a cursor to execute queries
    cursor = conn.cursor()

    # Retrieve the data from the specified column
    query = "SELECT [strContent],[strNewsTitle]  FROM [ESSL].[dbo].[20220217_LocalNews_Found_Last_1_Month] WHERE [strContent] LIKE '%Yücel Gedik%' AND [strNewsTitle] LIKE '%Gedik%'"
    cursor.execute(query)

    # Store the results in a list
    results = cursor.fetchall()
    stopwords = ['acaba', 'ama', 'aslında', 'az', 'bazı', 'belki', 'biri', 'birkaç', 'birşey', 'biz', 'bu', 'çok', 'çünkü', 'da', 'daha', 'de', 'defa', 'diye', 'eğer', 'en', 'gibi', 'hem', 'hep', 'hepsi', 'her', 'hiç', 'için', 'ile', 'ise', 'kez', 'ki', 'kim', 'mı', 'mu', 'mü', 'nasıl', 'ne', 'neden', 'nerde', 'nerede', 'nereye', 'niçin', 'niye', 'o', 'sanki', 'şey', 'siz', 'şu', 'tüm', 've', 'veya', 'ya', 'yani']

    items = [(result[0], result[1]) for result in results if result[0] is not None and result[1] is not None]
    
    vectorizer_title = CountVectorizer(stop_words=stopwords)
    title_matrix = vectorizer_title.fit_transform([item[1] for item in items])

    similarity_title = cosine_similarity(title_matrix)

    vectorizer_content = CountVectorizer(stop_words=stopwords)
    content_matrix = vectorizer_content.fit_transform([item[0] for item in items])
    content_similarity = cosine_similarity(content_matrix)

    item_map = {}

    for i, item in enumerate(items):
        if item not in item_map:
            item_map[item] = len(item_map)
        for j, other_item in enumerate(items):
            # If the items are similar and the other item is not already in the item map, add it with the same id number as the current item
            if i != j and ((content_similarity[i][j] >= 0.5) or (similarity_title[i][j] >= 0.8 and content_similarity[i][j] >= 0.3)): 
                if other_item not in item_map:
                    item_map[other_item] = item_map[item]

    similarity_data = []

   
    for item1, id1 in item_map.items():
       
        index1 = items.index(item1)

        for item2, id2 in item_map.items():
            index2 = items.index(item2)
            if id1 == id2:
               
                data = {
                    "Content1": item1[0],
                    "Content2": item2[0],
                    "id1": id1,
                    "id2": id2,
                    "Ttile1": item1[1],
                    "Title2": item2[1],
                    "title_similarity": similarity_title[index1][index2],
                    "content_similarity": content_similarity[index1][index2]
                }

        similarity_data.append(data)

    json_data = json.dumps(similarity_data,ensure_ascii=False, indent=4, sort_keys=True)
    with open("data.json", "w", encoding='utf-8') as f:
        f.write(json_data)

calculate_similarity('DESKTOP-JT60920','ESSL')

