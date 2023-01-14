import json
import os
import pyodbc
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def calculate_similarity(server_name, database_name):
    
    min_shortData=260  # minimum lenght of short text
    shortData_Ctreshold=0.26  #threshold of content of short data
    shortData_Ttreshold=0.4  #threshold of title of short data
    shortData_TCtreshold=0.23 #threshold of content of short data depends on title
    
    Ctreshold=0.5  #threshld of content 
    Ttreshold=0.8 #threshold of title 
    TCtreshold=0.3 #threshold of content depends on title


    conn_string = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={server_name};"
        f"DATABASE={database_name};"
    )

    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()

    
    #query = "SELECT [strContent],[strNewsTitle]  FROM [ESSL].[dbo].[20220217_LocalNews_Found_Last_1_Month] WHERE [strContent] LIKE '%Yücel Gedik%' AND [strNewsTitle] LIKE '%Gedik%'"
    #query = "SELECT TOP(50) [strContent],[strNewsTitle],[strUrl]  FROM [ESSL].[dbo].[20220217_LocalNews_Found_Last_1_Month] WHERE [strContent] LIKE '%Yücel Gedik%' AND [strNewsTitle] LIKE '%Gedik%'"
    query="SELECT TOP(50) [strContent],[strNewsTitle],[strUrl] FROM [ESSL].[dbo].[20220217_LocalNews_Found_Last_1_Month]  WHERE [strContent] LIKE '%izmir%'"
    cursor.execute(query)

    results = cursor.fetchall()
   
    stopwords = ['acaba', 'ama', 'aslında', 'az', 'bazı', 'belki', 'biri', 'birkaç', 'birşey', 'biz', 'bu', 'çok', 'çünkü', 'da', 'daha', 'de', 'defa', 'diye', 'eğer', 'en', 'gibi', 'hem', 'hep', 'hepsi', 'her', 'hiç', 'için', 'ile', 'ise', 'kez', 'ki', 'kim', 'mı', 'mu', 'mü', 'nasıl', 'ne', 'neden', 'nerde', 'nerede', 'nereye', 'niçin', 'niye', 'o', 'sanki', 'şey', 'siz', 'şu', 'tüm', 've', 'veya', 'ya', 'yani']
    items = [(result[0],result[1],result[2]) for result in results if result[0] is not None and result[1] is not None and result[2] is not None]
    
    vectorizer_title = TfidfVectorizer(stop_words=stopwords)
    title_matrix = vectorizer_title.fit_transform([item[1] for item in items])

    similarity_title = cosine_similarity(title_matrix)

    vectorizer_content = TfidfVectorizer(stop_words=stopwords)
    content_matrix = vectorizer_content.fit_transform([item[0] for item in items])
    content_similarity = cosine_similarity(content_matrix)
    

    item_map = {}

    for i, item in enumerate(items):
        if item not in item_map:
            item_map[item] = len(item_map)
        for j, other_item in enumerate(items):
            if len(other_item)< min_shortData or len(item)< min_shortData:
                if i != j and ((content_similarity[i][j] >= shortData_Ctreshold) or (similarity_title[i][j] >= shortData_Ttreshold and content_similarity[i][j] >= shortData_TCtreshold)): 
                        item_map[other_item] = item_map[item]
            else: 
                if i != j and ((content_similarity[i][j] >= Ctreshold) or (similarity_title[i][j] >= Ttreshold and content_similarity[i][j] >= TCtreshold)): 
                    if other_item not in item_map:
                        item_map[other_item] = item_map[item]
    
    similarity_data = []

    for i , (item1, id1) in enumerate(item_map.items()):
        index1 = items.index(item1)
        for j , (item2, id2) in enumerate(item_map.items()):

            index2 = items.index(item2)
            if i!=j and id1 == id2:
                data = {
                    "Content1": item1[0],
                    "char1":len(item1[0]),
                    "Content2": item2[0],
                    "char2":len(item2[0]),
                    "id1": id1,
                    "id2": id2,
                    "Ttile1": item1[1],
                    "Title2": item2[1],
                    "url1":item1[2],
                    "url2":item2[2],
                    "title_similarity": similarity_title[index1][index2],
                    "content_similarity": content_similarity[index1][index2]
                }

        similarity_data.append(data)
    for i in similarity_data:
            json_data = json.dumps(i,ensure_ascii=False, indent=4, sort_keys=True)

            if os.path.exists(f"news{i['id1']}.json"):
                with open(f"news{i['id1']}.json", "a", encoding='utf-8') as f:
                    f.write(json_data)
            else:
                with open(f"news{i['id1']}.json", "w", encoding='utf-8') as f:
                    f.write(json_data)
calculate_similarity('DESKTOP-JT60920','ESSL')