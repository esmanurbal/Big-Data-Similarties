import json
import pyodbc
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer

from sklearn.metrics.pairwise import cosine_similarity

def calculate_similarity(server_name, database_name):
    
    conn_string = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={server_name};"
        f"DATABASE={database_name};"
    )

    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()

    query = "SELECT TOP(50) [strContent],[strNewsTitle], [strUrl]  FROM [LocalNews].[dbo].[20220217_LocalNews_Found_Last_1_Month] WHERE [strUrl] LIKE '%twitter%'"
    cursor.execute(query)

    results = cursor.fetchall()
    stopwords = ['acaba','Haber', 'ama','rt', '@','gazete','gazetesi','son','dakika','gelişmeleri','son dakika gelişmeleri', 'haber', 'aslında', 'az', 'bazı', 'belki', 'biri', 'birkaç', 'birşey', 'biz', 'bu', 'çok', 'çünkü', 'da', 'daha', 'de', 'defa', 'diye', 'eğer', 'en', 'gibi', 'hem', 'hep', 'hepsi', 'her', 'hiç', 'için', 'ile', 'ise', 'kez', 'ki', 'kim', 'mı', 'mu', 'mü', 'nasıl', 'ne', 'neden', 'nerde', 'nerede', 'nereye', 'niçin', 'niye', 'o', 'sanki', 'şey', 'siz', 'şu', 'tüm', 've', 'veya', 'ya', 'yani']


    # Create a list of tuples, where each tuple contains the strNewsTitle and strContent for a row
    items = [(result[0], result[1], result[2]) for result in results if result[0] is not None and result[1] is not None and result[2] is not None]
    news_url = [(result[2]) for result in results]


    # Create a CountVectorizer to calculate the cosine similarity for strNewsTitle
    vectorizer_title = TfidfVectorizer(stop_words=stopwords)

    # Transform strNewsTitle into a matrix of token counts
    title_matrix = vectorizer_title.fit_transform([item[1] for item in items])


    similarity_title = cosine_similarity(title_matrix)

    # Create a CountVectorizer to calculate the cosine similarity for strContent
    vectorizer_content = TfidfVectorizer(stop_words=stopwords)

    # Transform strContent into a matrix of token counts
    content_matrix = vectorizer_content.fit_transform([item[0] for item in items])

    similarity_content = cosine_similarity(content_matrix)

    item_map = {}

    for i, item in enumerate(items):
    # If the item is not already in the item map, add it with a new id number
        if item not in item_map:
            item_map[item] = len(item_map)

        # Iterate through the other items
        for j, other_item in enumerate(items):
            if ('twitter' in news_url[i]):
                point_content = 0.6
            else:
                point_content = 0.3
            # If the items are similar and the other item is not already in the item map, add it with the same id number as the current item
            if i != j and ((similarity_content[i][j] >= point_content) or ((similarity_content[i][j] >= point_content and similarity_title[i][j] > 0.4))):
                if other_item not in item_map:
                    item_map[other_item] = item_map[item]

   
    items = []

    # Iterate through the items in the item map
    for item, item_id in item_map.items():
        item_dict = {
            "url": item[2],
            "title": item[1],
            "content": item[0],
            "id": item_id,
        }
        # Add the dictionary to the list of items
        items.append(item_dict)
    json_str = json.dumps(items, ensure_ascii=False, indent=4, sort_keys=True)

    with open("items11.json", "w", encoding='utf-8') as f:
        # Write the list of items to the file as JSON
        f.write(json_str)
        

calculate_similarity('.\SQLEXPRESS','LocalNews')