import numpy as np
import pandas as pd
import mysql.connector
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer

cnx = mysql.connector.connect(user='root', password='nimda',
                              host='127.0.0.1',
                              database='moviecatalog_test')


pd.set_option("display.max_rows", None, "display.max_columns", None)

view1 = pd.read_sql("SELECT * FROM movie_view8", cnx)

view2 = pd.read_sql("SELECT * FROM movie_view1", cnx)

merged_views = pd.merge(view1,view2,on='idmovie')
df = pd.DataFrame(merged_views)

#print(df.head(3))

#print(df.shape)

columns = ['title', 'director', 'release_date', 'actors', 'genres']

#print(df[columns].head(3))

def get_important_features(data):
    important_features = []
    for i in range(0, data.shape[0]):
        important_features.append(data['title'][i] + ' | ' + str(data['director'][i]) + ' | ' + str(data['actors'][i]) + ' | ' + str(data['genres'][i]))
    return important_features

df['important_features'] = get_important_features(df)


#print(df.head(3))

df['INDEX'] = np.arange(df.shape[0])

#print(df.head(3))

cm = CountVectorizer().fit_transform(df['important_features'])
cs = cosine_similarity(cm)
print(cs)
print(cs.shape)

title_temp= 'Chaco'

#print("LOOOOOOOOOOOOOOOOOOOOOOOL \n\n\n")
#print(df.head(3))
print("LOOOOOOOOOOOOOOOOOOOOOOOL \n\n\n")

data_index = df[df.title == title_temp]['INDEX'].values[0]
scores = list(enumerate(cs[data_index]))
sorted_scores = sorted(scores, key = lambda x:x[1], reverse = True)
sorted_scores = sorted_scores[1:]

print(sorted_scores)
lol = df.loc[df['INDEX'] == 95]
print(lol)

cnx.close()