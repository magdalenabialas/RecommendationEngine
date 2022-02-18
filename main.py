import numpy as np
import pandas as pd
import mysql.connector
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer

#Połaczenie z baza danych Mysql
cnx = mysql.connector.connect(user='root', password='nimda',
                              host='127.0.0.1',
                              database='moviecatalog_test')
pd.set_option("display.max_rows", None, "display.max_columns", None)
cursor = cnx.cursor()
curso1 = cnx.cursor()


#Dodanie do DataFrame danych z widoków view1 oraz view2. Widoki bazują na tabelach Movie, MoviesActors oraz MoviesGenres
view1 = pd.read_sql("SELECT * FROM movies_actors_gc", cnx)
view2 = pd.read_sql("SELECT * FROM movies_genres_gc", cnx)
merged_views = pd.merge(view1,view2,on='idmovie')
df_movie = pd.DataFrame(merged_views)

users_movies = pd.read_sql("WITH cte AS (SELECT iduser as cid, idmovie, rand() as r FROM movies_users where rate >= 7) select iduser, M1, if(M2=M1,null,M2) as M2 from (SELECT distinct iduser, (select idmovie from cte where cid = iduser order by r limit 1) as M1, (select idmovie from cte where cid = iduser order by r desc limit 1) as M2 FROM movies_users where rate >= 7) as s", cnx)
df_usersmovies = pd.DataFrame(users_movies)
print(df_usersmovies )

df_usersmovies = df_usersmovies .dropna()
print(df_usersmovies)


df_movie_matrix = pd.DataFrame(merged_views)

#Dodanie dodatkowej tabeli important _features zawierającej wszystkie atrybuty, ktore beda brane pod uwage
#przy wyliczaniu podobieństwa jak title, director, actors, genres, oddzielone separatorem ' | '
def get_important_features(data):
    important_features = []
    for i in range(0, data.shape[0]):
        important_features.append(data['title'][i] + ' | ' + str(data['director'][i]) + ' | ' + str(data['actors'][i]) + ' | ' + str(data['genres'][i]))
    return important_features

df_movie_matrix['important_features'] = get_important_features(df_movie)

#dodanie dodatkowej columny index, bedacej indexem w widoku DataFrame. Bedzoe to pomocne przy sortowaniu wynikow dla macierzy pobienstw
df_movie_matrix['INDEX'] = np.arange(df_movie_matrix.shape[0])

#wyliczanie podobienstwa pomiedzy filmami. Przeksztalcenie danych z kolumny important_features w macierz podobienstw i wyliczenie
#podobienstwa Cosinus
cm = CountVectorizer().fit_transform(df_movie_matrix['important_features'])
cs = cosine_similarity(cm)
print(cs)



for i,row in df_usersmovies.iterrows():
    print("\n\n\n Wyliecznia dla M1")
    idmovie_temp = row['M1']
    iduser_temp = row['iduser']
    data_index =df_movie_matrix[df_movie.idmovie == idmovie_temp]['INDEX'].values[0]
    scores = list(enumerate(cs[data_index]))
    sorted_scores = sorted(scores, key = lambda x:x[1], reverse = True)
    sorted_scores = sorted_scores[1:]
    #wypisanie 5 najlepszych wynikow dla danego tytulu kazdy element listy ma forme INDEX, STOPIEN PODOBIENSTWA
    # trzeba wypisac elementy tylko po indexie najlepiej w petli i przypisac do tymczasowej zmiennej, po czym znalezc film z takim samym indexem (przechowywanym w tymczasowej zmiennej)
    print(sorted_scores[:5])
    top_5 = sorted_scores[:5]
    print("\n\n\n USERS: " + str(i))
    k = 0

    # M1======================================================
    for j in range (0,5):
        details_for_index = df_movie_matrix.loc[df_movie_matrix['INDEX'] == top_5[j][0]]
        index_temp = top_5[j][0]
        idmovie = df_movie.loc[index_temp]['idmovie']
        query2 = f'SELECT Count(*) FROM movies_users WHERE iduser = {iduser_temp} AND idmovie = {idmovie}'
        cursor.execute(query2)
        count = cursor.fetchone()[0]
        if count == 0:
            print("\n\n\n details: " + str(j))
            print(details_for_index)

            update = ''
            if k == 0:
                #wrzucamy movie1reko
                update= f'update users set reccomended_movie1 = {idmovie} WHERE id = {iduser_temp}'
                cursor.execute(update)
                k = k + 1
            else:
                break

        else:
            print("\n\n\n user ocenil film zarekomendowany " + str(j))
            print(top_5)



    #M2======================================================
    print("\n\n\n Wyliecznia dla M2")
    idmovie_temp = row['M2']
    iduser_temp = row['iduser']
    data_index = df_movie_matrix[df_movie.idmovie == idmovie_temp]['INDEX'].values[0]
    scores = list(enumerate(cs[data_index]))
    sorted_scores = sorted(scores, key=lambda x: x[1], reverse=True)
    sorted_scores = sorted_scores[1:]
    # wypisanie 5 najlepszych wynikow dla danego tytulu kazdy element listy ma forme INDEX, STOPIEN PODOBIENSTWA
    # trzeba wypisac elementy tylko po indexie najlepiej w petli i przypisac do tymczasowej zmiennej, po czym znalezc film z takim samym indexem (przechowywanym w tymczasowej zmiennej)
    print(sorted_scores[:5])
    top_5 = sorted_scores[:5]
    print("\n\n\n USERS: " + str(i))
    k = 0

    for j in range(0, 5):
        details_for_index = df_movie_matrix.loc[df_movie_matrix['INDEX'] == top_5[j][0]]
        index_temp = top_5[j][0]
        idmovie = df_movie.loc[index_temp]['idmovie']
        query2 = f'SELECT Count(*) FROM movies_users WHERE iduser = {iduser_temp} AND idmovie = {idmovie}'
        cursor.execute(query2)
        count = cursor.fetchone()[0]
        print(count)
        if count == 0:
            print("\n\n\n details: " + str(j))
            print(details_for_index)

            update = ''
            if k == 0:
                # wrzucamy movie1reko
                update = f'update users set reccomended_movie2 = {idmovie} WHERE id = {iduser_temp}'
                cursor.execute(update)
                k = k + 1
            else:
                break

        else:
            print("\n\n\n user ocenil film zarekomendowany " + str(j))
            print(top_5)

cnx.close()