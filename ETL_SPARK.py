#Partie1

import urllib.request
def download_file(filename):
    url=f"https://assets-datascientest.s3.eu-west-1.amazonaws.com/{filename}.csv"
    urllib.request.urlretrieve(url,f"{filename}.csv")
    print(f"Fichier {filename} téléchargé avec succès!")
download_file("gps_user")
download_file("gps_app")

#Partie2

from pyspark.sql import *
from pyspark.sql.functions import mean
spark=SparkSession\
      .builder\
      .appName("Exam de Spark1")\
      .config("spark.jars", "/home/ubuntu/EXAM_SPARK/mysql-connector-j-8.3.0.jar")\
      .master("local[*]")\
      .getOrCreate()

raw_app = spark.read.option("header", True)\
                    .option("inferSchema", True)\
                    .option("escape", "\"")\
                    .csv("gps_app.csv")

raw_user = spark.read.option("header", True)\
                     .option("inferSchema", True)\
                     .option("escape", "\"")\
                     .csv("gps_user.csv")
raw_app=raw_app.withColumnRenamed("App","APP")\
               .withColumnRenamed("Category","CATEGORY")\
               .withColumnRenamed("Rating","RATING")\
               .withColumnRenamed("Reviews","REVIEWS")\
               .withColumnRenamed("Size","SIZE")\
               .withColumnRenamed("Installs","INSTALLS").withColumnRenamed("Android Ver","ANDROID_VER").withColumnRenamed("Content Rating","CONTENT_RATING")\
               .withColumnRenamed("Genres","GENRES")\
               .withColumnRenamed("Last Updated","LAST_UPDATED")\
               .withColumnRenamed("Current Ver","CURRENT_VER")\
               .withColumnRenamed("Android Ver","ANDROID_VER")\
               .withColumnRenamed("Type","TYPE")\
               .withColumnRenamed("Price","PRICE")

raw_user=raw_user.withColumnRenamed("App","APP")\
                 .withColumnRenamed("Translated_Review","TRANSLATED_REVIEW")\
                 .withColumnRenamed("Sentiment","SENTIMENT")\
                 .withColumnRenamed("Sentiment_Polarity","SENTIMENT_POLARITY")\
                 .withColumnRenamed("Sentiment_Subjectivity","SENTIMENT_SUBJECTIVITY") 




#Pour l'affichage du nombres de null et NaN pour chaque colonne dans une base de données

import numpy as np
from pyspark.sql.functions import isnan, isnull,col
from pyspark.sql.types import BooleanType
def getMissingValues(dataframe):
  count = dataframe.count()
  columns = dataframe.columns
  nan_count = []
  # we can't check for nan in a boolean type column
  for column in columns:
    if dataframe.schema[column].dataType == BooleanType():
      nan_count.append(0)
    else:
      nan_count.append(dataframe.where(isnan(col(column))).count())
  null_count = [dataframe.where(isnull(col(column))).count() for column in columns]
  return([count, columns, nan_count, null_count])
def missingTable(stats):
  count, columns, nan_count, null_count = stats
  count = str(count)
  nan_count = [str(element) for element in nan_count]
  null_count = [str(element) for element in null_count]
  max_init = np.max([len(str(count)), 10])
  line1 = "+" + max_init*"-" + "+"
  line2 = "|" + (max_init-len(count))*" " + count + "|"
  line3 = "|" + (max_init-9)*" " + "nan count|"
  line4 = "|" + (max_init-10)*" " + "null count|"
  for i in range(len(columns)):
    max_column = np.max([len(columns[i]),\
                        len(nan_count[i]),\
                        len(null_count[i])])
    line1 += max_column*"-" + "+"
    line2 += (max_column - len(columns[i]))*" " + columns[i] + "|"
    line3 += (max_column - len(nan_count[i]))*" " + nan_count[i] + "|"
    line4 += (max_column - len(null_count[i]))*" " + null_count[i] + "|"
  lines = f"{line1}\n{line2}\n{line1}\n{line3}\n{line4}\n{line1}"
  print(lines)



    
#Partie3


#1

from pyspark.sql.functions import *
#missingTable(getMissingValues(raw_app))
#raw_app.filter(~isnan("RATING")).describe(["RATING"]).show() 
#on remarque que l'ecart-type est bas donc les valeurs ne sont pas dispersée, il faudrait donc par toute logique utilisée la moyenne plutôt que la médiane
rating_average=raw_app.filter(~isnan("RATING")).select(avg("RATING")).first()[0]
raw_app1=raw_app.fillna({"RATING":rating_average})
raw_app1.describe(["RATING"]).show()
missingTable(getMissingValues(raw_app1))



#2

raw_app1.groupBy("TYPE").count().show()
#On remarque que plus de 90% des valeurs sont égale à "Free" donc ce serait la valeur la plus logique avec laquelle la changer
raw_app2 = raw_app1.withColumn("TYPE", when(isnan(col("TYPE")), None).otherwise(col("TYPE")))
raw_app2=raw_app2.fillna({"TYPE":"Free"})
missingTable(getMissingValues(raw_app2))


#3

raw_app_valeur_unique=raw_app2.groupBy("TYPE").count().filter(col("count")==1).show()
#On remarque alors qu'il n'y a alors que la valeur 0 qui n'a pas de duplicat



#4

#Afin de transormer les NaN en null afin de ne pas multiplier les opérations par la suite
raw_app2 = raw_app2.withColumn("CURRENT_VER", when(isnan(col("CURRENT_VER")), None).otherwise(col("CURRENT_VER")))
raw_app2 = raw_app2.withColumn("ANDROID_VER", when(isnan(col("ANDROID_VER")), None).otherwise(col("ANDROID_VER")))

#On extrait à l'aide d'un groupby, l'apparition la plus fréquente dans CURRENT_VER et ANDROID_VER
modalite_current_ver = raw_app2.groupBy("CURRENT_VER") \
    .count() \
    .orderBy(desc("count")) \
    .first()[0]

modalite_android_ver = raw_app2.groupBy("ANDROID_VER") \
    .count() \
    .orderBy(desc("count")) \
    .first()[0]

#On remplace alors les valeurs manquantes dans CURRENT_VER et ANDROID_VER par leur valeurs respectifs la plus fréquente 
raw_app3=raw_app2.fillna({"CURRENT_VER":modalite_current_ver})
raw_app4=raw_app3.fillna({"ANDROID_VER":modalite_android_ver})




#5

#Nous montre pour chaque colonnes les valeurs manquantes (Spark fait une distinction entre nan et null)
missingTable(getMissingValues(raw_app4))
#On remarque il manque une valeur manquante dans CONTENT_RATING, on fait comme precedemment, on la remplace par sa modalité
modalite_content_rating = raw_app2.groupBy("CONTENT_RATING") \
    .count() \
    .orderBy(desc("count")) \
    .first()[0]
raw_app5=raw_app4.fillna({"CONTENT_RATING":modalite_content_rating})
missingTable(getMissingValues(raw_app5))
#On voit alors qu'il ne reste plus aucune valeur manquante




#Partie4


#1

missingTable(getMissingValues(raw_user))
#On ne prend que les lignes qui contient Nan pour la colonne "SENTIMENT"
valeur_null=raw_user.filter(isnan(col("SENTIMENT")))
#On remarque par la commande suivante que les lignes qui contient NaN pour la colonne "SENTIMENT" contient aussi NaN pour les autres colonnes.
missingTable(getMissingValues(valeur_null))


#2

raw_user_clean=raw_user.filter(~isnan(col("SENTIMENT")))


#3 

raw_user_clean=raw_user.filter(~isnan(col("SENTIMENT")))
missingTable(getMissingValues(raw_user_clean))
#On voit qu'il ne reste plus que les null, on va donc les remplacer par la modalité de "TRANSLATED_REVIEW"
raw_user_withoutnull=raw_user_clean.filter(~isnull("TRANSLATED_REVIEW"))
modalite_translated_review = raw_user_withoutnull.groupBy("TRANSLATED_REVIEW") \
    .count() \
    .orderBy(desc("count")) \
    .first()[0] 
raw_user1=raw_user_clean.fillna({"TRANSLATED_REVIEW":modalite_translated_review})
print("Maintenant le tableau est entièrement nettoyée des valeurs manquante:\n")
missingTable(getMissingValues(raw_user1))




#Partie5



#1

from pyspark.sql.types import DoubleType, FloatType

raw_user1 = raw_user1.withColumn("SENTIMENT_POLARITY", raw_user1["SENTIMENT_POLARITY"].cast(DoubleType()))
raw_user1 = raw_user1.withColumn("SENTIMENT_SUBJECTIVITY", raw_user1["SENTIMENT_SUBJECTIVITY"].cast(DoubleType()))

raw_user1.printSchema()  # Vérifier si les types sont bien modifiés
missingTable(getMissingValues(raw_user1))
#De ce qu'on voit, comme il n'y a pas de valeur manquante après la conversion, cela signifie qu'il reste des valeurs non numériques dans les colonnes sentiment_polarity et sentiment_subjectivity



#2

raw_user1.printSchema()
raw_user1 = raw_user1.withColumn("SENTIMENT_POLARITY", raw_user1["SENTIMENT_POLARITY"].cast(FloatType()))
raw_user1 = raw_user1.withColumn("SENTIMENT_SUBJECTIVITY", raw_user1["SENTIMENT_SUBJECTIVITY"].cast(FloatType()))



#3


from pyspark.sql.functions import regexp_replace, col

raw_user1 = raw_user1.withColumn("TRANSLATED_REVIEW", regexp_replace(col("TRANSLATED_REVIEW"),"[^a-zA-Z0-9]", " "))
#[a-zA-Z0-9] qui correspond à toutes les caractère alphanumériques et donc ajout de ^pour obtenir la négation de cela donc tout les caractères spéciaux, c'est-à-dire qui ne sont pas alha-numérique
raw_user1 = raw_user1.withColumn("TRANSLATED_REVIEW", regexp_replace(col("TRANSLATED_REVIEW"), "  ", " "))


#4


from pyspark.sql.functions import regexp_replace, col, when
raw_user1 = raw_user1.withColumn("TRANSLATED_REVIEW", lower(col("TRANSLATED_REVIEW")))

replacements = [
    (r"(?i).*fun.*", "fun"),
    (r"(?i).*not bad.*", "ok"),
    (r"(?i).*don't like.*", "bad"),
    (r"(?i).*doesnt like.*", "bad"),
    (r"(?i).*no like.*", "bad"),
    (r"(?i).*like.*", "like"),
    (r"(?i).*useful.*", "useful"),
    (r"(?i).*don't love.*", "bad"),
    (r"(?i).*doesnt love.*", "bad"),
    (r"(?i).*no love.*", "bad"),
    (r"(?i).*love.*", "love"),
    (r"(?i).*good.*", "good"),
    (r"(?i).*nice.*", "nice"),
    (r"(?i).*great.*", "great"),
    (r"(?i).*uncool.*", "bad"),
    (r"(?i).*not cool.*", "cool"),
    (r"(?i).*no cool.*", "cool"),
    (r"(?i).*cool.*", "cool"),
    (r"(?i).*super.*", "super"),
    (r"(?i).*ok.*", "ok"),
    (r"(?i).*hate.*", "bad"),
    (r"(?i).*thank.*", "thank"),
    (r"(?i).*awesome.*", "awesome"),
    (r"(?i).*worst.*", "bad"),
    (r"(?i).*best.*", "best"),
    (r"(?i).*excellent.*", "amazing"),
    (r"(?i).*not bad.*", "ok"),
    (r"(?i).*bad.*", "bad"),
    (r"(?i).*very.*", "super"),
    (r"(?i).*kool.*", "cool"),
    (r"(?i).*perfect.*", "perfect"),
    (r"(?i).*not easy.*", "not easy"),
    (r"(?i).*easy.*", "easy"),
    (r"(?i).*not fine.*", "bad"),
    (r"(?i).*fine.*", "fine"),
    (r"(?i).*waste.*", "waste"),
    (r"(?i).*not amazing.*", "amazing"),
    (r"(?i).*amazing.*", "amazing"),
    (r"(?i).*suck.*", "bad"),
    (r"(?i).*garbage.*", "bad"),
    (r"(?i).*idk.*", "idk"),
    (r"(?i).*wonderful.*", "amazing"),
    (r"(?i).*fantastic.*", "amazing"),
    (r"(?i).*god.*", "amazing"),
    (r"(?i).*addictive.*", "amazing"),
    (r"(?i).*nyc.*", "cool"),
    (r"(?i).*nice.*", "cool"),
    (r"(?i).*nc.*", "cool"),
    (r"(?i).*thx.*", "amazing"),
    (r"(?i).*tnx.*", "amazing"),
    (r"(?i).*dont work.*", "bad"),
    (r"(?i).*doesn't work.*", "bad"),
    (r"(?i).*no work.*", "no work"),
    (r"(?i).*w[o0]?rk\s*w[e3]?ll.*", "super"),
    (r"(?i).*work wll.*", "super"),
    (r"(?i).*work. *", "ok"),
    (r"(?i).*work.*", "ok"),
    (r"(?i).*\\badd\\b.*", "add"),
    (r"(?i).*\\bad\\b.*", "add"),
    (r"(?i).*\\bads\\b.*", "add"),
    (r"(?i).*\\badds\\b.*", "add"),
    (r"(?i).*\\bok.*", "ok"),
    (r"(?i).*wow.*", "amazing"),
    (r"(?i).*ads.*", "bof"),
    (r"(?i).*brilliant.*", "amazing"),
    (r"(?i).*boring.*", "bad"),
    (r"(?i).*great.*", "great"),
    (r"(?i).*limited use.*", "limited use"),
    (r"(?i).*highly recommended.*", "highly recommended"),
    (r"(?i).*useful.*", "usefull"),
    (r"(?i).*really nice.*", "really nice"),
    (r"(?i).*well designed.*", "well designed"),
    (r"(?i).*limited use.*", "limited use"),
    (r"(?i).*dissapointing.*", "dissapointing"),
    (r"(?i).*great.*", "great"),
    (r"(?i).*no perfect.*", "ok"),
    (r"(?i).*not perfect.*", "ok"),
    (r"(?i).*perfect.*", "perfect"),
    (r"(?i).*\\bok.*", "ok"),
    (r"(?i).*useless.*", "useless"),
    (r"(?i).*avoid it.*", "avoid it"),
    (r"(?i).*\\brecommend.*", "recommend"),
    (r"(?i).*outstanding.*", "outstanding"),
    (r"(?i).*no efficient.*", "no efficient"),
    (r"(?i).*not efficient.*", "no efficient"),
    (r"(?i).*efficient.*", "efficient"),
    (r"(?i).*nice.*", "nice"),
    (r"(?i).*fast.*", "fast"),
    (r"(?i).*well designed.*", "well designed"),
    (r"(?i).*wel designed.*", "well designed"),
    (r"(?i).*wll designed.*", "well designed"),
    (r"(?i).*well designd.*", "well designed"),
    (r"(?i).*impressive.*", "impressive"),
    (r"(?i).*\\bbest\\b.*", "best"),
    (r"(?i).*fun.*", "fun"),
    (r"(?i).*helpful.*", "helpfull"),
    (r"(?i).*helpfull.*", "helpfull"),
    (r"(?i).*ok.*", "ok"),
    (r"(?i).*fantastic.*", "fantastic"),
    (r"(?i).*good.*", "good")
]

# Appliquer toutes les transformations en une seule boucle
for pattern, replacement in replacements:
    raw_user1 = raw_user1.withColumn(
        "TRANSLATED_REVIEW", regexp_replace(col("TRANSLATED_REVIEW"), pattern, replacement)
    )




#5 


#from pyspark.sql.functions import col,length, desc
#raw_filter=raw_user1.filter((length(col("TRANSLATED_REVIEW"))<=10)&(length(col("TRANSLATED_REVIEW"))>1))
#raw_filter.groupBy("TRANSLATED_REVIEW")\
#           .count()\
#           .orderBy(desc(length(col("TRANSLATED_REVIEW")))).show(500)



#6-7


#rdd=raw_user1.rdd
#valeur_exclue=["bad","bof","waste","useless","rubbish","eight"]
#rdd_clean=rdd.filter(lambda row:((len(row["TRANSLATED_REVIEW"])>=3) and (row["TRANSLATED_REVIEW"] not in valeur_exclue)))
#first20_positive_values=rdd_clean.map(lambda row:(row["TRANSLATED_REVIEW"],1))\
#                           .reduceByKey(lambda a,b:a+b)\
#                           .sortBy(lambda x:x[1],ascending=False)\
#                           .take(20)

#print(first20_positive_values)


#partie6


from pyspark.sql.functions import col,regexp_replace,to_date
from pyspark.sql.types import IntegerType,DoubleType


#1


raw_app = raw_app5
raw_app.describe("REVIEWS").show()
#Il est donc impossible qu'il est des valeurs de types string dans cet colonnes sinon il n'aurait pas put calculer la moyenne par exemple
raw_app = raw_app.withColumn("REVIEWS",raw_app["REVIEWS"].cast(IntegerType()))



#2

raw_app = raw_app.withColumn("INSTALLS",regexp_replace(col("INSTALLS"),"[^0-9-]",""))
raw_app = raw_app.withColumn("INSTALLS",col("INSTALLS").cast(IntegerType()))



#3

raw_app = raw_app.withColumn("PRICE",regexp_replace(col("PRICE"),"[^0-9-.]",""))
raw_app = raw_app.withColumn("PRICE",raw_app["PRICE"].cast(DoubleType()))


#4

raw_app = raw_app.withColumn("LAST_UPDATED",to_date(raw_app["LAST_UPDATED"],"MMMM d, yyyy"))




##
raw_app.write \
       .mode('overwrite') \
       .format("jdbc") \
       .option("url", "jdbc:mysql://localhost:3306/database") \
       .option("dbtable", "gps_app") \
       .option("user", "user") \
       .option("password", "password") \
       .save()

raw_user.write \
        .mode('overwrite') \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/database") \
        .option("dbtable", "gps_user") \
        .option("user", "user") \
        .option("password", "password") \
        .save()

spark.stop()
