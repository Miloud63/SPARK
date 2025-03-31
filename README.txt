Nous procédons à la création d’une pipeline de données ETL (Extraction, Transformation,Chargement):
Extraction : Spark lit les données directement depuis des URLs distantes (fichiers CSV disponibles en ligne). Il télécharge et charge ces fichiers dans des DataFrames pour les traiter.
Transformation : Le script ETL_SPARK.py applique des transformations via PySpark : nettoyage, jointures, renommage de colonnes, formatage de types, filtrage, etc.
Chargement : Les données transformées sont enregistrées dans une base MySQL (hébergée via Docker), dans de nouvelles tables.

Pour le démarrage d’une base de données MySQL, exécution du script suivant: 
	docker run --name my-mysql -p 3306:3306 \
           -e MYSQL_ROOT_PASSWORD=my-secret-pw \
           -e MYSQL_USER=user \
           -e MYSQL_PASSWORD=password \
           -e MYSQL_DATABASE=database \
           -d mysql:latest

Afin d’exécuter le script afin de faire du calcul distribué et procéder aux transformations de manière plus efficace (et aussi par la nature de notre script), nous exécutons:
	spark-submit \
  --jars ./jars/mysql-connector-j-8.3.0.jar \
  ETL_SPARK.py
