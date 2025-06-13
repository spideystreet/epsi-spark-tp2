#!/bin/zsh

# Ce script compile et exécute l'application Scala directement avec sbt.
# sbt s'occupe de trouver Spark et les dépendances pour nous.
echo "🚀 Lancement de la compilation et de l'exécution avec sbt run..."
sbt run

spark-submit \
  --class SimpleApp \
  --master local[*] \
  target/scala-2.13/app_2.13-1.0.jar