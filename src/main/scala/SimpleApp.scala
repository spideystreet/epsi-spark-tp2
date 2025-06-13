import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col // On importe la fonction 'col'

object SimpleApp {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder
      .appName("TP Spark - Partie 1")
      .config("spark.hadoop.user.name", "hadoop")
      .master("local[*]") // Utilisation du master local
      .getOrCreate()

    // --- 1. Chargement des données ---
    println("1. Chargement du fichier train_clean.csv...")
    val df = spark.read
      .option("header", "true") // Utilise la première ligne comme en-tête
      .option("inferSchema", "true") // Spark essaie de deviner les types
      .csv("data/train_clean.csv")

    // --- 2. Affichage des dimensions ---
    val rowCount = df.count()
    val colCount = df.columns.length
    println(s"2. Le DataFrame a $rowCount lignes et $colCount colonnes.")

    // --- 3. Affichage d'un extrait ---
    println("3. Extrait du DataFrame :")
    df.show(5, false) // 5 lignes, sans tronquer les colonnes

    // --- 4. Affichage du schéma ---
    println("4. Schéma du DataFrame :")
    df.printSchema()

    // --- 5. Assignation des types Entier ---
    println("5. Conversion des colonnes en Entier...")
    val dfCasted = df
      .withColumn("goal", col("goal").cast("integer"))
      .withColumn("final_status", col("final_status").cast("integer"))
    
    println("Schéma après conversion :")
    dfCasted.printSchema()


    // On arrête la session Spark
    spark.stop()
    println("\n👋 Fin de la Partie 1. Session Spark arrêtée. 👋")
  }
}