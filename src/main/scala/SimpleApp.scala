import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf} // Ajout de 'udf'

object SimpleApp {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder
      .appName("TP Spark - Partie 2")
      .config("spark.hadoop.user.name", "hadoop")
      .master("local[*]")
      .getOrCreate()

    // --- 1. Chargement et préparation initiale ---
    println("--- Chargement et préparation initiale ---")
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/train_clean.csv")

    val dfCasted = df
      .withColumn("goal", col("goal").cast("integer"))
      .withColumn("final_status", col("final_status").cast("integer"))
    
    println("Schéma initial après casting :")
    dfCasted.printSchema()

    // --- 2. Cleaning ---
    println("\n--- Début du Cleaning ---")

    // 2.1. Suppression de colonnes
    println("2.1. Suppression des colonnes 'disable_communication', 'backers_count', 'state_changed_at'...")
    val dfDropped = dfCasted.drop("disable_communication", "backers_count", "state_changed_at")
    
    // 2.2. Création des UDFs pour country et currency
    println("2.2. Création et application des UDFs pour country et currency...")

    // Fonction pour nettoyer 'country'
    val cleanCountryUDF = udf((country: String, currency: String) => {
      if (country == "False") currency
      else if (country != null && country.length != 2) null
      else country
    })

    // Fonction pour nettoyer 'currency'
    val cleanCurrencyUDF = udf((currency: String) => {
      if (currency != null && currency.length != 3) null
      else currency
    })

    // Application des UDFs
    val dfCleanedCountry = dfDropped
      .withColumn("country2", cleanCountryUDF(col("country"), col("currency")))
      .withColumn("currency2", cleanCurrencyUDF(col("currency")))

    // 2.3. Filtrage sur 'final_status'
    println("\n2.3. Répartition des classes pour 'final_status' :")
    dfCleanedCountry.groupBy("final_status").count().show()

    println("Filtrage pour ne garder que les status 0 et 1...")
    val dfFiltered = dfCleanedCountry.filter(col("final_status").isin(0, 1))

    println("Répartition après filtrage :")
    dfFiltered.groupBy("final_status").count().show()

    println("\nExtrait du DataFrame après cleaning :")
    dfFiltered.show(5, false)

    println("\nSchéma final après cleaning :")
    dfFiltered.printSchema()

    // On arrête la session Spark
    spark.stop()
    println("\n👋 Fin de la Partie 2. Session Spark arrêtée. 👋")
  }
}