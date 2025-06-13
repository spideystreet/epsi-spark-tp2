import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf, unix_timestamp, to_timestamp, datediff, round, lower, concat_ws}

object SimpleApp {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder
      .appName("TP Spark - Partie 3")
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

    // --- 3. Feature Engineering ---
    println("\n--- Début du Feature Engineering ---")

    // 3.1. Calcul de la durée de la campagne en jours
    println("3.1. Création de la colonne 'days_campaign'...")
    val dfWithDays = dfFiltered.withColumn("days_campaign", 
      datediff(to_timestamp(col("deadline")), to_timestamp(col("launched_at")))
    )

    // 3.2. Calcul du temps de préparation en heures
    println("3.2. Création de la colonne 'hours_prepa'...")
    val dfWithHours = dfWithDays.withColumn("hours_prepa", 
      round((unix_timestamp(col("launched_at")) - unix_timestamp(col("created_at"))) / 3600, 3)
    )

    // 3.3. Suppression des colonnes de date originales
    println("3.3. Suppression des colonnes de date originales...")
    val dfDatesCleaned = dfWithHours.drop("launched_at", "created_at", "deadline")
    
    // 3.4. Création de la colonne 'text'
    println("3.4. Création de la colonne 'text' par concaténation...")
    val dfWithText = dfDatesCleaned.withColumn("text", 
      concat_ws(" ", 
        lower(col("name")), 
        lower(col("desc")), 
        lower(col("keywords"))
      )
    )

    println("\nExtrait du DataFrame après Feature Engineering (focus sur les nouvelles colonnes) :")
    dfWithText.select("days_campaign", "hours_prepa", "text").show(5, false)

    println("\nSchéma final après Feature Engineering :")
    dfWithText.printSchema()

    // On arrête la session Spark
    spark.stop()
    println("\n👋 Fin de la Partie 3. Session Spark arrêtée. 👋")
  }
}