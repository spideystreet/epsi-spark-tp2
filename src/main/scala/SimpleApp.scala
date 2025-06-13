import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf, unix_timestamp, to_timestamp, datediff, round, lower, concat_ws, expr}
import org.apache.spark.sql.types.IntegerType

object SimpleApp {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder
      .appName("TP Spark - Final")
      .config("spark.hadoop.user.name", "hadoop")
      .master("local[*]")
      .getOrCreate()

    // --- 1. Chargement et préparation initiale ---
    println("--- Chargement et préparation initiale ---")
    val df = spark.read
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv("data/train.csv")

    println(s"1.1. Nombre de lignes: ${df.count()}")
    println(s"1.2. Nombre de colonnes: ${df.columns.length}")
    println("1.3. Extrait du DataFrame brut :")
    df.show(5)

    // 1.4. Conversion de types (Casting)
    // On utilise try_cast pour éviter les erreurs sur les lignes mal formées
    val dfCasted = df
      .withColumn("goal", expr("try_cast(goal as int)"))
      .withColumn("final_status", expr("try_cast(final_status as int)"))
    
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
    val dfWithDays = dfFiltered.withColumn("deadline_ts", to_timestamp(expr("try_cast(deadline as long)")))
      .withColumn("launched_at_ts", to_timestamp(expr("try_cast(launched_at as long)")))
      .withColumn("created_at_ts", to_timestamp(expr("try_cast(created_at as long)")))
      .withColumn("days_campaign", 
        datediff(col("deadline_ts"), col("launched_at_ts"))
      )

    // 3.2. Calcul du temps de préparation en heures
    println("3.2. Création de la colonne 'hours_prepa'...")
    val dfWithHours = dfWithDays.withColumn("hours_prepa", 
      (col("launched_at_ts").cast("long") - col("created_at_ts").cast("long")) / 3600
    )

    // 3.3. Suppression des colonnes de date originales...
    println("3.3. Suppression des colonnes de date originales...")
    val df_dates_cleaned = dfWithHours.drop("deadline", "created_at", "launched_at", "deadline_ts", "launched_at_ts", "created_at_ts")

    // 3.4. Création de la colonne 'text' par concaténation...
    println("3.4. Création de la colonne 'text' par concaténation...")
    val df_with_text = df_dates_cleaned
      .withColumn("name", lower(col("name")))
      .withColumn("desc", lower(col("desc")))
      .withColumn("text", 
        concat_ws(" ", 
          lower(col("name")), 
          lower(col("desc")), 
          lower(col("keywords"))
        )
      )

    println("\nExtrait du DataFrame après Feature Engineering (focus sur les nouvelles colonnes) :")
    df_with_text.select("days_campaign", "hours_prepa", "text").show(5, false)

    println("\nSchéma final après Feature Engineering :")
    df_with_text.printSchema()

    // --- 4. Traitement des valeurs nulles ---
    println("\n--- Traitement des valeurs nulles ---")
    val dfFinal = df_with_text.na.fill(-1, Seq("days_campaign", "hours_prepa", "goal"))
                             .na.fill("unknown", Seq("country2", "currency2"))

    println("Extrait du DataFrame final (après traitement des null) :")
    dfFinal.show(5, false)
    
    println("\nSchéma final :")
    dfFinal.printSchema()

    // --- 5. Sauvegarde du DataFrame ---
    val outputPath = "data/kickstarter_preprocessed"
    println(s"\n--- Sauvegarde du DataFrame final au format Parquet dans : $outputPath ---")
    dfFinal.write
      .mode("overwrite") // Écrase le dossier s'il existe déjà
      .parquet(outputPath)
    
    println("Sauvegarde terminée avec succès !")

    // On arrête la session Spark
    spark.stop()
    println("\n👋 Fin du TP. Session Spark arrêtée. 👋")
  }
}