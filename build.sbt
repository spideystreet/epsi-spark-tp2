name := "App"
version := "1.0"
scalaVersion := "2.13.14" // Mise à jour pour correspondre aux dépendances

// Dépendance à Spark SQL
libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.0.0-preview1"

// Dépendance pour la réflexion, nécessaire pour les UDFs
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value