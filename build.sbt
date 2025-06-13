name := "App"
version := "1.0"
scalaVersion := "2.13.12" // Assurez-vous que cette version est compatible avec votre Spark

// Dépendance à Spark SQL
libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.0.0" % "provided"