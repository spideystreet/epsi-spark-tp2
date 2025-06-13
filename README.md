# Projet de Prédiction de Succès de Campagnes Kickstarter

Ce projet s'inscrit dans le cadre du TP n°2 du module Spark. L'objectif est de construire un pipeline de prétraitement (pre-processing) de données robustes avec Scala et Spark, afin de préparer un jeu de données pour l'entraînement d'un modèle de Machine Learning capable de prédire le succès d'une campagne de financement participatif sur la plateforme Kickstarter.

---

## 🚀 Technologies utilisées

*   **Langage :** Scala (version 2.13.14)
*   **Framework de calcul distribué :** Apache Spark
*   **Outil de build :** sbt (Simple Build Tool)

---

## 📂 Structure du projet

```
.
├── build.sbt                 # Fichier de configuration pour sbt (dépendances, version de Scala...)
├── data/
│   ├── train.csv             # Données brutes des campagnes Kickstarter.
│   └── kickstarter_preprocessed/ # Dossier de sortie contenant le DataFrame final au format Parquet.
├── launchScalaApp.zsh        # Script shell pour compiler et lancer l'application.
├── project/                  # Fichiers de configuration internes de sbt.
├── src/
│   └── main/
│       └── scala/
│           └── SimpleApp.scala # Le code source de l'application Spark.
└── README.md                 # Ce fichier.
```

---

## 🛠️ Comment lancer le pipeline

Le projet utilise `sbt` qui gère à la fois la compilation du code Scala et l'exécution de l'application Spark.

Pour lancer l'intégralité du pipeline de traitement :

1.  **Assurez-vous d'être à la racine du projet.**
2.  **Rendez le script de lancement exécutable (à faire une seule fois) :**
    ```bash
    chmod +x launchScalaApp.zsh
    ```
3.  **Exécutez le script :**
    ```bash
    ./launchScalaApp.zsh
    ```

Le script va :
1.  Compiler le code source Scala.
2.  Télécharger les dépendances nécessaires (Spark, etc.).
3.  Lancer l'application Spark.
4.  Effectuer toutes les étapes de nettoyage et de transformation.
5.  Sauvegarder le DataFrame final dans le dossier `data/kickstarter_preprocessed`.

---

## ✨ Étapes du pipeline de prétraitement

Le script `SimpleApp.scala` effectue les opérations suivantes, conformément au sujet du TP :

1.  **Chargement des Données :**
    *   Lecture du fichier `data/train.csv`.
    *   Utilisation de `try_cast` pour convertir les colonnes numériques (`goal`, `final_status`) et les timestamps (`deadline`, etc.) de manière sécurisée, en ignorant les lignes mal formatées.

2.  **Nettoyage (Cleaning) :**
    *   Suppression des colonnes jugées inutiles ou présentant des fuites du futur (`disable_communication`, `backers_count`, `state_changed_at`).
    *   Création et application d'UDFs (User Defined Functions) pour nettoyer et standardiser les colonnes `country` et `currency`.
    *   Filtrage du dataset pour ne conserver que les projets ayant un statut final clair (succès `1` ou échec `0`).

3.  **Ingénierie de fonctionnalités (Feature Engineering) :**
    *   Calcul de la durée de la campagne en jours (`days_campaign`).
    *   Calcul du temps de préparation de la campagne en heures (`hours_prepa`).
    *   Mise en minuscule et concaténation des colonnes textuelles (`name`, `desc`, `keywords`) en une seule colonne `text`.

4.  **Finalisation et Sauvegarde :**
    *   Remplacement des valeurs `null` restantes par des valeurs par défaut.
    *   Sauvegarde du DataFrame final au format **Parquet** dans `data/kickstarter_preprocessed`, un format optimisé pour les analyses Spark.