# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse

def main():
    """
    Cette methode est le point d'entrée de mon job.
    Elle va essentiellement faire 3 choses:
        - recuperer les arguments passés via la ligne de commande
        - creer une session spark
        - lancer le traitement
    Le traitement ne doit pas se faire dans la main pour des soucis de testabilité.
    """
    parser = argparse.ArgumentParser(
        description='Discover driving sessions into log files.')
    parser.add_argument('-f', "--full", required=True)
    parser.add_argument('-s', "--school", required=True)
    parser.add_argument('-o', '--output', required=True)

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    process(spark,args.full,args.school,args.output)

def process(spark, full, school, output):
    """
    Contient les traitements qui permettent de lire,transformer et sauvegarder mon resultat.
    :param spark: la session spark
    :param full:  le chemin du dataset full
    :param school: le chemin du datasetschool
    :param output: l'emplacement souhaité du resultat
    """
    #On charge les deux fichiers csv full et school
    full = spark.read.option('header', 'true').option('inferSchema', 'true').csv(full)
    school = spark.read.option('header', 'true').option('inferSchema', 'true').option('sep', ';').csv(school)
    
    #On ne garde que les colonnes qui nous intéressent
    school = school.select('Code établissement', 'Appellation officielle', 'Code postal')
    full = full.select('valeur_fonciere', 'code_postal', 'code_type_local', 'type_local')
    
    #On supprime les lignes avec des null
    school = school.na.drop()
    full = full.na.drop()

    #On ne s'interesse qu'aux habitations de particulier. On supprime donc tous les locaux industriels (code_type_local 4)
    full = full.filter(full.code_type_local != 4)
    
    #Pour chaque code postal, on ne s'intéresse qu'au nombre d'établissements scolaires présents
    school = school.groupby('Code postal').count()
    
    #On transforme les données pour qu'elles soient plus manipulables
    school = school.withColumnRenamed('Code postal', 'codepostal')
    
    #On fait la jointure sur le code postal
    df = full.join(school.hint('broadcast'), full.code_postal == school.codepostal, 'inner')
    
    #On a le code postal en double, on en drop un
    df = df.drop('codepostal')
    
    #On choisit d'enregistrer en parquet pour améliorer les performances de requetage
    df.write.parquet(output) 
   
if __name__ == '__main__':
    main()
