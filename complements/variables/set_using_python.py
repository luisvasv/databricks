# Databricks notebook source
from typing import Dict, Any

# COMMAND ----------

# define dynamic class
class Variables:
    pass

# COMMAND ----------

# define struct class to transform dictionaries to objet.key
class Struct:
    def __init__(self, **config: dict):
        """
        class constructor Struct

        parameters :
            config : dictionary with the values
        outputs :
            struct objet
        usage:
            >>> structure = Struct(**{'version':'1.0','createdby':'luis'})
            >>> structure.version
            1.0
            >>> structure.createdby
            luis
        """
        self.__dict__.update(config)

# COMMAND ----------

# define variables
variables: Dict[str, Any] ={
    "dataset":{
        "social_ads": "https://gitlab.com/luisvasv/public/-/raw/master/datasets/001.analitica.predictiva/003.social.ads.csv"
    }
}

# COMMAND ----------

# set up attributes
training: Variables  = Variables()
for key, value in variables.items():
    setattr(training, key, Struct(**value))
