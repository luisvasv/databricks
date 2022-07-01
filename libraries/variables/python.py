# Databricks notebook source
class Constants:
    def __init__(self):
        self.__OK: int = 0
        self.__ERROR: int = -1
        self.__ENGINE: str = 'databricks'

    @property
    def OK(self):
        return self.__OK

    @property
    def ERROR(self):
        return self.__ERROR

    @property
    def ENGINE(self):
        return self.__ENGINE


# COMMAND ----------

CONSTANTS = Constants()
