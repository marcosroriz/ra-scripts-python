#!/usr/bin/env python
# coding: utf-8

# Classe que fornecer funções para listar e executar Regras de Monitoramento

# Imports básicos
import pandas as pd

# Import específico
from rule import Rule

# Classe
class RuleManager(object):
    def __init__(self, pg_engine):
        self.pg_engine = pg_engine

    def get_all_rules(self):
        """
        Obtém todas as regras de monitoramento do banco de dados.
        Retorna uma lista de objetos Rule.
        """
        
        query = "SELECT * FROM public.regra_monitoramento_os"
        df_regras = pd.read_sql(query, self.pg_engine)

        regras = [Rule(row_dados_regra, self.pg_engine) for _, row_dados_regra in df_regras.iterrows()]
        
        return regras   