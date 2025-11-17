#!/usr/bin/env python
# coding: utf-8

# Funções utilitárias para construção das queries SQL


# Subqueries para filtrar as oficinas, seções e ordens de serviço quando TODAS não for selecionado
def subquery_oficinas(lista_oficinas, prefix="", termo_all="TODAS"):
    query = ""
    if termo_all not in lista_oficinas:
        query = f"""AND {prefix}"DESCRICAO DA OFICINA" IN ({', '.join([f"'{x}'" for x in lista_oficinas])})"""

    return query


def subquery_secoes(lista_secaos, prefix="", termo_all="TODAS"):
    query = ""
    if termo_all not in lista_secaos:
        query = f"""AND {prefix}"DESCRICAO DA SECAO" IN ({', '.join([f"'{x}'" for x in lista_secaos])})"""

    return query


def subquery_os(lista_os, prefix="", termo_all="TODAS"):
    if not lista_os or termo_all in lista_os:
        return ""
    valores = ", ".join([f"'{x}'" for x in lista_os if x])  
    if not valores:  
        return ""

    return f'AND {prefix}"DESCRICAO DO SERVICO" IN ({valores})'



def subquery_modelos(lista_modelos, prefix="", termo_all="TODAS"):
    query = ""
    if termo_all not in lista_modelos:
        query = f"""AND {prefix}"DESCRICAO DO MODELO" IN ({', '.join([f"'{x}'" for x in lista_modelos])})"""

    return query

