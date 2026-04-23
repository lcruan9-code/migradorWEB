package br.com.lcsistemas.syspdv.sql;

import java.util.*;

/**
 * Define a ordem de saída das tabelas no dump MySQL 5.5.38.
 * A estrutura (DDL) e as colunas agora são descobertas dinamicamente pelo SqlFileWriter
 * a partir do banco de dados H2 (bootstrapped via banco_novo.sql).
 */
public class LcSchema {

    /** Ordem de saída das tabelas no dump para evitar erros de FK e manter padrão */
    public static final List<String> TABLE_ORDER = Arrays.asList(
        "produto","unidade","categoria","subcategoria","fabricante",
        "fornecedor","cliente","cst","ncm","cest",
        "grupotributacao","ajusteestoque","estoquesaldo","estoque",
        "receber","pagar"
    );

    /** Verifica se a tabela está no escopo da migração */
    public static boolean hasTable(String table) { 
        return TABLE_ORDER.contains(table.toLowerCase()); 
    }
}
