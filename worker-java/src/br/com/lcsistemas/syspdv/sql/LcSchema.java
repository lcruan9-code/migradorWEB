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

    /**
     * Mapa: nome UPPERCASE do portal → nome lowercase do dump SQL.
     * Usado para converter a seleção do usuário em nomes de tabelas reais.
     */
    private static final Map<String, String> PORTAL_TO_SQL = new LinkedHashMap<>();
    static {
        PORTAL_TO_SQL.put("PRODUTO",          "produto");
        PORTAL_TO_SQL.put("UNIDADE",          "unidade");
        PORTAL_TO_SQL.put("CATEGORIA",        "categoria");
        PORTAL_TO_SQL.put("SUBCATEGORIA",     "subcategoria");
        PORTAL_TO_SQL.put("FABRICANTE",       "fabricante");
        PORTAL_TO_SQL.put("FORNECEDORES",     "fornecedor");   // plural → singular
        PORTAL_TO_SQL.put("CLIENTE",          "cliente");
        PORTAL_TO_SQL.put("CST",              "cst");
        PORTAL_TO_SQL.put("NCM",              "ncm");
        PORTAL_TO_SQL.put("CEST",             "cest");
        PORTAL_TO_SQL.put("GRUPO_TRIBUTACAO", "grupotributacao");
        PORTAL_TO_SQL.put("AJUSTE_ESTOQUE",   "ajusteestoque");
        PORTAL_TO_SQL.put("ESTOQUE_SALDO",    "estoquesaldo");
        PORTAL_TO_SQL.put("ESTOQUE",          "estoque");
        PORTAL_TO_SQL.put("RECEBER",          "receber");
        PORTAL_TO_SQL.put("PAGAR",            "pagar");
    }

    /**
     * Converte um conjunto de nomes UPPERCASE do portal para nomes lowercase do dump.
     * Nomes desconhecidos são ignorados. Conjunto vazio → retorna vazio (= dump tudo).
     */
    public static Set<String> toSqlNames(Set<String> portalNames) {
        if (portalNames == null || portalNames.isEmpty()) return Collections.emptySet();
        Set<String> result = new LinkedHashSet<>();
        for (String name : portalNames) {
            String sql = PORTAL_TO_SQL.get(name.toUpperCase());
            if (sql != null) result.add(sql);
        }
        return result;
    }
}
