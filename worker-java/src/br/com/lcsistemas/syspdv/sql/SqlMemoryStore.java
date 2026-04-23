package br.com.lcsistemas.syspdv.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stub vazio — o modo in-memory foi substituído pelo H2 (SDD v3.0).
 *
 * ctx.getMemoryStore() SEMPRE retorna null.
 * Os steps verificam (store != null) antes de chamar qualquer método,
 * portanto este código nunca executa em runtime — apenas compila.
 *
 * Métodos presentes: suficientes para satisfazer o compilador.
 */
public class SqlMemoryStore {

    /** Retorna todas as linhas de uma tabela. Nunca chamado (store é sempre null). */
    public List<Map<String, Object>> selectAll(String table) {
        return new ArrayList<Map<String, Object>>();
    }

    /** Insere uma linha e retorna o ID gerado. Nunca chamado. */
    public int insert(String table, Map<String, Object> row) {
        return 0;
    }

    /** Insere dado estático de referência. Nunca chamado. */
    public void insertStatic(String table, Map<String, Object> row) {
        // no-op
    }

    /** Define o próximo AUTO_INCREMENT. Nunca chamado. */
    public void setAutoIncrement(String table, int nextId) {
        // no-op
    }

    /** Remove todas as linhas de uma tabela. Nunca chamado. */
    public void clear(String table) {
        // no-op
    }
}
