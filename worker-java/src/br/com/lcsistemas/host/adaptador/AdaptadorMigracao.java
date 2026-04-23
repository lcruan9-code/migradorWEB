package br.com.lcsistemas.host.adaptador;

import java.sql.Connection;

/**
 * Interface para estratégias de migração HOST.
 */
public interface AdaptadorMigracao {
    String getNome();
    void executar(Connection conn) throws Exception;
}
