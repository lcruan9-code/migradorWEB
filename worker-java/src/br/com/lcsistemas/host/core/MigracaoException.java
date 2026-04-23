package br.com.lcsistemas.host.core;

/**
 * Exceção específica da migração HOST.
 */
public class MigracaoException extends Exception {

    public MigracaoException(String step, String mensagem) {
        super("[" + step + "] " + mensagem);
    }

    public MigracaoException(String step, String mensagem, Throwable cause) {
        super("[" + step + "] " + mensagem, cause);
    }
}
