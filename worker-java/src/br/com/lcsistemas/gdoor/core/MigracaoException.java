package br.com.lcsistemas.gdoor.core;

/**
 * Exceção específica do Motor de Migração.
 * Carrega o nome do step que falhou para facilitar o diagnóstico.
 */
public class MigracaoException extends Exception {

    private final String nomeStep;

    public MigracaoException(String nomeStep, String mensagem) {
        super(mensagem);
        this.nomeStep = nomeStep;
    }

    public MigracaoException(String nomeStep, String mensagem, Throwable causa) {
        super(mensagem, causa);
        this.nomeStep = nomeStep;
    }

    public String getNomeStep() {
        return nomeStep;
    }
}
