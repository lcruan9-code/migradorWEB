package br.com.lcsistemas.host.core;

/**
 * Contrato de um passo (step) de migração HOST.
 */
public interface MigracaoStep {

    /** Nome descritivo do step — usado em logs e estatísticas. */
    String getNome();

    /** Preparação antes do execute (índices, etc.). */
    void prepare(MigracaoContext ctx) throws MigracaoException;

    /** Lógica principal de migração. */
    void execute(MigracaoContext ctx) throws MigracaoException;

    /** Limpeza após execute (drop de índices, colunas temp, etc.). */
    void cleanup(MigracaoContext ctx);

    /** Desfaz alterações em caso de erro. */
    void rollback(MigracaoContext ctx);
}
