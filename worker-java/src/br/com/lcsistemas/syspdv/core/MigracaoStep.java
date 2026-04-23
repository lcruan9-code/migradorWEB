package br.com.lcsistemas.syspdv.core;

/**
 * Interface que todo Step de migração deve implementar.
 * Cada Step encapsula a lógica de transferência de UMA entidade
 * (ex: NCM, Fornecedor, Produto…).
 */
public interface MigracaoStep {

    /** Nome exibido na UI e nos logs (ex: "NCM", "Fornecedor"). */
    String getNome();

    /**
     * Preparação: limpa dados no destino, adiciona colunas auxiliares no
     * Firebird, ajusta AUTO_INCREMENT, etc.
     */
    void prepare(MigracaoContext ctx) throws MigracaoException;

    /**
     * Execução principal: lê da origem e escreve no destino.
     * Deve registrar estatísticas via ctx.registrarStats().
     */
    void execute(MigracaoContext ctx) throws MigracaoException;

    /**
     * Rollback: desfaz o que prepare/execute escreveram no destino.
     * Chamado automaticamente pelo engine em caso de falha.
     */
    void rollback(MigracaoContext ctx);

    /**
     * Cleanup: remove colunas auxiliares do Firebird, libera recursos.
     * Sempre chamado após execute (com ou sem erro).
     */
    void cleanup(MigracaoContext ctx);
}
