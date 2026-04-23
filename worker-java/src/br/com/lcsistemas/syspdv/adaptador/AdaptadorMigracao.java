package br.com.lcsistemas.syspdv.adaptador;

import java.sql.Connection;

/**
 * Strategy Pattern — contrato de toda estratégia de migração.
 *
 * <p>Cada implementação representa uma versão específica do Gdoor ou
 * uma abordagem alternativa de migração. O {@link ExecutorMigracao}
 * executa adaptadores em sequência, com fallback automático.
 *
 * <p>Contrato:
 * <ul>
 *   <li>{@code conn} é a conexão JDBC de <b>origem</b> (Firebird) já aberta.</li>
 *   <li>A conexão de destino (MySQL) é acessível via contexto interno do adaptador.</li>
 *   <li>Qualquer falha fatal deve ser lançada como {@link Exception} para acionar o fallback.</li>
 *   <li>O adaptador é responsável pelo seu próprio cleanup interno.</li>
 * </ul>
 *
 * Implementações existentes:
 * <ul>
 *   <li>{@link MigracaoGdoor5} — migração Gdoor 5.x (produção, validada)</li>
 *   <li>{@link MigracaoGdoor6} — Gdoor 6.x (estrutura futura)</li>
 *   <li>{@link MigracaoGdoor7} — Gdoor 7.x (estrutura futura)</li>
 * </ul>
 */
public interface AdaptadorMigracao {

    /** Callback de cancelamento verificado a cada step. */
    interface CancelCheck {
        boolean isCancelado();
    }

    /**
     * Nome legível desta estratégia — exibido nos logs e na UI.
     * Ex: "MigracaoGdoor5", "MigracaoGdoor6".
     */
    String getNome();

    /**
     * Executa a migração completa usando a conexão de origem fornecida.
     *
     * @param conn Conexão JDBC de origem (Firebird), já aberta e ativa.
     * @throws Exception em caso de falha irrecuperável — aciona o fallback no executor.
     */
    void executar(Connection conn) throws Exception;
}
