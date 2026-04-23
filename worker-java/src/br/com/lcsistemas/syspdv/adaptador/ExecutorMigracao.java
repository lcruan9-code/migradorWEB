package br.com.lcsistemas.syspdv.adaptador;

import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.Connection;
import java.util.List;
import java.util.logging.Logger;

/**
 * Executor de migração com mecanismo de fallback automático.
 *
 * <p>Executa os {@link AdaptadorMigracao} em sequência:
 * <ul>
 *   <li>✅ Se o adaptador tiver <b>sucesso</b> → para a execução imediatamente.</li>
 *   <li>❌ Se o adaptador <b>falhar</b>       → registra o erro e tenta o próximo.</li>
 *   <li>💀 Se <b>todos falharem</b>           → lança {@link MigracaoException} controlada.</li>
 * </ul>
 *
 * <p>Regra de segurança: qualquer exceção lançada por um adaptador é capturada e
 * registrada antes de tentar o próximo, garantindo que a falha de uma estratégia
 * nunca impeça a tentativa das seguintes.
 *
 * Uso:
 * <pre>
 *   ExecutorMigracao.executar(origemConn, migradores, logCallback);
 * </pre>
 */
public final class ExecutorMigracao {

    private static final Logger LOG = Logger.getLogger(ExecutorMigracao.class.getName());

    /** Interface funcional para callback de log — compatível com Java 8. */
    public interface LogCallback {
        void log(String msg);
    }

    /** Classe utilitária — não instanciar. */
    private ExecutorMigracao() {}

    /**
     * Executa os adaptadores em sequência com fallback automático.
     *
     * @param conn       Conexão JDBC de origem (Firebird), já aberta.
     * @param migradores Lista ordenada de adaptadores (prioridade decrescente).
     * @param logCallback Callback para envio de mensagens de log à UI.
     * @throws MigracaoException Se todos os adaptadores falharem.
     */
    public static void executar(Connection conn,
                                List<AdaptadorMigracao> migradores,
                                LogCallback logCallback) throws MigracaoException {

        if (migradores == null || migradores.isEmpty()) {
            throw new MigracaoException("ExecutorMigracao",
                "Nenhum adaptador de migração disponível para esta versão do Firebird.");
        }

        int tentativa = 0;
        int total = migradores.size();
        Exception ultimoErro = null;

        for (AdaptadorMigracao migrador : migradores) {
            tentativa++;
            String nome = migrador.getNome();

            log(logCallback, "[ExecutorMigracao] Tentando estratégia " + tentativa + "/" + total
                           + ": " + nome);

            try {
                migrador.executar(conn);

                // ✅ Sucesso — para aqui
                log(logCallback, "[ExecutorMigracao] Estratégia '" + nome + "' concluída com sucesso.");
                return;

            } catch (Exception e) {
                // ❌ Falhou — registra e tenta próximo
                ultimoErro = e;
                log(logCallback, "[ExecutorMigracao] Estratégia '" + nome + "' falhou: "
                               + e.getMessage());

                if (tentativa < total) {
                    log(logCallback, "[ExecutorMigracao] Tentando próxima estratégia...");
                }
            }
        }

        // 💀 Todas as estratégias falharam
        String mensagem = "Todas as " + total + " estratégia(s) de migração falharam. "
                        + "Último erro: " + (ultimoErro != null ? ultimoErro.getMessage() : "desconhecido");

        log(logCallback, "[ExecutorMigracao] ERRO FATAL: " + mensagem);
        throw new MigracaoException("ExecutorMigracao", mensagem, ultimoErro);
    }

    // ── Helper de log seguro ──────────────────────────────────────────────────

    private static void log(LogCallback callback, String msg) {
        LOG.info(msg);
        if (callback != null) {
            try { callback.log(msg); } catch (Exception ignored) {}
        }
    }
}
