package br.com.lcsistemas.host.engine;

import br.com.lcsistemas.host.adaptador.AdaptadorMigracao;
import br.com.lcsistemas.host.adaptador.MigracaoHost;
import br.com.lcsistemas.host.config.MigracaoConfig;
import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Logger;
import javax.swing.SwingWorker;

/**
 * Orquestra toda a migração HOST (MySQL) → lc_sistemas (MySQL).
 * Roda dentro de um SwingWorker para não bloquear a UI.
 * NÃO usa Firebird — conexões de origem e destino são ambas MySQL.
 *
 * Uso:
 * <pre>
 *   MigracaoEngine engine = new MigracaoEngine(config);
 *   engine.setListener(meuListener);
 *   engine.executar();
 *   engine.cancelar();
 * </pre>
 */
public class MigracaoEngine {

    private static final Logger LOG = Logger.getLogger(MigracaoEngine.class.getName());

    // ── Interface de callbacks para a UI ──────────────────────────────────────

    public interface ProgressListener {
        void onLog(String msg);
        void onStepInicio(String nome, int atual, int total);
        void onStepConcluido(String nome, int inseridos, int ignorados, int erros, boolean ok);
        void onConcluido(boolean ok, String mensagemFinal);
    }

    // ── Estado ────────────────────────────────────────────────────────────────

    private final MigracaoConfig          config;
    private ProgressListener              listener;
    private volatile boolean              cancelado = false;
    private SwingWorker<Void, Void>       worker;

    public MigracaoEngine(MigracaoConfig config) {
        this.config = config;
    }

    public void setListener(ProgressListener l) { this.listener = l; }

    // ── API pública ───────────────────────────────────────────────────────────

    /** Inicia a migração em thread de background. */
    public void executar() {
        cancelado = false;
        worker = new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                migrar();
                return null;
            }
        };
        worker.execute();
    }

    /** Sinaliza cancelamento. */
    public void cancelar() {
        cancelado = true;
        if (worker != null) worker.cancel(true);
    }

    // =========================================================================
    //  Fluxo principal de migração
    // =========================================================================

    private void migrar() {
        log("=================================================");
        log("  LC Sistemas — HOST Migration Engine v1.0");
        log("  Origem: " + config.buildUrlOrigem());
        log("  Destino: " + config.buildUrlMySQL());
        log("=================================================");

        Connection origemConn  = null;
        Connection destinoConn = null;

        try {
            // ── 1. Conecta ao destino (lc_sistemas — MySQL) ───────────────────
            log("[1/4] Conectando ao banco de destino (lc_sistemas)...");
            destinoConn = conectarMySQL(config.buildUrlMySQL(),
                config.getMyUsuario(), config.getMySenha());
            destinoConn.setAutoCommit(false);
            log("      Destino OK.");

            if (cancelado) { concluido(false, "Cancelado."); return; }

            // ── 2. Conecta à origem (host — MySQL) ────────────────────────────
            log("[2/4] Conectando ao banco de origem (host)...");
            origemConn = conectarMySQL(config.buildUrlOrigem(),
                config.getOrigemUsuario(), config.getOrigemSenha());
            origemConn.setAutoCommit(true); // DDL na origem usa autocommit
            log("      Origem OK.");

            if (cancelado) { concluido(false, "Cancelado."); return; }

            // ── 3. Montar contexto ────────────────────────────────────────────
            log("[3/4] Montando contexto...");
            MigracaoContext ctx = new MigracaoContext(config);
            ctx.setOrigemConn(origemConn);
            ctx.setDestinoConn(destinoConn);

            // Atualiza CNPJ da empresa no destino (se configurado)
            String cnpj = config.getEmpresaCnpj();
            if (!cnpj.isEmpty()) {
                log("[PRÉ] Atualizando CNPJ da empresa (id=" + config.getEmpresaId() + "): " + cnpj);
                try {
                    PreparedStatement psEmpresa = destinoConn.prepareStatement(
                        "UPDATE lc_sistemas.empresa SET cnpj = ? WHERE id = ?");
                    psEmpresa.setString(1, cnpj);
                    psEmpresa.setInt(2, config.getEmpresaId());
                    int rows = psEmpresa.executeUpdate();
                    psEmpresa.close();
                    destinoConn.commit();
                    log("[PRÉ] empresa.cnpj atualizado — " + rows + " linha(s).");
                } catch (SQLException eCnpj) {
                    log("[PRÉ] AVISO: não foi possível atualizar empresa.cnpj: " + eCnpj.getMessage());
                }
            }

            if (cancelado) { concluido(false, "Cancelado."); return; }

            // ── 4. Executar migração HOST ──────────────────────────────────────
            log("[4/4] Iniciando migração HOST...");

            final MigracaoHost.CancelCheck cancelCheck = new MigracaoHost.CancelCheck() {
                @Override
                public boolean isCancelado() { return cancelado; }
            };

            AdaptadorMigracao migracao = new MigracaoHost(ctx, listener, cancelCheck);
            migracao.executar(origemConn);

            // ── Resumo final ──────────────────────────────────────────────────
            log("=================================================");
            log("  Migração concluída com sucesso!");
            for (MigracaoContext.StepStats s : ctx.getStats().values()) {
                log(String.format("    %-25s inseridos=%-6d ignorados=%-6d %s",
                    s.nome, s.inseridos, s.ignorados,
                    s.erros == 0 ? "OK" : "ERROS=" + s.erros));
            }
            log("=================================================");
            concluido(true, "Migração HOST concluída com sucesso! " + ctx.getStats().size() + " steps.");

        } catch (MigracaoException e) {
            log("ERRO NA MIGRAÇÃO: " + e.getMessage());
            concluido(false, "Migração falhou: " + e.getMessage());

        } catch (Exception e) {
            log("ERRO INESPERADO: " + e.getMessage());
            concluido(false, "Erro inesperado: " + e.getMessage());

        } finally {
            fechar(origemConn,  "origem (host)");
            fechar(destinoConn, "destino (lc_sistemas)");
        }
    }

    // ── Conexões ──────────────────────────────────────────────────────────────

    private Connection conectarMySQL(String url, String usuario, String senha) throws SQLException {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            try { Class.forName("com.mysql.jdbc.Driver"); } catch (ClassNotFoundException ignored) {}
        }
        return DriverManager.getConnection(url, usuario, senha);
    }

    // ── Helpers de notificação ────────────────────────────────────────────────

    private void log(String msg) {
        LOG.info(msg);
        if (listener != null) listener.onLog(msg);
    }

    private void concluido(boolean ok, String msg) {
        if (listener != null) listener.onConcluido(ok, msg);
    }

    private void fechar(Connection conn, String nome) {
        if (conn != null) {
            try { conn.close(); }
            catch (Exception e) { LOG.warning("Erro ao fechar " + nome + ": " + e.getMessage()); }
        }
    }
}
