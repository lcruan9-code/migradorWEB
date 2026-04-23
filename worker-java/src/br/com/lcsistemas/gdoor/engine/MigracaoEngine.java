package br.com.lcsistemas.gdoor.engine;

import br.com.lcsistemas.gdoor.adaptador.AdaptadorMigracao;
import br.com.lcsistemas.gdoor.adaptador.ExecutorMigracao;
import br.com.lcsistemas.gdoor.adaptador.FabricaMigracao;
import br.com.lcsistemas.gdoor.adaptador.MigracaoGdoor5;
import br.com.lcsistemas.gdoor.config.MigracaoConfig;
import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;
import br.com.lcsistemas.gdoor.firebird.GerenciadorFirebird;
import br.com.lcsistemas.gdoor.versao.DetectorFirebird;
import br.com.lcsistemas.gdoor.versao.VersaoFirebird;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.List;

import java.util.logging.Logger;
import javax.swing.SwingWorker;

/**
 * Orquestra toda a migração Firebird (GDOOR) → MySQL.
 * Roda dentro de um SwingWorker para não bloquear a UI.
 *
 * <p><b>Evolução v2.1:</b>
 * <ul>
 *   <li>Detecta automaticamente a versão do Firebird instalado.</li>
 *   <li>Inicia automaticamente o Firebird correto se nenhum serviço estiver ativo.</li>
 *   <li>Encerra o Firebird ao final (somente o que o engine iniciou).</li>
 *   <li>Delega a execução à estratégia correta via {@link FabricaMigracao}.</li>
 *   <li>100% compatível com a versão anterior.</li>
 * </ul>
 *
 * Uso:
 * <pre>
 *   MigracaoEngine engine = new MigracaoEngine(config);
 *   engine.setListener(meuListener);
 *   engine.executar();        // inicia em background
 *   engine.cancelar();        // sinaliza cancelamento
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

    private final MigracaoConfig  config;
    private ProgressListener      listener;
    private volatile boolean      cancelado = false;
    private SwingWorker<Void, Void> worker;

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

    /** Sinaliza cancelamento (o engine verifica entre steps). */
    public void cancelar() {
        cancelado = true;
        if (worker != null) worker.cancel(true);
    }

    // =========================================================================
    //  Fluxo principal de migração
    // =========================================================================

    private void migrar() {
        log("=================================================");
        log("  LC Sistemas — GDOOR Migration Engine v2.1");
        log("  CNPJ Empresa: " + config.getEmpresaCnpj());
        log("=================================================");

        Connection origemConn  = null;
        Connection destinoConn = null;

        // Callback de log reutilizado pelo GerenciadorFirebird e ExecutorMigracao
        final GerenciadorFirebird.LogCallback logCallback = new GerenciadorFirebird.LogCallback() {
            @Override
            public void log(String msg) {
                MigracaoEngine.this.log(msg);
            }
        };

        try {
            // ── 1. Destino (MySQL) primeiro ───────────────────────────────────
            log("[1/5] Conectando ao banco de destino (MySQL)...");
            destinoConn = conectarMySQL();
            destinoConn.setAutoCommit(false);
            log("      Destino OK: " + config.buildUrlMySQL());

            if (cancelado) { concluido(false, "Cancelado."); return; }

            // ── 2. Garantir serviço Firebird ativo ────────────────────────────
            // GerenciadorFirebird: verifica porta, inicia a versão correta se necessário.
            log("[2/5] Verificando serviço Firebird em "
                + config.getFbHost() + ":" + config.getFbPorta() + "...");
            GerenciadorFirebird.garantirConectividade(config, logCallback);

            // ── 3. Conectar ao banco de origem (Firebird — com auto-retry ODS) ──
            // Se o banco for de versão superior ao Firebird iniciado, detecta o ODS,
            // troca para a versão correta e retenta automaticamente.
            log("[3/5] Conectando ao banco de origem (Firebird)...");
            origemConn = conectarOrigemComAutoRetry(logCallback);
            origemConn.setAutoCommit(true); // Firebird: autocommit para DDL
            log("      Origem OK: " + config.buildUrlFirebird());

            if (cancelado) { concluido(false, "Cancelado."); return; }

            // ── 4. Montar contexto ────────────────────────────────────────────
            log("[4/5] Montando contexto...");
            MigracaoContext ctx = new MigracaoContext(config);
            ctx.setOrigemConn(origemConn);
            ctx.setDestinoConn(destinoConn);

            // ── Atualizar CNPJ da empresa no destino ──────────────────────────
            String cnpj = config.getEmpresaCnpj();
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

            if (cancelado) { concluido(false, "Cancelado."); return; }

            // ── Detectar versão do Firebird em execução ───────────────────────
            // DetectorFirebird: nunca lança erro fatal.
            log("[PRÉ] Detectando versão do Firebird...");
            VersaoFirebird versao = DetectorFirebird.detectar(origemConn);
            log("      Versão detectada: " + versao);

            if (cancelado) { concluido(false, "Cancelado."); return; }

            // ── 5. Executar migração adaptada ─────────────────────────────────
            // FabricaMigracao → AdaptadorMigracao → ExecutorMigracao (com fallback)
            log("[5/5] Iniciando migração adaptada para " + versao + "...");

            final MigracaoGdoor5.CancelCheck cancelCheck = new MigracaoGdoor5.CancelCheck() {
                @Override
                public boolean isCancelado() {
                    return cancelado;
                }
            };

            List<AdaptadorMigracao> migradores =
                FabricaMigracao.getMigradores(versao, ctx, listener, cancelCheck);

            // Reutiliza o logCallback já criado acima
            ExecutorMigracao.LogCallback execLogCallback = new ExecutorMigracao.LogCallback() {
                @Override
                public void log(String msg) {
                    MigracaoEngine.this.log(msg);
                }
            };

            ExecutorMigracao.executar(origemConn, migradores, execLogCallback);

            // ── Resumo final ──────────────────────────────────────────────────
            log("=================================================");
            log("  Migração concluída com sucesso!");
            for (MigracaoContext.StepStats s : ctx.getStats().values()) {
                log(String.format("    %-25s %s",
                    s.nome, s.erros == 0 ? "OK" : "Com erros (" + s.erros + ")"));
            }
            log("=================================================");
            concluido(true, "Migração concluída com sucesso! " + ctx.getStats().size() + " steps.");

        } catch (MigracaoException e) {
            log("ERRO NA MIGRAÇÃO: " + e.getMessage());
            concluido(false, "Migração falhou: " + e.getMessage());

        } catch (Exception e) {
            log("ERRO INESPERADO: " + e.getMessage());
            concluido(false, "Erro inesperado: " + e.getMessage());

        } finally {
            fechar(origemConn,  "origem");
            fechar(destinoConn, "destino");
            // Para o Firebird apenas se foi o ENGINE que o iniciou
            GerenciadorFirebird.pararSeIniciado(logCallback);
        }
    }

    // ── Conexões ──────────────────────────────────────────────────────────────

    private Connection conectarMySQL() throws SQLException {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            try { Class.forName("com.mysql.jdbc.Driver"); } catch (ClassNotFoundException ignored) {}
        }
        return DriverManager.getConnection(config.buildUrlMySQL(),
            config.getMyUsuario(), config.getMySenha());
    }

    private Connection conectarFirebird() throws SQLException {
        try {
            Class.forName("org.firebirdsql.jdbc.FBDriver");
        } catch (ClassNotFoundException e) {
            LOG.warning("Driver Jaybird não encontrado no classpath!");
        }
        return DriverManager.getConnection(config.buildUrlFirebird(),
            config.getFbUsuario(), config.getFbSenha());
    }

    /**
     * Tenta conectar ao banco Firebird com retry automático por incompatibilidade de ODS.
     *
     * <p>Fluxo:
     * <ol>
     *   <li>Tenta a conexão JDBC normalmente.</li>
     *   <li>Se falhar com erro ODS ({@code isc_wrong_ods / 335544379}):
     *       <ul>
     *         <li>Extrai a versão ODS do banco pela mensagem do erro.</li>
     *         <li>Pede ao {@link GerenciadorFirebird} para parar o Firebird atual
     *             e iniciar a versão compatível.</li>
     *         <li>Retenta a conexão.</li>
     *       </ul>
     *   </li>
     *   <li>Erros não-ODS são propagados imediatamente.</li>
     *   <li>Máximo de {@code MAX_TENTATIVAS_ODS} trocas de versão antes de desistir.</li>
     * </ol>
     *
     * @param logCallback Callback de log passado ao GerenciadorFirebird.
     * @return Conexão JDBC aberta e válida com o banco Firebird.
     * @throws Exception Se não for possível conectar após todas as tentativas.
     */
    private Connection conectarOrigemComAutoRetry(
            GerenciadorFirebird.LogCallback logCallback) throws Exception {

        // Máximo de trocas de versão (nunca trava em loop infinito)
        final int MAX_TENTATIVAS_ODS = 5;

        for (int tentativa = 1; tentativa <= MAX_TENTATIVAS_ODS; tentativa++) {
            try {
                return conectarFirebird();

            } catch (SQLException e) {

                // Tenta identificar se é erro de ODS incompatível
                VersaoFirebird versaoNecessaria =
                    GerenciadorFirebird.detectarVersaoPorODS(e.getMessage());

                if (versaoNecessaria != VersaoFirebird.DESCONHECIDA
                        && tentativa < MAX_TENTATIVAS_ODS) {

                    // ── ODS incompatível: trocar para versão correta ──────────
                    log("⚠ ODS incompatível: banco criado em " + versaoNecessaria
                        + " — Firebird atual não suporta este formato.");
                    log("   Auto-ajustando para " + versaoNecessaria
                        + " (tentativa " + tentativa + "/" + MAX_TENTATIVAS_ODS + ")...");

                    boolean trocou = GerenciadorFirebird.trocarParaVersao(
                        versaoNecessaria, config, logCallback);

                    if (!trocou) {
                        throw new Exception(
                            "Banco requer " + versaoNecessaria
                            + ", mas nenhuma instalação compatível foi iniciada.\n"
                            + "Verifique se a pasta FIREBIRD do projeto contém "
                            + versaoNecessaria + ".\n"
                            + "Erro original: " + e.getMessage());
                    }

                    log("   Retentando conexão com " + versaoNecessaria + "...");
                    // Continua o loop → próxima tentativa com a versão correta

                } else {
                    // Outro tipo de erro ou esgotou tentativas → propaga
                    throw e;
                }
            }
        }

        throw new Exception("Não foi possível conectar ao banco Firebird após "
            + MAX_TENTATIVAS_ODS + " tentativas de auto-ajuste de versão.");
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
