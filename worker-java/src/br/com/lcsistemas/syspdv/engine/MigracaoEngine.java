package br.com.lcsistemas.syspdv.engine;

import br.com.lcsistemas.syspdv.adaptador.AdaptadorMigracao;
import br.com.lcsistemas.syspdv.adaptador.ExecutorMigracao;
import br.com.lcsistemas.syspdv.adaptador.FabricaMigracao;
import br.com.lcsistemas.syspdv.config.MigracaoConfig;
import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;
import br.com.lcsistemas.syspdv.firebird.GerenciadorFirebird;
import br.com.lcsistemas.syspdv.sql.SqlFileRunner;
import br.com.lcsistemas.syspdv.sql.SqlFileWriter;
import br.com.lcsistemas.syspdv.versao.DetectorFirebird;
import br.com.lcsistemas.syspdv.versao.VersaoFirebird;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.List;

import java.util.logging.Logger;

/**
 * Orquestra toda a migração Firebird (GDOOR) → MySQL.
 * Roda em uma thread própria para não bloquear o worker HTTP.
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
    private Thread workerThread;

    public MigracaoEngine(MigracaoConfig config) {
        this.config = config;
    }

    public void setListener(ProgressListener l) { this.listener = l; }

    // ── API pública ───────────────────────────────────────────────────────────

    /** Inicia a migração em thread de background. */
    public void executar() {
        cancelado = false;
        workerThread = new Thread(() -> {
            try {
                migrar();
            } catch (Throwable t) {
                LOG.warning("Falha inesperada na thread da migração: " + t.getMessage());
            }
        }, "migracao-engine");
        workerThread.setDaemon(true);
        workerThread.start();
    }

    /** Sinaliza cancelamento (o engine verifica entre steps). */
    public void cancelar() {
        cancelado = true;
        if (workerThread != null) workerThread.interrupt();
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

        SqlFileWriter  sqlWriter = null;

        // Callback de log reutilizado pelo GerenciadorFirebird e ExecutorMigracao
        final GerenciadorFirebird.LogCallback logCallback = new GerenciadorFirebird.LogCallback() {
            @Override
            public void log(String msg) {
                MigracaoEngine.this.log(msg);
            }
        };

        try {
            // ── 1. Destino ────────────────────────────────────────────────────
            if (config.isModoSqlOutput()) {
                // ── Modo SQL Output: usa H2 Embutido em modo MySQL ────────────
                log("[1/5] Modo SQL Output: Inicializando H2 Embutido (MySQL Mode)...");
                log("      Arquivo de saída final: " + config.getSqlOutputPath());
                
                // UUID único por job: garante banco H2 fresco (mem nomeado é compartilhado entre conexões na JVM)
                String h2DbId = java.util.UUID.randomUUID().toString().replace("-", "");
                String h2Url = "jdbc:h2:mem:" + h2DbId + ";MODE=MySQL;DATABASE_TO_UPPER=FALSE;CASE_INSENSITIVE_IDENTIFIERS=TRUE;DB_CLOSE_DELAY=-1";
                try {
                    Class.forName("org.h2.Driver");
                    destinoConn = DriverManager.getConnection(h2Url, "sa", "");
                    // autoCommit=true durante bootstrap: cada statement é independente,
                    // evitando que falhas (CREATE DATABASE, USE) invalidem a transação inteira
                    destinoConn.setAutoCommit(true);
                    destinoConn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS lc_sistemas");
                    destinoConn.createStatement().execute("SET SCHEMA lc_sistemas");
                } catch (Exception e) {
                    throw new MigracaoException("Engine", "Erro ao iniciar H2: " + e.getMessage(), e);
                }

                // Bootstrap: Executa o SCRIPT inicial (banco_novo.sql) com autoCommit=true
                try {
                    log("      Executando bootstrap do schema (banco_novo.sql)...");
                    SqlFileRunner.LogCallback bootstrapLog = new SqlFileRunner.LogCallback() {
                        @Override public void log(String msg) { MigracaoEngine.this.log(msg); }
                    };
                    SqlFileRunner.runFromResource(destinoConn, "/br/com/lcsistemas/syspdv/resource/banco_novo.sql", bootstrapLog);
                    // Diagnóstico: distribuição de tabelas por schema (para confirmar a criação)
                    java.sql.ResultSet rsDiag = destinoConn.createStatement().executeQuery(
                        "SELECT TABLE_SCHEMA, COUNT(*) FROM INFORMATION_SCHEMA.TABLES " +
                        "WHERE TABLE_TYPE='BASE TABLE' GROUP BY TABLE_SCHEMA ORDER BY TABLE_SCHEMA");
                    int totalCriadas = 0;
                    while (rsDiag.next()) {
                        String sch = rsDiag.getString(1);
                        int cnt = rsDiag.getInt(2);
                        if (!"INFORMATION_SCHEMA".equalsIgnoreCase(sch)) {
                            totalCriadas += cnt;
                            log("      [H2] schema=" + sch + " -> " + cnt + " tabelas");
                        }
                    }
                    rsDiag.close();
                    // Verifica tabelas específicas da migração
                    String[] chave = {"empresa","unidade","produto","cliente","fornecedor","ncm","cst","cest"};
                    for (String t : chave) {
                        try { destinoConn.createStatement().executeQuery("SELECT 1 FROM lc_sistemas." + t + " LIMIT 1").close();
                        } catch (Exception ex) { log("      [H2] FALTANDO: lc_sistemas." + t); }
                    }
                    log("      Bootstrap concluido. Total tabelas: " + totalCriadas);
                    // Agora sim: transacional para os steps de migração
                    destinoConn.setAutoCommit(false);
                } catch (Exception e) {
                    throw new MigracaoException("Engine", "Erro no bootstrap do H2: " + e.getMessage(), e);
                }

                sqlWriter = new SqlFileWriter(Paths.get(config.getSqlOutputPath()));
            } else {
                log("[1/5] Conectando ao banco de destino (MySQL)...");
                destinoConn = conectarMySQL();
                destinoConn.setAutoCommit(false);
                log("      Destino OK: " + config.buildUrlMySQL());
            }

            if (cancelado) { concluido(false, "Cancelado."); return; }

            // ── 2-3. Firebird (ignorado no modo SYSPDV — origem é MySQL) ────────
            if (config.isModoSyspdv()) {
                log("[2/5] Modo SYSPDV: Firebird ignorado (origem é o próprio MySQL).");
                log("[3/5] Modo SYSPDV: usando conexão MySQL como origem.");
            } else {
                log("[2/5] Verificando serviço Firebird em "
                    + config.getFbHost() + ":" + config.getFbPorta() + "...");
                GerenciadorFirebird.garantirConectividade(config, logCallback);

                log("[3/5] Conectando ao banco de origem (Firebird)...");
                origemConn = conectarOrigemComAutoRetry(logCallback);
                origemConn.setAutoCommit(true);
                log("      Origem OK: " + config.buildUrlFirebird());
            }

            if (cancelado) { concluido(false, "Cancelado."); return; }

            // ── 4. Montar contexto ────────────────────────────────────────────
            log("[4/5] Montando contexto...");
            MigracaoContext ctx = new MigracaoContext(config);
            ctx.setOrigemConn(config.isModoSyspdv() ? destinoConn : origemConn);
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
            // No modo SQL Output, origemConn pode ser null aqui (Firebird aberto acima).
            // Nesse caso, usa DESCONHECIDA: FabricaMigracao roteia pelo campo sistema.
            VersaoFirebird versao;
            if (origemConn != null) {
                log("[PRÉ] Detectando versão do Firebird...");
                versao = DetectorFirebird.detectar(origemConn);
                log("      Versão detectada: " + versao);
            } else {
                versao = VersaoFirebird.DESCONHECIDA;
                log("[PRÉ] Modo SQL Output — versão Firebird: DESCONHECIDA (será roteada pelo sistema)");
            }

            if (cancelado) { concluido(false, "Cancelado."); return; }

            // ── 5. Executar migração adaptada ─────────────────────────────────
            // FabricaMigracao → AdaptadorMigracao → ExecutorMigracao (com fallback)
            log("[5/5] Iniciando migração adaptada para " + versao + "...");

            final AdaptadorMigracao.CancelCheck cancelCheck = new AdaptadorMigracao.CancelCheck() {
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

            Connection execOrigem = config.isModoSyspdv() ? destinoConn : origemConn;
            ExecutorMigracao.executar(execOrigem, migradores, execLogCallback);

            // ── Escrever dump SQL se modo SQL Output ──────────────────────────
            if (config.isModoSqlOutput() && sqlWriter != null && destinoConn != null) {
                log("[SQL] Gerando arquivo dump final no padrão MySQL 5.5.38...");
                sqlWriter.writeFromConnection(destinoConn);
                log("[SQL] Dump gerado com sucesso em: " + config.getSqlOutputPath());
            }

            // ── Resumo final ──────────────────────────────────────────────────
            log("=================================================");
            log("  Migração concluída com sucesso!");
            for (MigracaoContext.StepStats s : ctx.getStats().values()) {
                log(String.format("    %-25s %s",
                    s.nome, s.erros == 0 ? "OK" : "Com erros (" + s.erros + ")"));
            }
            if (config.isModoSqlOutput()) {
                log("  Dump SQL: " + config.getSqlOutputPath());
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

        // syspdv SEMPRE usa Firebird 2.5 (ODS 11.2) — faz upgrade antes de tentar conectar
        if ("syspdv".equalsIgnoreCase(config.getSistema())) {
            log("[syspdv] Banco Firebird 2.5 detectado — executando upgrade ODS antes de conectar...");
            tentarGfixUpgrade(config.getFbArquivo(), logCallback);
        }

        // Máximo de trocas de versão (nunca trava em loop infinito)
        final int MAX_TENTATIVAS_ODS = 5;
        boolean gfixTentado = "syspdv".equalsIgnoreCase(config.getSistema()); // já tentou acima

        for (int tentativa = 1; tentativa <= MAX_TENTATIVAS_ODS; tentativa++) {
            try {
                return conectarFirebird();

            } catch (SQLException e) {

                // Tenta identificar se é erro de ODS incompatível
                VersaoFirebird versaoNecessaria =
                    GerenciadorFirebird.detectarVersaoPorODS(e.getMessage());

                if (versaoNecessaria != VersaoFirebird.DESCONHECIDA
                        && tentativa < MAX_TENTATIVAS_ODS) {

                    // ── ODS incompatível: tentar gfix -upgrade antes de trocar binário ──
                    if (!gfixTentado) {
                        gfixTentado = true;
                        log("⚠ ODS incompatível (" + versaoNecessaria + ") — tentando gfix -upgrade...");
                        if (tentarGfixUpgrade(config.getFbArquivo(), logCallback)) {
                            log("   gfix OK — retentando conexão...");
                            continue;
                        }
                    }

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

    /**
     * Patch direto no header do .fdb para upgrade de ODS 11.2 → 12.0.
     *
     * Layout do header page (Firebird 2.5/3.0, little-endian):
     *   offset  0-15: pag header (type, flags, generation, scn)
     *   offset 16-19: hdr_page_size (ULONG)
     *   offset 20-21: hdr_ods_version (USHORT)  ← 11 → 12
     *   offset 62-63: hdr_ods_minor (USHORT)     ← 2  →  0
     *   offset 64-65: hdr_ods_minor_original     ← 2  →  0
     *
     * Nota: gfix -upgrade não existe nos pacotes Ubuntu 22.04;
     * este patch Java faz o equivalente.
     */
    private boolean tentarGfixUpgrade(String dbPath, GerenciadorFirebird.LogCallback logCallback) {
        try {
            java.io.RandomAccessFile raf = new java.io.RandomAccessFile(dbPath, "rw");

            byte[] header = new byte[128];
            raf.seek(0);
            int read = raf.read(header);
            if (read < 66) {
                raf.close();
                log("[ods] Arquivo muito pequeno para ser um banco Firebird válido");
                return false;
            }

            // Lê ODS atual (little-endian)
            int odsMajor = (header[20] & 0xFF) | ((header[21] & 0xFF) << 8);
            int odsMinor = (header[62] & 0xFF) | ((header[63] & 0xFF) << 8);
            log("[ods] ODS detectado: " + odsMajor + "." + odsMinor);

            if (odsMajor == 12) {
                raf.close();
                log("[ods] Já é ODS 12 — sem necessidade de patch");
                return true;
            }
            if (odsMajor != 11) {
                raf.close();
                log("[ods] ODS " + odsMajor + " desconhecido — patch não aplicado");
                return false;
            }

            // Patch: ODS major 11 → 12
            header[20] = 12;
            header[21] = 0;
            // Patch: ODS minor 2 → 0
            header[62] = 0;
            header[63] = 0;
            // Patch: ODS minor original → 0
            header[64] = 0;
            header[65] = 0;

            raf.seek(0);
            raf.write(header, 0, 128);
            raf.close();

            log("[ods] Patch aplicado: ODS " + odsMajor + "." + odsMinor + " → 12.0");
            return true;

        } catch (Exception e) {
            log("[ods] Erro no patch ODS: " + e.getMessage());
            return false;
        }
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
