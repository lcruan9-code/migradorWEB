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
        String         h2DbDir  = null;  // para limpeza no finally

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
                
                // H2 FILE-BACKED: dados em /tmp para não inflar o heap Java.
                // In-memory explodia o undo buffer com 5347 produtos × 400+ UPDATEs.
                String h2DbId = java.util.UUID.randomUUID().toString().replace("-", "");
                h2DbDir = "/tmp/h2-" + h2DbId;
                new java.io.File(h2DbDir).mkdirs();
                String h2Url = "jdbc:h2:file:" + h2DbDir + "/db"
                    + ";MODE=MySQL;DATABASE_TO_UPPER=FALSE;CASE_INSENSITIVE_IDENTIFIERS=TRUE"
                    + ";CACHE_SIZE=16384;DB_CLOSE_ON_EXIT=FALSE";
                log("[H2] Banco FILE-BACKED: " + h2DbDir + "/db");
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

                // syspdv usa ODS 11.2 (FB 2.5) — converter para ODS 12.2 antes de conectar
                if ("syspdv".equalsIgnoreCase(config.getSistema())) {
                    log("[2.5/5] syspdv detectado: convertendo banco ODS 11.2 → 12.2 via gbak...");
                    realizarGbakConversao(logCallback);
                }

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

                // ── Injetar UPDATE CNPJ no final do arquivo ───────────────────
                String cnpjDestino = config.getCnpjDestino();
                if (cnpjDestino != null && !cnpjDestino.isEmpty()) {
                    try {
                        java.nio.file.Path sqlPath = Paths.get(config.getSqlOutputPath());
                        String updateSql = "\n-- CNPJ da empresa destino\n"
                            + "UPDATE `empresa` SET `cnpj` = '"
                            + cnpjDestino.replace("'", "''") + "' WHERE `id` = 1;\n";
                        java.nio.file.Files.write(sqlPath,
                            updateSql.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                            java.nio.file.StandardOpenOption.APPEND);
                        log("[SQL] UPDATE empresa.cnpj='" + cnpjDestino + "' adicionado ao final do arquivo.");
                    } catch (Exception eCnpj) {
                        log("[SQL] AVISO: não foi possível adicionar UPDATE CNPJ: " + eCnpj.getMessage());
                    }
                }
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
            // Limpa arquivos temporários do H2 file-backed
            if (h2DbDir != null) {
                deletarDiretorio(new java.io.File(h2DbDir));
                log("[H2] Arquivos temporários removidos: " + h2DbDir);
            }
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
     * Conecta ao banco Firebird.
     *
     * <p>Para syspdv, {@link #realizarGbakConversao} já foi chamado antes deste método,
     * convertendo o banco de ODS 11.2 (FB 2.5) para ODS 12.2 (FB 3.0) via gbak embedded.
     * Todos os sistemas conectam ao Firebird 3.0 na porta 3050 com Legacy_Auth.
     */
    private Connection conectarOrigemComAutoRetry(
            GerenciadorFirebird.LogCallback logCallback) throws Exception {
        return conectarFirebird();
    }

    // ── Conversão ODS 11.2 → 12.2 via gbak (para syspdv / Firebird 2.5) ─────────

    /**
     * Usa gbak25 com libfbembed (FB 2.5 embedded) para fazer backup do banco ODS 11.2,
     * depois restaura via FB 3.0 criando um banco ODS 12.2 que Jaybird consegue abrir.
     * Atualiza config.fbArquivo para apontar ao banco restaurado.
     */
    private void realizarGbakConversao(GerenciadorFirebird.LogCallback logCallback)
            throws Exception {

        String origPath = config.getFbArquivo();
        // Remove prefixo host: caso venha "localhost:/path" — gbak embedded usa caminho direto
        if (origPath.contains(":")) {
            origPath = origPath.substring(origPath.lastIndexOf(':') + 1);
        }

        String fbkPath      = origPath + ".bak.fbk";
        String restoredPath = origPath + ".fb30.fdb";

        // Limpa arquivos de tentativas anteriores
        new java.io.File(fbkPath).delete();
        new java.io.File(restoredPath).delete();

        // ── Passo 1: backup via gbak25 + libfbembed (lê ODS 11.2 sem server) ────
        log("[gbak] 1/2 — backup ODS 11.2 com FB 2.5 embedded");
        log("[gbak]   entrada : " + origPath);
        log("[gbak]   backup  : " + fbkPath);
        {
            // Sem -user/-password: modo embedded com root usa credenciais do OS
            // (root = SYSDBA no embedded) e bypassa validação contra security2.fdb.
            // ISC_USER/ISC_PASSWORD também omitidos pelo mesmo motivo.
            ProcessBuilder pb = new ProcessBuilder(
                "/opt/fb25/bin/gbak25",
                "-b",
                origPath, fbkPath
            );
            pb.environment().put("LD_LIBRARY_PATH", "/opt/fb25/lib");
            pb.environment().put("FIREBIRD",         "/opt/fb25");
            pb.environment().put("FIREBIRD_TMP",     "/tmp");
            pb.environment().put("HOME",             "/tmp");
            pb.redirectErrorStream(true);

            Process proc = pb.start();
            String out = new String(proc.getInputStream().readAllBytes(),
                java.nio.charset.StandardCharsets.UTF_8);
            int exit = proc.waitFor();

            if (!out.isEmpty()) {
                String trimmed = out.length() > 1500
                    ? out.substring(0, 1000) + "\n...\n" + out.substring(out.length() - 400)
                    : out;
                log("[gbak] " + trimmed.trim());
            }

            if (exit != 0 || !new java.io.File(fbkPath).exists()) {
                throw new Exception(
                    "gbak25 -backup falhou (exit=" + exit + "). "
                    + "Verifique se /opt/fb25/bin/gbak25 existe e o arquivo .fdb é acessível.");
            }
            log("[gbak] ✅ Backup gerado: " + new java.io.File(fbkPath).length() + " bytes");
        }

        // ── Passo 2: restaurar via gbak FB 3.0 (cria ODS 12.2) ──────────────────
        log("[gbak] 2/2 — restaurando como ODS 12.2 via FB 3.0");
        log("[gbak]   restaurado: " + restoredPath);
        {
            // FB 3.0 server roda como user 'firebird'. O diretório chunks-XXXX foi
            // criado pelo Java (root) com permissão 755. Precisamos de 777 para que
            // o server possa criar o arquivo restaurado lá.
            try {
                String parentDir = new java.io.File(restoredPath).getParent();
                new ProcessBuilder("chmod", "777", parentDir)
                    .start().waitFor();
                log("[gbak] chmod 777 " + parentDir);
            } catch (Exception ex) {
                log("[gbak] AVISO: não foi possível chmod no diretório pai: " + ex.getMessage());
            }

            ProcessBuilder pb = new ProcessBuilder(
                "/usr/bin/gbak",
                "-create",
                "-replace",
                "-user",     "SYSDBA",
                "-password", "masterkey",
                fbkPath,
                "localhost:" + restoredPath
            );
            pb.redirectErrorStream(true);

            Process proc = pb.start();
            String out = new String(proc.getInputStream().readAllBytes(),
                java.nio.charset.StandardCharsets.UTF_8);
            int exit = proc.waitFor();

            if (!out.isEmpty()) {
                String trimmed = out.length() > 1500
                    ? out.substring(0, 1000) + "\n...\n" + out.substring(out.length() - 400)
                    : out;
                log("[gbak] " + trimmed.trim());
            }

            if (exit != 0 || !new java.io.File(restoredPath).exists()) {
                throw new Exception("gbak -create falhou (exit=" + exit + ")");
            }
            log("[gbak] ✅ Banco restaurado: " + new java.io.File(restoredPath).length() + " bytes");
        }

        // ── Aponta config para o banco ODS 12.2 ──────────────────────────────────
        config.setFbArquivo(restoredPath);

        // Libera espaço: remove o backup intermediário (.fbk) e o FDB original
        // para reduzir pressão de memória/disco durante a migração
        boolean fbkDel = new java.io.File(fbkPath).delete();
        log("[gbak] backup .fbk removido: " + fbkDel + " (liberando ~" +
            (new java.io.File(fbkPath).exists() ? "0" : "118") + " MB)");

        log("[gbak] ✅ Conversão concluída — conectando via FB 3.0 (ODS 12.2)");
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

    /** Remove recursivamente um diretório e todo seu conteúdo (cleanup H2 temp). */
    private void deletarDiretorio(java.io.File dir) {
        if (dir == null || !dir.exists()) return;
        java.io.File[] files = dir.listFiles();
        if (files != null) {
            for (java.io.File f : files) {
                if (f.isDirectory()) deletarDiretorio(f);
                else f.delete();
            }
        }
        dir.delete();
    }
}
