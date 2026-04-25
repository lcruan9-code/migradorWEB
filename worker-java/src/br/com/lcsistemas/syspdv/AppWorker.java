package br.com.lcsistemas.syspdv;

import br.com.lcsistemas.syspdv.config.MigracaoConfig;
import br.com.lcsistemas.syspdv.engine.MigracaoEngine;
import br.com.lcsistemas.syspdv.firebird.GerenciadorFirebird;
import br.com.lcsistemas.syspdv.sql.ReferenceData;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * Servidor HTTP local para o Portal de Migração LC.
 * Porta 8080. Recebe upload do .FDB, roda a engine e serve o .sql gerado.
 *
 * POST /api/processar   — multipart com campo "sistema" + "arquivo" (.fdb/.fbk)
 * GET  /api/status/:id  — status, progresso e logs do job
 * GET  /api/download/:id — download do TabelasParaImportacao.sql
 */
public class AppWorker {

    private static final int    PORT = resolvePort();
    private static final ObjectMapper JSON = new ObjectMapper();

    static final Map<String, JobState> JOBS      = new ConcurrentHashMap<>();
    static final Map<String, Path>     CHUNK_DIRS = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/health",       new HealthHandler());
        server.createContext("/diag",         new DiagHandler());
        server.createContext("/api/processar", new ProcessarHandler());
        server.createContext("/api/status",    new StatusHandler());
        server.createContext("/api/download",  new DownloadHandler());
        server.createContext("/api/cidades",   new CidadesHandler());
        server.createContext("/api/estados",   new EstadosHandler());
        server.createContext("/api/chunk",     new ChunkHandler());
        server.createContext("/api/finalize",  new FinalizeHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("[AppWorker] Rodando em http://localhost:" + PORT);
        System.out.println("[AppWorker] Aguardando jobs do portal...");

        // Pré-aquece o Firebird em background para evitar a primeira migração ficar
        // parada aguardando subida/verificação do serviço.
        Thread warmup = new Thread(() -> {
            try {
                MigracaoConfig warmConfig = new MigracaoConfig();
                String baseDir = System.getProperty("user.dir");
                warmConfig.setFbHost("localhost");
                warmConfig.setFbPorta("3050");
                warmConfig.setPastaFirebird(new java.io.File(baseDir, "FIREBIRD").getAbsolutePath());
                GerenciadorFirebird.garantirConectividade(warmConfig, msg -> System.out.println("[AppWorker] " + msg));
            } catch (Exception e) {
                System.out.println("[AppWorker] Warmup Firebird ignorado: " + e.getMessage());
            }
        }, "firebird-warmup");
        warmup.setDaemon(true);
        warmup.start();
    }

    private static int resolvePort() {
        String raw = System.getenv("PORT");
        if (raw == null || raw.trim().isEmpty()) {
            return 8080;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return 8080;
        }
    }

    // =========================================================================
    //  GET /health
    // =========================================================================

    static class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            addCors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(204, -1); return;
            }
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
                respond(ex, 405, err("Método não permitido")); return;
            }
            respond(ex, 200, "{\"ok\":true,\"build\":\"2025-04-25-gbak-v2\"}");
        }
    }

    // =========================================================================
    //  GET /diag  — diagnóstico do ambiente (Firebird, binários, portas)
    // =========================================================================

    static class DiagHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            addCors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(204, -1); return;
            }
            StringBuilder sb = new StringBuilder();
            sb.append("=== DIAG AppWorker ===\n\n");

            sb.append("--- Resolução de localhost ---\n");
            try {
                java.net.InetAddress[] addrs = java.net.InetAddress.getAllByName("localhost");
                for (java.net.InetAddress a : addrs) sb.append("  " + a + "\n");
            } catch (Exception e) {
                sb.append("  erro: " + e.getMessage() + "\n");
            }

            sb.append("\n--- Porta 3050 ---\n");
            // IPv4 explícito
            try (java.net.Socket s = new java.net.Socket()) {
                s.connect(new java.net.InetSocketAddress("127.0.0.1", 3050), 1000);
                sb.append("127.0.0.1:3050 = ABERTA\n");
            } catch (Exception e) {
                sb.append("127.0.0.1:3050 = FECHADA (" + e.getMessage() + ")\n");
            }
            // IPv6 explícito
            try (java.net.Socket s = new java.net.Socket()) {
                s.connect(new java.net.InetSocketAddress("::1", 3050), 1000);
                sb.append("::1:3050 = ABERTA\n");
            } catch (Exception e) {
                sb.append("::1:3050 = FECHADA (" + e.getMessage() + ")\n");
            }
            // localhost genérico
            try (java.net.Socket s = new java.net.Socket()) {
                s.connect(new java.net.InetSocketAddress("localhost", 3050), 1000);
                sb.append("localhost:3050 = ABERTA\n");
            } catch (Exception e) {
                sb.append("localhost:3050 = FECHADA (" + e.getMessage() + ")\n");
            }

            sb.append("\n--- /proc/net/tcp (porta 3050 = 0BEA hex) ---\n");
            try (java.io.BufferedReader br = new java.io.BufferedReader(
                    new java.io.FileReader("/proc/net/tcp"))) {
                String line;
                sb.append(br.readLine() + "\n"); // header
                while ((line = br.readLine()) != null) {
                    if (line.contains(":0BEA") || line.contains(":0bea")) {
                        sb.append(line + "\n");
                    }
                }
            } catch (Exception e) {
                sb.append("/proc/net/tcp error: " + e.getMessage() + "\n");
            }
            try (java.io.BufferedReader br = new java.io.BufferedReader(
                    new java.io.FileReader("/proc/net/tcp6"))) {
                String line;
                sb.append("\n/proc/net/tcp6 (porta 3050):\n");
                sb.append(br.readLine() + "\n");
                while ((line = br.readLine()) != null) {
                    if (line.contains(":0BEA") || line.contains(":0bea")) {
                        sb.append(line + "\n");
                    }
                }
            } catch (Exception e) {
                sb.append("/proc/net/tcp6 error: " + e.getMessage() + "\n");
            }

            sb.append("\n--- Binários Firebird ---\n");
            for (String p : new String[]{
                    "/usr/sbin/firebird", "/usr/sbin/fbguard", "/usr/sbin/fb_inet_server",
                    "/usr/lib/firebird/3.0/bin/fbguard", "/usr/lib/firebird/3.0/bin/fb_inet_server",
                    "/usr/bin/gbak", "/usr/bin/isql-fb", "/usr/bin/gsec"}) {
                java.io.File f = new java.io.File(p);
                if (f.exists()) sb.append(p + " [" + (f.canExecute() ? "exec" : "no-exec") + "]\n");
            }

            sb.append("\n--- /app/FIREBIRD ---\n");
            appendDir(sb, new java.io.File("/app/FIREBIRD"), 3, "");

            sb.append("\n--- Security DB ---\n");
            for (String p : new String[]{
                    "/var/lib/firebird/3.0/system/security3.fdb",
                    "/var/lib/firebird/3.0/security/security3.fdb",
                    "/var/lib/firebird/3.0/security3.fdb"}) {
                java.io.File f = new java.io.File(p);
                sb.append(p + ": " + (f.exists() ? "OK (" + f.length() + " bytes)" : "MISSING") + "\n");
            }

            sb.append("\n--- Processos ---\n");
            try {
                Process proc = new ProcessBuilder("ps", "aux").redirectErrorStream(true).start();
                java.io.BufferedReader br = new java.io.BufferedReader(
                        new java.io.InputStreamReader(proc.getInputStream()));
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.toLowerCase().contains("firebird") || line.contains("fbguard")
                            || line.contains("fb_inet") || line.contains("fbserver")) {
                        sb.append(line + "\n");
                    }
                }
                proc.waitFor();
            } catch (Exception e) {
                sb.append("ps error: " + e.getMessage() + "\n");
            }

            sb.append("\n--- init.d status ---\n");
            try {
                Process proc = new ProcessBuilder("/etc/init.d/firebird3.0", "status")
                        .redirectErrorStream(true).start();
                java.io.BufferedReader br = new java.io.BufferedReader(
                        new java.io.InputStreamReader(proc.getInputStream()));
                String line;
                while ((line = br.readLine()) != null) sb.append(line + "\n");
                proc.waitFor();
            } catch (Exception e) {
                sb.append("init.d error: " + e.getMessage() + "\n");
            }

            sb.append("\n--- Sockets TCP ouvindo ---\n");
            for (String[] cmd : new String[][]{{"ss", "-tlnp"}, {"netstat", "-tlnp"}}) {
                try {
                    Process proc = new ProcessBuilder(cmd).redirectErrorStream(true).start();
                    java.io.BufferedReader br = new java.io.BufferedReader(
                            new java.io.InputStreamReader(proc.getInputStream()));
                    String line;
                    boolean found = false;
                    while ((line = br.readLine()) != null) {
                        sb.append(line + "\n");
                        found = true;
                    }
                    proc.waitFor();
                    if (found) break;
                } catch (Exception e) {
                    sb.append(cmd[0] + " error: " + e.getMessage() + "\n");
                }
            }

            sb.append("\n--- Firebird log ---\n");
            for (String logPath : new String[]{
                    "/var/log/firebird/firebird3.0.log",
                    "/var/lib/firebird/3.0/firebird.log",
                    "/tmp/firebird.log"}) {
                java.io.File f = new java.io.File(logPath);
                if (f.exists()) {
                    sb.append("=== " + logPath + " (last 30 lines) ===\n");
                    try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(f, "r")) {
                        long len = raf.length();
                        long pos = Math.max(0, len - 4096);
                        raf.seek(pos);
                        byte[] buf = new byte[(int)(len - pos)];
                        raf.readFully(buf);
                        String[] lines = new String(buf, StandardCharsets.UTF_8).split("\n");
                        int start = Math.max(0, lines.length - 30);
                        for (int i = start; i < lines.length; i++) sb.append(lines[i] + "\n");
                    } catch (Exception e) {
                        sb.append("read error: " + e.getMessage() + "\n");
                    }
                    break;
                } else {
                    sb.append(logPath + ": MISSING\n");
                }
            }

            sb.append("\n--- Teste Auth SYSDBA (Jaybird) ---\n");
            try { Class.forName("org.firebirdsql.jdbc.FBDriver"); } catch (Exception ignore) {}
            String[] testPasswords = {"masterkey", "", "masterke", "sysdba", "SYSDBA"};
            String[] authPlugins   = {"Srp256", "Legacy_Auth"};
            for (String plugin : authPlugins) {
                for (String pass : testPasswords) {
                    String testUrl = "jdbc:firebirdsql://localhost:3050/tmp/auth_test_"
                        + System.currentTimeMillis() + ".fdb?authPlugins=" + plugin + "&wireCrypt=DISABLED";
                    try {
                        java.sql.DriverManager.setLoginTimeout(3);
                        java.sql.Connection tc = java.sql.DriverManager.getConnection(testUrl, "SYSDBA", pass);
                        tc.close();
                        sb.append("[" + plugin + "] senha='" + pass + "' → AUTH+DB OK\n");
                    } catch (Exception e) {
                        String msg = e.getMessage() == null ? "" : e.getMessage();
                        boolean authFail = msg.contains("335544472") || msg.contains("not defined") || msg.contains("isc_login");
                        sb.append("[" + plugin + "] senha='" + pass + "' → " +
                            (authFail ? "AUTH FAIL" : "Auth OK / DB err: " + msg.substring(0, Math.min(100, msg.length()))) + "\n");
                    }
                }
            }

            sb.append("\n--- firebird.conf (sem comentários) ---\n");
            try (java.io.BufferedReader br = new java.io.BufferedReader(
                    new java.io.FileReader("/etc/firebird/3.0/firebird.conf"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String t = line.trim();
                    if (!t.isEmpty() && !t.startsWith("#")) sb.append(line + "\n");
                }
            } catch (Exception e) {
                sb.append("firebird.conf error: " + e.getMessage() + "\n");
            }

            byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
            ex.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
        }

        private void appendDir(StringBuilder sb, java.io.File dir, int depth, String indent) {
            if (!dir.exists()) { sb.append(indent + dir.getAbsolutePath() + ": NOT FOUND\n"); return; }
            java.io.File[] files = dir.listFiles();
            if (files == null) return;
            for (java.io.File f : files) {
                sb.append(indent + f.getName() + (f.isDirectory() ? "/" : "") + "\n");
                if (f.isDirectory() && depth > 1) appendDir(sb, f, depth - 1, indent + "  ");
            }
        }
    }

    // =========================================================================
    //  POST /api/processar  (multipart/form-data)
    // =========================================================================

    static class ProcessarHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            addCors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(204, -1); return;
            }
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                respond(ex, 405, err("Método não permitido")); return;
            }

            try {
                String contentType = ex.getRequestHeaders().getFirst("Content-Type");
                String boundary    = extractBoundary(contentType);

                // Cria workdir antes do parse para que o streaming grave direto nele.
                // No Linux/Docker: /app/fdb-work (chmod 777) — acessível pelo Firebird user.
                // No Windows: temp padrão do sistema.
                java.nio.file.Path workBase = resolverWorkDir();
                Path tmpDir = Files.createTempDirectory(workBase, "migrador-");
                tmpDir.toFile().setReadable(true, false);
                tmpDir.toFile().setExecutable(true, false);

                // Streaming multipart: grava o .fdb direto em disco — sem readAllBytes() em memória.
                MultipartData data = parseMultipartStreaming(ex.getRequestBody(), boundary, tmpDir);
                String sistema  = data.sistema  != null ? data.sistema  : "syspdv";
                String filename = data.filename != null ? data.filename : "banco.fdb";

                if (data.fileStreamPath == null) {
                    respond(ex, 400, err("Arquivo não recebido")); return;
                }

                Path uploadPath = java.nio.file.Paths.get(data.fileStreamPath);
                if (!Files.exists(uploadPath) || Files.size(uploadPath) == 0) {
                    respond(ex, 400, err("Arquivo vazio ou não recebido")); return;
                }
                uploadPath.toFile().setReadable(true, false);

                String jobId = UUID.randomUUID().toString();
                JobState job = new JobState(jobId, tmpDir);
                JOBS.put(jobId, job);

                // Se for .FBK, restaura para .FDB antes de migrar
                Path fdbPath;
                String lname = filename.toLowerCase();
                if (lname.endsWith(".fbk") || lname.endsWith(".gbk")) {
                    String fdbName = filename.substring(0, filename.lastIndexOf('.')) + ".fdb";
                    Path restoredPath = tmpDir.resolve(fdbName);
                    job.addLog("[Worker] Arquivo .FBK detectado — iniciando restauração gbak...");
                    restaurarFbk(uploadPath, restoredPath, job);
                    fdbPath = restoredPath;
                } else {
                    fdbPath = uploadPath;
                }

                iniciarMigracao(jobId, job, sistema, fdbPath.toString(), data);

                ObjectNode resp = JSON.createObjectNode();
                resp.put("jobId", jobId);
                respond(ex, 200, JSON.writeValueAsString(resp));

            } catch (Exception e) {
                e.printStackTrace();
                respond(ex, 500, err(e.getMessage()));
            }
        }
    }

    // =========================================================================
    //  GET /api/status/{jobId}
    // =========================================================================

    static class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            addCors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(204, -1); return;
            }
            String jobId = lastSegment(ex.getRequestURI().getPath());
            JobState job = JOBS.get(jobId);
            if (job == null) { respond(ex, 404, err("Job não encontrado")); return; }

            ObjectNode resp = JSON.createObjectNode();
            resp.put("jobId",     job.id);
            resp.put("status",    job.status);
            resp.put("progresso", job.progresso);
            resp.put("total",     job.total);
            ArrayNode logsArr = resp.putArray("logs");
            synchronized (job.logs) { for (String l : job.logs) logsArr.add(l); }
            respond(ex, 200, JSON.writeValueAsString(resp));
        }
    }

    // =========================================================================
    //  GET /api/download/{jobId}
    // =========================================================================

    static class DownloadHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            addCors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(204, -1); return;
            }

            String jobId = lastSegment(ex.getRequestURI().getPath());
            JobState job = JOBS.get(jobId);

            if (job == null || job.sqlPath == null || !Files.exists(job.sqlPath)) {
                respond(ex, 404, err("Arquivo SQL não disponível. Certifique-se que a migração concluiu.")); return;
            }

            byte[] bytes = Files.readAllBytes(job.sqlPath);
            ex.getResponseHeaders().add("Content-Type", "application/octet-stream");
            ex.getResponseHeaders().add("Content-Disposition", "attachment; filename=\"TabelasParaImportacao.sql\"");
            ex.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
        }
    }

    // =========================================================================
    //  Restauração de .FBK → .FDB via gbak bundled
    // =========================================================================

    /**
     * Localiza o gbak bundled na pasta FIREBIRD/ relativa ao diretório da aplicação.
     * Detecta o OS para usar o nome correto do binário (.exe no Windows, sem extensão no Linux).
     * A pasta FIREBIRD/ com todos os binários é responsabilidade do deployment.
     *
     * Retorna [caminho_gbak, caminho_fbhome].
     */
    private static String[] resolverGbak() throws Exception {
        String baseDir   = System.getProperty("user.dir");
        boolean isWin    = System.getProperty("os.name", "").toLowerCase().contains("win");
        String  binName  = isWin ? "gbak.exe" : "gbak";

        // 1. Verifica paths do sistema (Linux/Docker com Firebird instalado via apt)
        if (!isWin) {
            String[] systemPaths = { "/usr/bin/gbak", "/usr/local/bin/gbak", "/usr/sbin/gbak" };
            for (String sp : systemPaths) {
                java.nio.file.Path p = java.nio.file.Paths.get(sp);
                if (Files.exists(p)) {
                    // fbHome = diretório pai de 'bin' (ex: /usr → usado como FIREBIRD env)
                    String fbHome = p.getParent().getParent().toAbsolutePath().toString();
                    return new String[]{ p.toAbsolutePath().toString(), fbHome };
                }
            }
        }

        // 2. Procura na pasta FIREBIRD/ bundled (ambiente Windows local)
        String[] versoes = {
            "Firebird-2.5.9.manual",
            "Firebird-2.5.manual",
            "Firebird-3.0.manual",
            "Firebird-4.0.manual"
        };
        for (String versao : versoes) {
            java.nio.file.Path p = java.nio.file.Paths.get(baseDir, "FIREBIRD", versao, "bin", binName);
            if (Files.exists(p)) {
                String fbHome = p.getParent().getParent().toAbsolutePath().toString();
                return new String[]{ p.toAbsolutePath().toString(), fbHome };
            }
        }

        throw new Exception(
            "gbak não encontrado. No Docker: instale firebird3.0-server. " +
            "No Windows: coloque a pasta FIREBIRD/ em " + baseDir);
    }

    private static java.nio.file.Path resolverWorkDir() {
        boolean isWin = System.getProperty("os.name", "").toLowerCase().contains("win");
        if (!isWin) {
            // No Linux/Docker: usa /app/fdb-work (chmod 777) para que o processo
            // Firebird (user 'firebird') consiga abrir o arquivo enviado pelo usuário.
            java.nio.file.Path linuxWork = java.nio.file.Paths.get("/app/fdb-work");
            if (Files.exists(linuxWork) && linuxWork.toFile().canWrite()) {
                return linuxWork;
            }
        }
        return java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"));
    }

    private static void restaurarFbk(Path fbkPath, Path fdbPath, JobState job)
            throws Exception {

        String[] gbakInfo = resolverGbak();
        String gbakExe = gbakInfo[0];
        String fbHome  = gbakInfo[1];

        job.addLog("[gbak] Usando: " + gbakExe);
        job.addLog("[gbak] Origem: " + fbkPath.getFileName());
        job.addLog("[gbak] Destino: " + fdbPath.getFileName());

        ProcessBuilder pb = new ProcessBuilder(
            gbakExe,
            "-create",
            fbkPath.toAbsolutePath().toString(),
            fdbPath.toAbsolutePath().toString(),
            "-user",     "SYSDBA",
            "-password", "masterkey"
        );
        pb.environment().put("FIREBIRD", fbHome);
        pb.redirectErrorStream(true);

        Process proc = pb.start();
        try (java.io.BufferedReader br = new java.io.BufferedReader(
                new java.io.InputStreamReader(proc.getInputStream()))) {
            String line;
            while ((line = br.readLine()) != null) {
                job.addLog("[gbak] " + line);
            }
        }

        int exitCode = proc.waitFor();
        if (exitCode != 0) {
            throw new Exception("gbak falhou com código " + exitCode
                + ". Verifique os logs acima.");
        }

        if (!Files.exists(fdbPath) || Files.size(fdbPath) == 0) {
            throw new Exception("gbak concluiu mas o .fdb não foi criado em: " + fdbPath);
        }

        job.addLog("[gbak] Restauração concluída — "
            + String.format("%.1f MB", Files.size(fdbPath) / 1_048_576.0));
    }

    // =========================================================================
    //  Engine em thread de background
    // =========================================================================

    private static void iniciarMigracao(String jobId, JobState job,
                                        String sistema, String fdbPath,
                                        MultipartData data) {
        Thread t = new Thread(() -> {
            job.status = "PROCESSANDO";
            job.addLog("[Worker] Job " + jobId.substring(0, 8) + " — sistema=" + sistema);
            job.addLog("[Worker] Arquivo: " + fdbPath);

            try {
                MigracaoConfig config = buildConfig(sistema, fdbPath, job, data);
                Path sqlOut = job.tmpDir.resolve("TabelasParaImportacao.sql");
                config.setSqlOutputPath(sqlOut.toString());

                MigracaoEngine engine = new MigracaoEngine(config);
                engine.setListener(new MigracaoEngine.ProgressListener() {
                    @Override public void onLog(String msg) { job.addLog(msg); }
                    @Override public void onStepInicio(String nome, int atual, int total) {
                        job.progresso = atual; job.total = total;
                    }
                    @Override public void onStepConcluido(String nome, int ins, int ign, int err, boolean ok) {
                        job.addLog("[" + nome + "] " + (ok ? "✓" : "✗")
                            + " ins=" + ins + " ign=" + ign + " err=" + err);
                    }
                    @Override public void onConcluido(boolean ok, String msg) {
                        job.status = ok ? "CONCLUIDO" : "ERRO";
                        if (ok) job.sqlPath = sqlOut;
                        job.addLog("[Worker] " + msg);
                    }
                });

                engine.executar();

            } catch (Exception e) {
                job.status = "ERRO";
                job.addLog("[Worker] ERRO: " + e.getMessage());
            }
        }, "migrador-" + jobId.substring(0, 8));
        t.setDaemon(true);
        t.start();
    }

    private static MigracaoConfig buildConfig(String sistema, String fdbPath, JobState job,
                                               MultipartData data) {
        MigracaoConfig c = new MigracaoConfig();
        c.setFbArquivo(fdbPath);
        c.setFbHost("localhost");
        c.setFbPorta("3050");
        c.setFbUsuario("SYSDBA");
        c.setFbSenha("masterkey");

        c.setMyHost("localhost");
        c.setMyPorta("3306");
        c.setMyDatabase("lc_sistemas");
        c.setMyUsuario("root");
        c.setMySenha("");

        c.setSistema(sistema);
        // Modo web: todos os sistemas usam Firebird como origem via arquivo .fdb.
        // modoSyspdv=false garante que a engine conecte ao Firebird (não MySQL) como origem.
        c.setModoSyspdv(false);

        // Pasta com as instalações bundled do Firebird (gbak, fbserver, etc.)
        String baseDir = System.getProperty("user.dir");
        c.setPastaFirebird(new java.io.File(baseDir, "FIREBIRD").getAbsolutePath());

        // UF, cidade e regime do formulário
        String uf     = data.uf     != null ? data.uf.trim().toUpperCase()     : "PA";
        String cidade = data.cidade != null ? data.cidade.trim()               : "";
        String regime = data.regime != null ? data.regime.trim().toUpperCase() : "SIMPLES";

        c.setClienteUf(uf);
        c.setRegimeTributario(regime);

        // Resolve id_estado pelo UF
        int idEstado = ReferenceData.getEstadoId(uf);
        c.setEstadoDefaultId(idEstado);

        // Resolve id_cidade pelo nome e iduf do estado
        if (!cidade.isEmpty() && idEstado > 0) {
            // Precisa do iduf do estado (campo iduf em ESTADOS)
            String idufStr = "";
            for (String[] e : ReferenceData.getEstados())
                if (e[1].equalsIgnoreCase(uf)) { idufStr = e[3]; break; }
            int idCidade = ReferenceData.getCidadeId(idufStr, cidade);
            c.setCidadeDefaultId(idCidade);
        }

        job.addLog("[Config] UF=" + uf + " | Cidade=" + cidade + " | Regime=" + regime
            + " | id_estado=" + c.getEstadoDefaultId() + " | id_cidade=" + c.getCidadeDefaultId());
        job.addLog("[Config] Sistema=" + sistema + " | Arquivo=" + fdbPath
            + " | pastaFB=" + c.getPastaFirebird());
        return c;
    }

    // =========================================================================
    //  GET /api/estados  — lista todos os estados
    //  GET /api/cidades?uf=XX  — cidades de um estado
    // =========================================================================

    static class EstadosHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            addCors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(204, -1); return;
            }
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (String[] e : ReferenceData.getEstados()) {
                if (!first) sb.append(',');
                first = false;
                sb.append("{\"id\":").append(e[0])
                  .append(",\"uf\":\"").append(e[1])
                  .append("\",\"nome\":\"").append(e[2])
                  .append("\",\"iduf\":\"").append(e[3]).append("\"}");
            }
            sb.append("]");
            respond(ex, 200, sb.toString());
        }
    }

    static class CidadesHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            addCors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(204, -1); return;
            }
            // Extrai uf do query string
            String query = ex.getRequestURI().getQuery();
            String uf = "";
            if (query != null) {
                for (String p : query.split("&")) {
                    if (p.startsWith("uf=")) { uf = p.substring(3).trim().toUpperCase(); break; }
                }
            }
            // Acha iduf do estado
            String iduf = "";
            for (String[] e : ReferenceData.getEstados())
                if (e[1].equals(uf)) { iduf = e[3]; break; }
            java.util.List<String[]> cidades = ReferenceData.getCidadesPorIduf(iduf);
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (String[] c : cidades) {
                if (!first) sb.append(',');
                first = false;
                sb.append("{\"id\":").append(c[0])
                  .append(",\"nome\":\"").append(c[1].replace("\"","\\\"")).append("\"}");
            }
            sb.append("]");
            respond(ex, 200, sb.toString());
        }
    }

    // =========================================================================
    //  POST /api/chunk/{jobId}/{index}  — recebe um chunk (octet-stream)
    // =========================================================================

    static class ChunkHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            addCors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(204, -1); return;
            }
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                respond(ex, 405, err("Método não permitido")); return;
            }
            // Path: /api/chunk/{jobId}/{index}
            String[] parts = ex.getRequestURI().getPath().split("/");
            if (parts.length < 5) { respond(ex, 400, err("Path inválido: /api/chunk/{jobId}/{index}")); return; }
            String jobId = parts[3];
            int index;
            try { index = Integer.parseInt(parts[4]); }
            catch (NumberFormatException e) { respond(ex, 400, err("Índice de chunk inválido")); return; }

            try {
                Path chunkDir = CHUNK_DIRS.computeIfAbsent(jobId, id -> {
                    try {
                        Path base = resolverWorkDir();
                        Path dir  = Files.createTempDirectory(base, "chunks-");
                        dir.toFile().setReadable(true, false);
                        dir.toFile().setExecutable(true, false);
                        return dir;
                    } catch (IOException e2) { throw new RuntimeException(e2); }
                });

                Path chunkFile = chunkDir.resolve(String.format("chunk_%05d", index));
                long written = 0;
                try (OutputStream out = new FileOutputStream(chunkFile.toFile())) {
                    byte[] buf = new byte[65536];
                    InputStream in = ex.getRequestBody();
                    int n;
                    while ((n = in.read(buf)) != -1) { out.write(buf, 0, n); written += n; }
                }
                respond(ex, 200, "{\"chunk\":" + index + ",\"size\":" + written + "}");
            } catch (Exception e) {
                e.printStackTrace();
                respond(ex, 500, err(e.getMessage()));
            }
        }
    }

    // =========================================================================
    //  POST /api/finalize/{jobId}  — reassembla chunks e inicia migração
    // =========================================================================

    static class FinalizeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            addCors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(204, -1); return;
            }
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                respond(ex, 405, err("Método não permitido")); return;
            }

            String jobId = lastSegment(ex.getRequestURI().getPath());
            Path chunkDir = CHUNK_DIRS.get(jobId);
            if (chunkDir == null) { respond(ex, 404, err("Sessão de upload não encontrada: " + jobId)); return; }

            try {
                // Metadata via JSON: {"sistema","uf","cidade","regime","filename"}
                com.fasterxml.jackson.databind.JsonNode body = JSON.readTree(ex.getRequestBody());
                String sistema  = body.has("sistema")  ? body.get("sistema").asText()  : "syspdv";
                String uf       = body.has("uf")       ? body.get("uf").asText()       : "PA";
                String cidade   = body.has("cidade")   ? body.get("cidade").asText()   : "";
                String regime   = body.has("regime")   ? body.get("regime").asText()   : "SIMPLES";
                String filename = body.has("filename") ? body.get("filename").asText() : "banco.fdb";

                // Ordena e reassembla chunks
                java.io.File[] chunkFiles = chunkDir.toFile().listFiles(
                    f -> f.getName().startsWith("chunk_"));
                if (chunkFiles == null || chunkFiles.length == 0) {
                    respond(ex, 400, err("Nenhum chunk recebido para jobId=" + jobId)); return;
                }
                java.util.Arrays.sort(chunkFiles, (a, b) -> a.getName().compareTo(b.getName()));

                String safeName = filename.replaceAll("[^\\w.\\-]", "_");
                if (safeName.isEmpty()) safeName = "upload.fdb";
                Path fdbPath = chunkDir.resolve(safeName);

                try (OutputStream out = new FileOutputStream(fdbPath.toFile())) {
                    for (java.io.File chunk : chunkFiles) {
                        try (InputStream in = new FileInputStream(chunk)) {
                            byte[] buf = new byte[65536]; int n;
                            while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
                        }
                        chunk.delete();
                    }
                }
                fdbPath.toFile().setReadable(true, false);

                // Monta MultipartData artificial com os campos do formulário
                MultipartData data = new MultipartData();
                data.uf      = uf;
                data.cidade  = cidade;
                data.regime  = regime;
                data.filename = filename;

                JobState job = new JobState(jobId, chunkDir);
                JOBS.put(jobId, job);
                CHUNK_DIRS.remove(jobId);

                // .FBK → restaura antes de migrar
                Path finalPath = fdbPath;
                String lname = filename.toLowerCase();
                if (lname.endsWith(".fbk") || lname.endsWith(".gbk")) {
                    String fdbName = filename.substring(0, filename.lastIndexOf('.')) + ".fdb";
                    Path restoredPath = chunkDir.resolve(fdbName);
                    job.addLog("[Worker] Arquivo .FBK detectado — iniciando restauração gbak...");
                    restaurarFbk(fdbPath, restoredPath, job);
                    finalPath = restoredPath;
                }

                iniciarMigracao(jobId, job, sistema, finalPath.toString(), data);

                ObjectNode resp = JSON.createObjectNode();
                resp.put("jobId", jobId);
                respond(ex, 200, JSON.writeValueAsString(resp));

            } catch (Exception e) {
                e.printStackTrace();
                respond(ex, 500, err(e.getMessage()));
            }
        }
    }

    // =========================================================================
    //  Multipart streaming parser — grava o arquivo direto em disco (sem OOM)
    // =========================================================================

    static class MultipartData {
        String sistema;
        String filename;
        String fileStreamPath; // caminho do arquivo gravado em disco pelo streaming parser
        String uf;
        String cidade;
        String regime;
    }

    /**
     * Faz o parse do multipart sem carregar o arquivo na memória RAM.
     * Campos de texto são bufferizados (pequenos). O campo "arquivo" é gravado
     * diretamente em outDir usando um ring-buffer de tamanho do delimitador.
     */
    static MultipartData parseMultipartStreaming(InputStream httpBody, String boundary, Path outDir)
            throws IOException {
        MultipartData result = new MultipartData();
        // Delimitador entre partes: \r\n--boundary
        byte[] delim = ("\r\n--" + boundary).getBytes(StandardCharsets.ISO_8859_1);
        BufferedInputStream in = new BufferedInputStream(httpBody, 65536);

        // Pula a linha de abertura: --boundary\r\n
        skipLn(in);

        while (true) {
            Map<String, String> hdrs = readHdrs(in);
            if (hdrs == null || hdrs.isEmpty()) break;

            String disp  = hdrs.getOrDefault("content-disposition", "");
            String name  = dispParam(disp, "name");
            String fname = dispParam(disp, "filename");
            boolean isFile = (fname != null && !fname.isEmpty());

            if (isFile) {
                // Grava diretamente em disco — zero alocação do arquivo em heap
                if (result.filename == null) result.filename = fname;
                String safeName = fname.replaceAll("[^\\w.\\-]", "_");
                if (safeName.isEmpty()) safeName = "upload.fdb";
                Path dest = outDir.resolve(safeName);
                try (OutputStream fos = new FileOutputStream(dest.toFile())) {
                    boolean more = copyTillDelim(in, delim, fos);
                    result.fileStreamPath = dest.toAbsolutePath().toString();
                    if (!more) break;
                }
            } else {
                // Campo de texto: bufferiza em memória (normalmente < 1 KB)
                ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
                boolean more = copyTillDelim(in, delim, baos);
                String value = baos.toString("UTF-8").trim();
                if (name != null) {
                    switch (name) {
                        case "sistema": result.sistema = value; break;
                        case "uf":      result.uf      = value; break;
                        case "cidade":  result.cidade  = value; break;
                        case "regime":  result.regime  = value; break;
                    }
                }
                if (!more) break;
            }

            // Após o delimitador lê 2 bytes: \r\n = mais partes, -- = final
            int b1 = in.read();
            int b2 = in.read();
            if (b1 == '-' && b2 == '-') break;
            // \r\n → continua para a próxima parte
        }

        return result;
    }

    /**
     * Copia bytes de `in` para `out` até encontrar `delim` usando ring-buffer.
     * Retorna true se o delimitador foi encontrado, false em EOF.
     * Nunca aloca mais do que delim.length bytes de overhead.
     */
    private static boolean copyTillDelim(InputStream in, byte[] delim, OutputStream out)
            throws IOException {
        int dlen  = delim.length;
        byte[] ring = new byte[dlen];
        int head  = 0; // índice do byte mais antigo no ring
        int count = 0; // quantos bytes estão no ring

        while (true) {
            int b = in.read();
            if (b == -1) {
                // EOF: descarrega o conteúdo restante do ring
                for (int i = 0; i < count; i++) out.write(ring[(head + i) % dlen] & 0xFF);
                return false;
            }

            if (count < dlen) {
                ring[(head + count) % dlen] = (byte) b;
                count++;
                if (count == dlen) {
                    // Ring acabou de encher: verifica match imediato
                    boolean match = true;
                    for (int i = 0; i < dlen; i++) {
                        if (ring[(head + i) % dlen] != delim[i]) { match = false; break; }
                    }
                    if (match) return true;
                }
            } else {
                // Ring cheio: emite o byte mais antigo antes de sobrescrevê-lo
                byte oldest = ring[head];
                out.write(oldest & 0xFF);
                ring[head] = (byte) b;
                head = (head + 1) % dlen;

                // Verifica se o ring agora contém o delimitador
                boolean match = true;
                for (int i = 0; i < dlen; i++) {
                    if (ring[(head + i) % dlen] != delim[i]) { match = false; break; }
                }
                if (match) return true;
            }
        }
    }

    /** Lê e descarta bytes até (e incluindo) o próximo '\n'. */
    private static void skipLn(InputStream in) throws IOException {
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') return;
        }
    }

    /**
     * Lê cabeçalhos HTTP de uma parte multipart até a linha em branco.
     * Retorna mapa lowercase-nome → valor, ou null em EOF.
     */
    private static Map<String, String> readHdrs(InputStream in) throws IOException {
        Map<String, String> hdrs = new LinkedHashMap<>();
        StringBuilder line = new StringBuilder();
        while (true) {
            int b = in.read();
            if (b == -1) return null;
            if (b == '\r') continue; // ignora CR
            if (b == '\n') {
                String s = line.toString();
                if (s.isEmpty()) break; // linha em branco = fim dos cabeçalhos
                int colon = s.indexOf(':');
                if (colon > 0) {
                    hdrs.put(s.substring(0, colon).trim().toLowerCase(),
                             s.substring(colon + 1).trim());
                }
                line.setLength(0);
            } else {
                line.append((char) b);
            }
        }
        return hdrs;
    }

    /** Extrai o valor de um parâmetro nomeado de um cabeçalho Content-Disposition. */
    private static String dispParam(String disp, String paramName) {
        String search = paramName + "=\"";
        int idx = disp.indexOf(search);
        if (idx < 0) return null;
        int start = idx + search.length();
        int end   = disp.indexOf('"', start);
        return end < 0 ? disp.substring(start) : disp.substring(start, end);
    }

    private static String extractBoundary(String contentType) {
        if (contentType == null) return "";
        for (String part : contentType.split(";")) {
            part = part.trim();
            if (part.startsWith("boundary="))
                return part.substring("boundary=".length()).trim();
        }
        return "";
    }

    // =========================================================================
    //  Helpers
    // =========================================================================

    static class JobState {
        final String id;
        final Path   tmpDir;
        volatile String status    = "PENDENTE";
        volatile int   progresso  = 0;
        volatile int   total      = 15;
        volatile Path  sqlPath    = null;
        final List<String> logs   = new ArrayList<>();

        JobState(String id, Path tmpDir) { this.id = id; this.tmpDir = tmpDir; }

        void addLog(String msg) {
            synchronized (logs) { logs.add(msg); }
            System.out.println("[" + id.substring(0, 8) + "] " + msg);
        }
    }

    private static void addCors(HttpExchange ex) {
        ex.getResponseHeaders().add("Access-Control-Allow-Origin",  "*");
        ex.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        ex.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");
    }

    private static void respond(HttpExchange ex, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    private static String lastSegment(String path) {
        String[] parts = path.split("/");
        return parts[parts.length - 1];
    }

    private static String err(String msg) {
        return "{\"erro\":\"" + (msg == null ? "erro" : msg.replace("\"","'")) + "\"}";
    }

}
