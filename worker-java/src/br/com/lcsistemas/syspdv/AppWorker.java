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

    static final Map<String, JobState> JOBS = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/health",       new HealthHandler());
        server.createContext("/api/processar", new ProcessarHandler());
        server.createContext("/api/status",    new StatusHandler());
        server.createContext("/api/download",  new DownloadHandler());
        server.createContext("/api/cidades",   new CidadesHandler());
        server.createContext("/api/estados",   new EstadosHandler());
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
            respond(ex, 200, "{\"ok\":true}");
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
                byte[] body        = ex.getRequestBody().readAllBytes();

                MultipartData data = parseMultipart(body, boundary);
                String sistema     = data.sistema != null ? data.sistema : "syspdv";
                String filename    = data.filename != null ? data.filename : "banco.fdb";

                if (data.fileBytes == null || data.fileBytes.length == 0) {
                    respond(ex, 400, err("Arquivo não recebido")); return;
                }

                // Salva o arquivo enviado em temp
                Path tmpDir      = Files.createTempDirectory("migrador-");
                Path uploadPath  = tmpDir.resolve(filename);
                Files.write(uploadPath, data.fileBytes);

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

        // Procura em todas as versões bundled — gbak do 2.5 serve para restaurar qualquer .fbk
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
            "gbak não encontrado na pasta FIREBIRD/ bundled.\n" +
            "Caminho esperado: " + java.nio.file.Paths.get(baseDir, "FIREBIRD", "Firebird-2.5.9.manual", "bin", binName) + "\n" +
            "Certifique-se de que a pasta FIREBIRD/ está no mesmo diretório que a aplicação.");
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
    //  Multipart parser mínimo
    // =========================================================================

    static class MultipartData {
        String sistema;
        String filename;
        byte[] fileBytes;
        String uf;
        String cidade;
        String regime;
    }

    static MultipartData parseMultipart(byte[] body, String boundary) throws IOException {
        MultipartData result = new MultipartData();
        byte[] boundaryBytes = ("--" + boundary).getBytes(StandardCharsets.ISO_8859_1);
        byte[] crlf          = "\r\n".getBytes(StandardCharsets.ISO_8859_1);

        List<byte[]> parts = splitParts(body, boundaryBytes);
        for (byte[] part : parts) {
            // Separa header do conteúdo (header termina em \r\n\r\n)
            int headerEnd = indexOf(part, new byte[]{'\r','\n','\r','\n'}, 0);
            if (headerEnd < 0) continue;

            String header  = new String(part, 0, headerEnd, StandardCharsets.ISO_8859_1);
            byte[] content = subArray(part, headerEnd + 4, part.length);

            // Remove CRLF final se presente
            if (content.length >= 2 &&
                content[content.length - 2] == '\r' && content[content.length - 1] == '\n') {
                content = subArray(content, 0, content.length - 2);
            }

            if (header.contains("name=\"sistema\"")) {
                result.sistema = new String(content, StandardCharsets.UTF_8).trim();
            } else if (header.contains("name=\"uf\"")) {
                result.uf = new String(content, StandardCharsets.UTF_8).trim();
            } else if (header.contains("name=\"cidade\"")) {
                result.cidade = new String(content, StandardCharsets.UTF_8).trim();
            } else if (header.contains("name=\"regime\"")) {
                result.regime = new String(content, StandardCharsets.UTF_8).trim();
            } else if (header.contains("name=\"arquivo\"")) {
                // Extrai filename=
                String fn = "";
                int fi = header.indexOf("filename=\"");
                if (fi >= 0) {
                    int fe = header.indexOf("\"", fi + 10);
                    fn = header.substring(fi + 10, fe);
                }
                result.filename  = fn.isEmpty() ? "banco.fdb" : fn;
                result.fileBytes = content;
            }
        }
        return result;
    }

    private static List<byte[]> splitParts(byte[] data, byte[] delimiter) {
        List<byte[]> parts = new ArrayList<>();
        int start = 0;
        while (start < data.length) {
            int pos = indexOf(data, delimiter, start);
            if (pos < 0) break;
            if (pos > start) parts.add(subArray(data, start, pos));
            start = pos + delimiter.length;
            // Pula \r\n após o delimiter
            if (start < data.length - 1 && data[start] == '\r' && data[start + 1] == '\n')
                start += 2;
        }
        return parts;
    }

    private static int indexOf(byte[] data, byte[] pattern, int from) {
        outer:
        for (int i = from; i <= data.length - pattern.length; i++) {
            for (int j = 0; j < pattern.length; j++)
                if (data[i + j] != pattern[j]) continue outer;
            return i;
        }
        return -1;
    }

    private static byte[] subArray(byte[] src, int from, int to) {
        byte[] dest = new byte[to - from];
        System.arraycopy(src, from, dest, 0, dest.length);
        return dest;
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
