package br.com.lcsistemas.syspdv.worker;

import br.com.lcsistemas.syspdv.config.MigracaoConfig;
import br.com.lcsistemas.syspdv.engine.MigracaoEngine;
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
 * Base do servidor HTTP de migração LC.
 * Contém toda a infraestrutura compartilhada: HTTP, jobs, multipart, status, download.
 *
 * Cada sistema de origem herda esta classe e implementa:
 *   - getPorta()          → porta HTTP exclusiva do sistema
 *   - getSistema()        → identificador ("clipp", "gdoor", etc.)
 *   - prepararArquivo()   → pré-processamento específico (ex: FBK restore no Clipp)
 *   - buildConfig()       → configuração específica do sistema
 *
 * REGRA: nunca altere esta classe para comportamento de um sistema específico.
 * Toda lógica de sistema fica na subclasse correspondente.
 */
public abstract class WorkerBase {

    protected static final ObjectMapper JSON = new ObjectMapper();
    protected final Map<String, JobState> jobs = new ConcurrentHashMap<>();

    // ── API para subclasses ───────────────────────────────────────────────────

    /** Porta HTTP que este worker irá escutar. */
    protected abstract int getPorta();

    /** Identificador do sistema (usado nos logs). */
    protected abstract String getSistema();

    /**
     * Pré-processa o arquivo enviado antes da migração.
     * Override nas subclasses que precisam de restauração FBK, conversão, etc.
     * Por padrão retorna o arquivo original sem modificação.
     */
    protected Path prepararArquivo(Path uploadPath, String filename, JobState job) throws Exception {
        return uploadPath;
    }

    /**
     * Constrói a MigracaoConfig específica do sistema.
     * Cada subclasse define suas origens, modos e configurações próprias.
     */
    protected abstract MigracaoConfig buildConfig(String fdbPath, JobState job, MultipartData data);

    // ── Inicialização do servidor ─────────────────────────────────────────────

    public void iniciar() throws IOException {
        int porta = getPorta();
        HttpServer server = HttpServer.create(new InetSocketAddress(porta), 0);
        server.createContext("/api/processar", new ProcessarHandler());
        server.createContext("/api/status",    new StatusHandler());
        server.createContext("/api/download",  new DownloadHandler());
        server.createContext("/api/cidades",   new CidadesHandler());
        server.createContext("/api/estados",   new EstadosHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("[" + getSistema().toUpperCase() + "] Worker rodando em http://localhost:" + porta);
    }

    // ── Handler: POST /api/processar ─────────────────────────────────────────

    class ProcessarHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            addCors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) { ex.sendResponseHeaders(204, -1); return; }
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod()))   { respond(ex, 405, err("Método não permitido")); return; }

            try {
                String boundary = extractBoundary(ex.getRequestHeaders().getFirst("Content-Type"));
                byte[] body     = ex.getRequestBody().readAllBytes();
                MultipartData data = parseMultipart(body, boundary);
                String filename    = data.filename != null ? data.filename : "banco.fdb";

                if (data.fileBytes == null || data.fileBytes.length == 0) {
                    respond(ex, 400, err("Arquivo não recebido")); return;
                }

                Path tmpDir     = Files.createTempDirectory("migrador-" + getSistema() + "-");
                Path uploadPath = tmpDir.resolve(filename);
                Files.write(uploadPath, data.fileBytes);

                String   jobId = UUID.randomUUID().toString();
                JobState job   = new JobState(jobId, tmpDir, getSistema());
                jobs.put(jobId, job);

                // Pré-processamento específico do sistema (ex: FBK restore)
                Path fdbPath;
                try {
                    fdbPath = prepararArquivo(uploadPath, filename, job);
                } catch (Exception e) {
                    job.status = "ERRO";
                    job.addLog("[Worker] Falha no pré-processamento: " + e.getMessage());
                    ObjectNode resp = JSON.createObjectNode();
                    resp.put("jobId", jobId);
                    respond(ex, 200, JSON.writeValueAsString(resp));
                    return;
                }

                iniciarMigracao(jobId, job, fdbPath.toString(), data);

                ObjectNode resp = JSON.createObjectNode();
                resp.put("jobId", jobId);
                respond(ex, 200, JSON.writeValueAsString(resp));

            } catch (Exception e) {
                e.printStackTrace();
                respond(ex, 500, err(e.getMessage()));
            }
        }
    }

    // ── Handler: GET /api/status/{jobId} ─────────────────────────────────────

    class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            addCors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) { ex.sendResponseHeaders(204, -1); return; }
            String   jobId = lastSegment(ex.getRequestURI().getPath());
            JobState job   = jobs.get(jobId);
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

    // ── Handler: GET /api/download/{jobId} ───────────────────────────────────

    class DownloadHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            addCors(ex);
            String   jobId = lastSegment(ex.getRequestURI().getPath());
            JobState job   = jobs.get(jobId);
            if (job == null || job.sqlPath == null || !Files.exists(job.sqlPath)) {
                respond(ex, 404, err("Arquivo SQL não disponível")); return;
            }
            byte[] bytes = Files.readAllBytes(job.sqlPath);
            ex.getResponseHeaders().add("Content-Type", "application/sql");
            ex.getResponseHeaders().add("Content-Disposition", "attachment; filename=\"TabelasParaImportacao.sql\"");
            ex.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
        }
    }

    // ── Handler: GET /api/estados ────────────────────────────────────────────

    class EstadosHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            addCors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) { ex.sendResponseHeaders(204, -1); return; }
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (String[] e : ReferenceData.getEstados()) {
                if (!first) sb.append(','); first = false;
                sb.append("{\"id\":").append(e[0]).append(",\"uf\":\"").append(e[1])
                  .append("\",\"nome\":\"").append(e[2]).append("\",\"iduf\":\"").append(e[3]).append("\"}");
            }
            sb.append("]");
            respond(ex, 200, sb.toString());
        }
    }

    // ── Handler: GET /api/cidades?uf=XX ──────────────────────────────────────

    class CidadesHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            addCors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) { ex.sendResponseHeaders(204, -1); return; }
            String query = ex.getRequestURI().getQuery(); String uf = "";
            if (query != null) for (String p : query.split("&")) if (p.startsWith("uf=")) { uf = p.substring(3).trim().toUpperCase(); break; }
            String iduf = "";
            for (String[] e : ReferenceData.getEstados()) if (e[1].equals(uf)) { iduf = e[3]; break; }
            List<String[]> cidades = ReferenceData.getCidadesPorIduf(iduf);
            StringBuilder sb = new StringBuilder("["); boolean first = true;
            for (String[] c : cidades) {
                if (!first) sb.append(','); first = false;
                sb.append("{\"id\":").append(c[0]).append(",\"nome\":\"").append(c[1].replace("\"","\\\"")).append("\"}");
            }
            sb.append("]");
            respond(ex, 200, sb.toString());
        }
    }

    // ── Engine em thread de background ───────────────────────────────────────

    private void iniciarMigracao(String jobId, JobState job, String fdbPath, MultipartData data) {
        Thread t = new Thread(() -> {
            job.status = "PROCESSANDO";
            job.addLog("[Worker] Job " + jobId.substring(0, 8) + " — sistema=" + getSistema());
            job.addLog("[Worker] Arquivo: " + fdbPath);
            try {
                MigracaoConfig config = buildConfig(fdbPath, job, data);
                Path sqlOut = job.tmpDir.resolve("TabelasParaImportacao.sql");
                config.setSqlOutputPath(sqlOut.toString());

                MigracaoEngine engine = new MigracaoEngine(config);
                engine.setListener(new MigracaoEngine.ProgressListener() {
                    @Override public void onLog(String msg) { job.addLog(msg); }
                    @Override public void onStepInicio(String nome, int atual, int total) {
                        job.progresso = atual; job.total = total;
                    }
                    @Override public void onStepConcluido(String nome, int ins, int ign, int err, boolean ok) {
                        job.addLog("[" + nome + "] " + (ok ? "✓" : "✗") + " ins=" + ins + " ign=" + ign + " err=" + err);
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
        }, "migrador-" + getSistema() + "-" + jobId.substring(0, 8));
        t.setDaemon(true);
        t.start();
    }

    // ── Helpers comuns de configuração (para subclasses) ─────────────────────

    protected MigracaoConfig baseConfig(String fdbPath) {
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
        return c;
    }

    protected void aplicarLocalizacao(MigracaoConfig c, MultipartData data, JobState job) {
        String uf     = data.uf     != null ? data.uf.trim().toUpperCase() : "PA";
        String cidade = data.cidade != null ? data.cidade.trim()           : "";
        String regime = data.regime != null ? data.regime.trim().toUpperCase() : "SIMPLES";
        c.setClienteUf(uf);
        c.setRegimeTributario(regime);
        int idEstado = ReferenceData.getEstadoId(uf);
        c.setEstadoDefaultId(idEstado);
        if (!cidade.isEmpty() && idEstado > 0) {
            String iduf = "";
            for (String[] e : ReferenceData.getEstados()) if (e[1].equalsIgnoreCase(uf)) { iduf = e[3]; break; }
            c.setCidadeDefaultId(ReferenceData.getCidadeId(iduf, cidade));
        }
        job.addLog("[Config] UF=" + uf + " | Cidade=" + cidade + " | Regime=" + regime
            + " | id_estado=" + c.getEstadoDefaultId() + " | id_cidade=" + c.getCidadeDefaultId());
    }

    // ── Multipart parser ──────────────────────────────────────────────────────

    public static class MultipartData {
        public String sistema, filename, uf, cidade, regime;
        public byte[] fileBytes;
    }

    static MultipartData parseMultipart(byte[] body, String boundary) {
        MultipartData result = new MultipartData();
        byte[] boundaryBytes = ("--" + boundary).getBytes(StandardCharsets.ISO_8859_1);
        List<byte[]> parts = splitParts(body, boundaryBytes);
        for (byte[] part : parts) {
            int headerEnd = indexOf(part, new byte[]{'\r','\n','\r','\n'}, 0);
            if (headerEnd < 0) continue;
            String header  = new String(part, 0, headerEnd, StandardCharsets.ISO_8859_1);
            byte[] content = subArray(part, headerEnd + 4, part.length);
            if (content.length >= 2 && content[content.length-2] == '\r' && content[content.length-1] == '\n')
                content = subArray(content, 0, content.length - 2);
            if      (header.contains("name=\"sistema\"")) result.sistema = new String(content, StandardCharsets.UTF_8).trim();
            else if (header.contains("name=\"uf\""))      result.uf      = new String(content, StandardCharsets.UTF_8).trim();
            else if (header.contains("name=\"cidade\""))  result.cidade  = new String(content, StandardCharsets.UTF_8).trim();
            else if (header.contains("name=\"regime\""))  result.regime  = new String(content, StandardCharsets.UTF_8).trim();
            else if (header.contains("name=\"arquivo\"")) {
                String fn = ""; int fi = header.indexOf("filename=\"");
                if (fi >= 0) { int fe = header.indexOf("\"", fi + 10); fn = header.substring(fi + 10, fe); }
                result.filename  = fn.isEmpty() ? "banco.fdb" : fn;
                result.fileBytes = content;
            }
        }
        return result;
    }

    // ── Utilitários estáticos ─────────────────────────────────────────────────

    public static class JobState {
        public final String id;
        public final Path   tmpDir;
        public final String sistema;
        public volatile String status   = "PENDENTE";
        public volatile int   progresso = 0;
        public volatile int   total     = 15;
        public volatile Path  sqlPath   = null;
        public final List<String> logs  = new ArrayList<>();

        public JobState(String id, Path tmpDir, String sistema) {
            this.id = id; this.tmpDir = tmpDir; this.sistema = sistema;
        }
        public void addLog(String msg) {
            synchronized (logs) { logs.add(msg); }
            System.out.println("[" + sistema.toUpperCase() + "][" + id.substring(0, 8) + "] " + msg);
        }
    }

    private static List<byte[]> splitParts(byte[] data, byte[] delimiter) {
        List<byte[]> parts = new ArrayList<>(); int start = 0;
        while (start < data.length) {
            int pos = indexOf(data, delimiter, start);
            if (pos < 0) break;
            if (pos > start) parts.add(subArray(data, start, pos));
            start = pos + delimiter.length;
            if (start < data.length - 1 && data[start] == '\r' && data[start + 1] == '\n') start += 2;
        }
        return parts;
    }

    private static int indexOf(byte[] data, byte[] pattern, int from) {
        outer: for (int i = from; i <= data.length - pattern.length; i++) {
            for (int j = 0; j < pattern.length; j++) if (data[i+j] != pattern[j]) continue outer;
            return i;
        } return -1;
    }

    private static byte[] subArray(byte[] src, int from, int to) {
        byte[] dest = new byte[to - from]; System.arraycopy(src, from, dest, 0, dest.length); return dest;
    }

    private static String extractBoundary(String ct) {
        if (ct == null) return "";
        for (String p : ct.split(";")) { p = p.trim(); if (p.startsWith("boundary=")) return p.substring("boundary=".length()).trim(); }
        return "";
    }

    protected static void addCors(HttpExchange ex) {
        ex.getResponseHeaders().add("Access-Control-Allow-Origin",  "*");
        ex.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        ex.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");
    }

    protected static void respond(HttpExchange ex, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    private static String lastSegment(String path) {
        String[] parts = path.split("/"); return parts[parts.length - 1];
    }

    protected static String err(String msg) {
        return "{\"erro\":\"" + (msg == null ? "erro" : msg.replace("\"","'")) + "\"}";
    }
}
