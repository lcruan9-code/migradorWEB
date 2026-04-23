package br.com.lcsistemas.syspdv.sql;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Gera o dump MySQL 5.5.38 a partir dos dados em SqlMemoryStore.
 * Chamado ao final da migração: escreve header, DDLs, INSERTs e UPDATEs.
 */
public class SqlFileWriter {

    private final Path outputPath;
    private final List<String> deferredSql = new ArrayList<String>(); // UPDATEs do AjusteGeral
    private Map<String, String> originalDdls = null;

    public SqlFileWriter(Path outputPath) {
        this.outputPath = outputPath;
    }

    public void addDeferredSql(String sql) {
        if (sql == null || sql.trim().isEmpty()) return;
        deferredSql.add(sql.trim());
    }

    /** 
     * Escreve o dump completo lendo diretamente do banco de dados (H2/MySQL).
     */
    public void writeFromConnection(java.sql.Connection conn) throws IOException, java.sql.SQLException {
        loadOriginalDdls();
        try (BufferedWriter w = openWriter()) {
            writeHeader(w);
            for (String table : LcSchema.TABLE_ORDER) {
                if (!LcSchema.hasTable(table)) continue;
                writeTableFromDatabase(w, conn, table);
            }
            writeDeferredUpdates(w);
            writeFooter(w);
        }
    }

    private BufferedWriter openWriter() throws IOException {
        return new BufferedWriter(
                new OutputStreamWriter(
                    new FileOutputStream(outputPath.toFile()), StandardCharsets.UTF_8));
    }

    private void writeHeader(BufferedWriter w) throws IOException {
        String now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        w.write("-- MySQL dump 10.13  Distrib 5.5.38, for Win64 (x86)\n");
        w.write("--\n");
        w.write("-- Host: 127.0.0.1    Database: lc_sistemas\n");
        w.write("-- ------------------------------------------------------\n");
        w.write("-- Server version\t5.5.38\n");
        w.write("\n");
        w.write("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n");
        w.write("/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;\n");
        w.write("/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;\n");
        w.write("/*!40101 SET NAMES utf8 */;\n");
        w.write("/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;\n");
        w.write("/*!40103 SET TIME_ZONE='+00:00' */;\n");
        w.write("/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;\n");
        w.write("/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;\n");
        w.write("/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;\n");
        w.write("/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;\n");
        w.write("\n");
    }

    private void writeTableFromDatabase(BufferedWriter w, java.sql.Connection conn, String table) throws IOException, java.sql.SQLException {
        w.write("--\n");
        w.write("-- Table structure for table `" + table + "`\n");
        w.write("--\n\n");
        w.write(getDynamicDdl(conn, table));
        w.write("\n\n");
        w.write("--\n");
        w.write("-- Dumping data for table `" + table + "`\n");
        w.write("--\n\n");

        try (java.sql.Statement st = conn.createStatement();
             java.sql.ResultSet rs = st.executeQuery("SELECT * FROM " + table)) {
            
            java.sql.ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            
            if (!rs.next()) {
                w.write("\n");
                return;
            }

            w.write("LOCK TABLES `" + table + "` WRITE;\n");
            w.write("/*!40000 ALTER TABLE `" + table + "` DISABLE KEYS */;\n");

            StringBuilder sb = new StringBuilder("INSERT INTO `").append(table).append("` VALUES ");
            boolean firstRow = true;
            int count = 0;

            do {
                if (!firstRow) sb.append(',');
                firstRow = false;
                sb.append('(');
                for (int i = 1; i <= colCount; i++) {
                    if (i > 1) sb.append(',');
                    sb.append(formatValue(rs.getObject(i)));
                }
                sb.append(')');
                
                count++;
                if (count >= 500) { // Batch inserts a cada 500 linhas
                    sb.append(';');
                    w.write(sb.toString());
                    w.write("\n");
                    sb.setLength(0);
                    sb.append("INSERT INTO `").append(table).append("` VALUES ");
                    firstRow = true;
                    count = 0;
                }
            } while (rs.next());

            if (!firstRow) {
                sb.append(';');
                w.write(sb.toString());
                w.write("\n");
            }

            w.write("/*!40000 ALTER TABLE `" + table + "` ENABLE KEYS */;\n");
            w.write("UNLOCK TABLES;\n\n");
        }
    }

    private void loadOriginalDdls() {
        if (originalDdls != null) return;
        originalDdls = new HashMap<String, String>();
        
        InputStream is = getClass().getResourceAsStream("/br/com/lcsistemas/syspdv/resource/banco_novo.sql");
        String source = "classpath";
        
        if (is == null) {
            String base = System.getProperty("user.dir");
            File[] files = {
                new File(base, "src/br/com/lcsistemas/syspdv/resource/banco_novo.sql"),
                new File(base, "worker-java/src/br/com/lcsistemas/syspdv/resource/banco_novo.sql"),
                new File("src/br/com/lcsistemas/syspdv/resource/banco_novo.sql"),
                new File("worker-java/src/br/com/lcsistemas/syspdv/resource/banco_novo.sql")
            };
            for (File f : files) {
                if (f.exists()) {
                    try { is = new FileInputStream(f); source = f.getAbsolutePath(); break; } catch (Exception e) {}
                }
            }
        }
        
        if (is == null) {
            System.err.println("[SqlFileWriter] ERRO: banco_novo.sql não encontrado em nenhum local!");
            return;
        }
        
        System.out.println("[SqlFileWriter] Carregando DDLs originais de: " + source);
        
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder fullSql = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                fullSql.append(line).append("\n");
            }
            
            String sql = fullSql.toString();
            // Regex mais flexível para capturar DROP + CREATE
            String[] parts = sql.split("(?i)DROP\\s+TABLE\\s+IF\\s+EXISTS\\s+`?");
            int count = 0;
            for (int i = 1; i < parts.length; i++) {
                String part = parts[i];
                // Encontra o fim do nome da tabela (pode terminar com ` ou apenas espaço/ponto-e-vírgula)
                int endTableName = part.indexOf("`") != -1 ? part.indexOf("`") : part.indexOf(";");
                if (endTableName > 0) {
                    String tableName = part.substring(0, endTableName).toLowerCase().trim();
                    int createIndex = part.toUpperCase().indexOf("CREATE TABLE");
                    if (createIndex != -1) {
                        int endCreate = part.indexOf(";", createIndex);
                        if (endCreate != -1) {
                            String ddl = "DROP TABLE IF EXISTS `" + tableName + "`;\n" + part.substring(part.indexOf("CREATE TABLE", createIndex), endCreate + 1);
                            originalDdls.put(tableName, ddl);
                            count++;
                        }
                    }
                }
            }
            System.out.println("[SqlFileWriter] " + count + " DDLs originais carregados com sucesso.");
        } catch (Exception e) {
            System.err.println("[SqlFileWriter] Erro ao processar banco_novo.sql: " + e.getMessage());
        } finally {
            if (is != null) {
                try { is.close(); } catch (Exception e) {}
            }
        }
    }

    private String getDynamicDdl(java.sql.Connection conn, String table) throws java.sql.SQLException {
        if (originalDdls != null && originalDdls.containsKey(table.toLowerCase())) {
            return originalDdls.get(table.toLowerCase());
        }
        
        StringBuilder sb = new StringBuilder();
        boolean foundTable = false;
        
        try (java.sql.Statement st = conn.createStatement();
             java.sql.ResultSet rs = st.executeQuery("SCRIPT NOPASSWORDS NOSETTINGS TABLE lc_sistemas." + table)) {
            while (rs.next()) {
                String line = rs.getString(1).trim();
                String upperLine = line.toUpperCase();
                
                // Ignorar cabeçalhos e criações de usuário/schema
                if (upperLine.startsWith("--") || upperLine.startsWith("CREATE USER") || upperLine.startsWith("CREATE SCHEMA")) {
                    continue;
                }
                
                if (upperLine.startsWith("CREATE TABLE") || upperLine.contains("CREATE CACHED TABLE") || upperLine.contains("CREATE MEMORY TABLE")) {
                    foundTable = true;
                }
                
                if (!line.isEmpty()) {
                    // Limpar e ajustar para MySQL (remove schema prefixes e sintaxe H2)
                    String stmt = line.replaceAll("(?i)(?:\"|`)?lc_sistemas(?:\"|`)?\\.", "")
                                      .replaceAll("(?i)(?:\"|`)?PUBLIC(?:\"|`)?\\.", "")
                                      .replaceAll("(?i)CACHED ", "")
                                      .replaceAll("(?i)MEMORY ", "")
                                      .replaceAll("(?i)\\bGENERATED\\s+(?:BY\\s+DEFAULT|ALWAYS)\\s+AS\\s+IDENTITY(?:\\s*\\([^;]*?\\))?", "AUTO_INCREMENT")
                                      .replaceAll("(?i)\\bSELECTIVITY\\s+\\d+\\b", "")
                                      .replaceAll("(?i)\\bDEFAULT\\s+ON\\s+NULL\\b", "")
                                      .replace("\"", "`");
                    stmt = stmt.replaceAll("\\s{2,}", " ").trim();
                    if (stmt.isEmpty()) {
                        continue;
                    }
                    if (!stmt.endsWith(";")) stmt += ";";
                    sb.append(stmt).append("\n");
                }
            }
        }
        
        if (foundTable) {
            return "DROP TABLE IF EXISTS `" + table + "`;\n" + sb.toString();
        }
        return "-- DDL not found dynamically for " + table;
    }

    private void writeDeferredUpdates(BufferedWriter w) throws IOException {
        if (deferredSql.isEmpty()) return;
        w.write("--\n-- Ajustes pos-migracao\n--\n\n");
        for (String sql : deferredSql) {
            // Remove schema prefix para execução sem USE
            String clean = sql.replaceAll("(?i)lc_sistemas\\.", "");
            w.write(clean);
            if (!clean.endsWith(";")) w.write(";");
            w.write("\n");
        }
        w.write("\n");
    }

    private void writeFooter(BufferedWriter w) throws IOException {
        String now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        w.write("/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;\n\n");
        w.write("/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;\n");
        w.write("/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;\n");
        w.write("/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;\n");
        w.write("/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;\n");
        w.write("/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;\n");
        w.write("/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;\n");
        w.write("/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;\n\n");
        w.write("-- Dump completed on " + now + "\n");
    }

    /**
     * Marcador para literais SQL (NOW(), DATE(NOW()), CURRENT_TIMESTAMP) que devem
     * ser escritos no dump sem aspas.
     */
    public static final class SqlLiteral {
        public final String expr;
        public SqlLiteral(String expr) { this.expr = expr; }
        @Override public String toString() { return expr; }
    }

    /** Formata um valor Java para SQL MySQL */
    public static String formatValue(Object v) {
        if (v == null) return "NULL";
        if (v instanceof SqlLiteral) return ((SqlLiteral) v).expr;
        if (v instanceof Double || v instanceof Float) {
            double d = ((Number) v).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) return "0.0000";
            return String.format(Locale.US, "%.4f", d);
        }
        if (v instanceof Number) return v.toString();
        if (v instanceof Boolean) return ((Boolean) v) ? "1" : "0";
        
        // Datas
        if (v instanceof java.util.Date) {
            return "'" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format((java.util.Date) v) + "'";
        }

        // String — escapa e envolve em aspas simples
        String s = v.toString();
        s = s.replace("\\", "\\\\")
             .replace("'",  "\\'")
             .replace("\n", "\\n")
             .replace("\r", "\\r")
             .replace("\t", "\\t")
             .replace("\0", "\\0");
        return "'" + s + "'";
    }
}
