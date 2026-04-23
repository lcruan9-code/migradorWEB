package br.com.lcsistemas.syspdv.sql;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Utilitário para executar scripts SQL pesados (como o banco_novo.sql).
 * Suporta múltiplas instruções separadas por ponto-e-vírgula.
 */
public class SqlFileRunner {

    private static final Logger LOG = Logger.getLogger(SqlFileRunner.class.getName());

    /** Callback opcional para redirecionar logs de erro para a UI. */
    public interface LogCallback {
        void log(String msg);
    }

    // Remove atributos MySQL-específicos incompatíveis com H2 2.x
    private static final Pattern P_DOUBLE  = Pattern.compile("(?i)\\bdouble\\(\\d+,\\d+\\)");
    private static final Pattern P_FLOAT   = Pattern.compile("(?i)\\bfloat\\(\\d+,\\d+\\)");
    private static final Pattern P_INT_W   = Pattern.compile("(?i)\\b(tinyint|smallint|mediumint|int|bigint)\\(\\d+\\)");
    private static final Pattern P_CHARSET = Pattern.compile("(?i)\\s*(DEFAULT\\s+)?CHARSET\\s*=\\s*\\w+");
    private static final Pattern P_COLLATE = Pattern.compile("(?i)\\s*COLLATE\\s*=?\\s*\\w+");
    private static final Pattern P_ENGINE  = Pattern.compile("(?i)\\s*ENGINE\\s*=\\s*\\w+");
    private static final Pattern P_ROW_FMT = Pattern.compile("(?i)\\s*ROW_FORMAT\\s*=\\s*\\w+");
    private static final Pattern P_COMMENT  = Pattern.compile("(?i)\\s*COMMENT\\s*=\\s*'[^']*'");
    // AUTO_INCREMENT=N como opção de tabela (≠ coluna): remover pois H2 2.x não suporta
    private static final Pattern P_AI_SEED  = Pattern.compile("(?i)\\bAUTO_INCREMENT\\s*=\\s*\\d+");
    // CHARACTER SET em coluna (ex: varchar(30) CHARACTER SET utf8)
    private static final Pattern P_CHAR_SET = Pattern.compile("(?i)\\s*CHARACTER\\s+SET\\s+\\w+");
    // UNSIGNED / ZEROFILL em tipos numéricos
    private static final Pattern P_UNSIGNED  = Pattern.compile("(?i)\\b(UNSIGNED|ZEROFILL)\\b");
    // Tipos de texto MySQL não suportados no H2 2.x → CLOB / VARCHAR
    private static final Pattern P_LONGTEXT  = Pattern.compile("(?i)\\b(longtext|mediumtext)\\b");
    private static final Pattern P_TINYTEXT  = Pattern.compile("(?i)\\btinytext\\b");
    private static final Pattern P_LONGBLOB  = Pattern.compile("(?i)\\b(longblob|mediumblob)\\b");

    private static String h2ify(String sql) {
        // Backtick → aspas duplas: H2 2.x pode ter regressão com backtick em certos contextos
        sql = sql.replace('`', '"');
        sql = P_DOUBLE.matcher(sql).replaceAll("double");
        sql = P_FLOAT.matcher(sql).replaceAll("float");
        sql = P_INT_W.matcher(sql).replaceAll("$1");
        sql = P_LONGTEXT.matcher(sql).replaceAll("clob");
        sql = P_TINYTEXT.matcher(sql).replaceAll("varchar(255)");
        sql = P_LONGBLOB.matcher(sql).replaceAll("blob");
        sql = P_ENGINE.matcher(sql).replaceAll("");
        sql = P_AI_SEED.matcher(sql).replaceAll("");
        sql = P_CHARSET.matcher(sql).replaceAll("");
        sql = P_CHAR_SET.matcher(sql).replaceAll("");
        sql = P_COLLATE.matcher(sql).replaceAll("");
        sql = P_UNSIGNED.matcher(sql).replaceAll("");
        sql = P_ROW_FMT.matcher(sql).replaceAll("");
        sql = P_COMMENT.matcher(sql).replaceAll("");
        return sql.trim();
    }

    /**
     * Executa o script SQL contido em um InputStream.
     *
     * <p>Tratamento de comentários:
     * <ul>
     *   <li>{@code -- ...} e {@code #...} → ignorados como comentário de linha.</li>
     *   <li>{@code /* ... *\/} (bloco real) → ignorados.</li>
     *   <li>{@code /*! ... *\/;} (conditional comments MySQL, single-line) → tratados como
     *       skip igual a USE/SET@: não bloqueiam o acumulador {@code sb}.</li>
     * </ul>
     *
     * @param logCallback callback opcional para redirecionar logs para a UI (pode ser null)
     */
    public static void run(Connection conn, InputStream is, LogCallback logCallback) throws IOException, SQLException {
        int executados = 0, erros = 0, ignorados = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;

            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();

                // Linhas vazias: ignorar sem afetar o buffer
                if (trimmed.isEmpty()) continue;

                // Comentários de linha (-- e #): ignorar
                if (trimmed.startsWith("--") || trimmed.startsWith("#")) continue;

                // Conditional comments MySQL (/*!...*/): single-line → tratar como skip
                // Ex: /*!40000 ALTER TABLE `x` DISABLE KEYS */;
                // Eles NÃO devem entrar no buffer sb; são pulados e contados como ignorados.
                if (trimmed.startsWith("/*!")) {
                    ignorados++;
                    continue;
                }

                // Comentários de bloco reais (/*...*/): ignorar
                if (trimmed.startsWith("/*")) continue;

                sb.append(line).append("\n");

                // Verifica se a instrução termina em ;
                if (trimmed.endsWith(";")) {
                    String sql = sb.toString().trim();
                    // Remove trailing ;
                    if (sql.endsWith(";")) sql = sql.substring(0, sql.length() - 1).trim();
                    if (!sql.isEmpty()) {
                        String sqlUpper = sql.toUpperCase().replaceAll("\\s+", " ");
                        // Ignora comandos MySQL que H2 não suporta ou que conflitam com o schema já configurado
                        boolean skip = sqlUpper.startsWith("USE ")
                            || sqlUpper.startsWith("CREATE DATABASE")
                            || sqlUpper.startsWith("DROP DATABASE")
                            || sqlUpper.startsWith("SET NAMES")
                            || sqlUpper.startsWith("SET @")
                            || sqlUpper.startsWith("SET @@")
                            || sqlUpper.startsWith("LOCK TABLES")
                            || sqlUpper.startsWith("UNLOCK TABLES")
                            // H2 2.x não suporta ALTER TABLE ... DISABLE/ENABLE KEYS
                            || sqlUpper.contains("DISABLE KEYS")
                            || sqlUpper.contains("ENABLE KEYS");
                        if (skip) {
                            ignorados++;
                        } else {
                            try (Statement st = conn.createStatement()) {
                                st.execute(h2ify(sql));
                                executados++;
                                // Log de progresso a cada 50 statements executados
                                if (logCallback != null && executados % 50 == 0) {
                                    logCallback.log("[Bootstrap] ... " + executados + " statements OK até agora");
                                }
                            } catch (SQLException e) {
                                erros++;
                                String errMsg = "[Bootstrap] ERRO #" + erros + ": "
                                    + e.getMessage().split("\n")[0]
                                    + " | SQL: " + sql.substring(0, Math.min(120, sql.length())).replace("\n", " ");
                                LOG.warning(errMsg);
                                if (logCallback != null) logCallback.log(errMsg);
                            }
                        }
                    }
                    sb.setLength(0);
                }
            }
        }

        // Conta tabelas criadas no schema lc_sistemas para diagnóstico
        int tabelasCriadas = 0;
        try {
            java.sql.ResultSet rs = conn.createStatement().executeQuery(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='LC_SISTEMAS' AND TABLE_TYPE='TABLE'");
            if (rs.next()) tabelasCriadas = rs.getInt(1);
            rs.close();
        } catch (Exception ignored) {}

        String resumo = "[Bootstrap] Concluido: " + executados + " executados, "
            + erros + " erros, " + ignorados + " ignorados | tabelas em lc_sistemas: " + tabelasCriadas;
        LOG.info(resumo);
        if (logCallback != null) logCallback.log(resumo);
    }

    /** Sobrecarga sem callback (compatibilidade com chamadas existentes). */
    public static void run(Connection conn, InputStream is) throws IOException, SQLException {
        run(conn, is, null);
    }

    /**
     * Helper para executar via recurso do classpath.
     * @param logCallback callback opcional para redirecionar erros do H2 para a UI (pode ser null)
     */
    public static void runFromResource(Connection conn, String resourcePath, LogCallback logCallback) throws IOException, SQLException {
        InputStream is = SqlFileRunner.class.getResourceAsStream(resourcePath);
        if (is == null) {
            File f = new File(resourcePath);
            if (f.exists()) is = new FileInputStream(f);
        }
        if (is != null) {
            run(conn, is, logCallback);
        } else {
            throw new FileNotFoundException("Script SQL nao encontrado: " + resourcePath);
        }
    }

    /** Sobrecarga sem callback. */
    public static void runFromResource(Connection conn, String resourcePath) throws IOException, SQLException {
        runFromResource(conn, resourcePath, null);
    }
}
