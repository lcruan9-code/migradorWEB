package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;
import br.com.lcsistemas.gdoor.core.MigracaoStep;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

/**
 * Classe abstrata base para todos os Steps de migração.
 * Fornece helpers JDBC, constantes de IDs e outros utilitários comuns.
 */
public abstract class StepBase implements MigracaoStep {

    protected static final Logger LOG = Logger.getLogger(StepBase.class.getName());

    // ── IDs fixos no destino (lc_sistemas) ───────────────────────────────────
    protected static final int ID_EMPRESA           = 1;
    protected static final int ID_USUARIO           = 1;
    protected static final int ID_PAIS              = 34;
    protected static final int ID_COBRADOR          = 1;
    protected static final int ID_VENDEDOR          = 1;
    protected static final int ID_OPERADOR          = 1;
    // Pagar
    protected static final int ID_PAGAMENTO_PAGAR   = 46;
    protected static final int ID_PLANOCONTAS_PAGAR = 108;
    protected static final int ID_CONTAMOV_PAGAR    = 1;
    // Receber
    protected static final int ID_PAGAMENTO_REC     = 6;
    protected static final int ID_PLANOCONTAS_REC   = 29;
    protected static final int ID_CONTAMOV_REC      = 1;
    protected static final int ID_CONVENIO          = 1;

    // ── Implementação padrão de prepare/cleanup/rollback ─────────────────────

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
        // Subclasses sobrescrevem se necessário
    }

    @Override
    public void cleanup(MigracaoContext ctx) {
        // Subclasses sobrescrevem se necessário
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        // Subclasses sobrescrevem se necessário
    }

    // ── Helpers JDBC ──────────────────────────────────────────────────────────

    /**
     * Executa um SQL DML/DDL no destino, lançando MigracaoException em caso de erro.
     */
    protected void exec(Connection conn, String sql) throws MigracaoException {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new MigracaoException(getNome(),
                    "SQL falhou: " + e.getMessage() + "\n  SQL: " + abbrev(sql), e);
        } finally {
            close(stmt);
        }
    }

    /**
     * Executa um SQL DML, ignorando erro (best-effort — apenas loga).
     */
    protected void execIgnore(Connection conn, String sql, String descricao) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            LOG.fine("[" + getNome() + "] ignorado (" + descricao + "): " + e.getMessage());
        } finally {
            close(stmt);
        }
    }

    /**
     * Executa um SQL e retorna o número de linhas afetadas.
     */
    protected int execUpdate(Connection conn, String sql) throws MigracaoException {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            return stmt.executeUpdate(sql);
        } catch (SQLException e) {
            throw new MigracaoException(getNome(),
                    "SQL update falhou: " + e.getMessage() + "\n  SQL: " + abbrev(sql), e);
        } finally {
            close(stmt);
        }
    }

    /**
     * Executa um SELECT e retorna o primeiro valor inteiro da primeira linha/coluna.
     * Retorna 0 se não houver resultado.
     */
    protected int execScalarInt(Connection conn, String sql) throws MigracaoException {
        Statement stmt = null;
        ResultSet rs   = null;
        try {
            stmt = conn.createStatement();
            rs   = stmt.executeQuery(sql);
            if (rs.next()) return rs.getInt(1);
            return 0;
        } catch (SQLException e) {
            throw new MigracaoException(getNome(),
                    "SELECT scalar falhou: " + e.getMessage(), e);
        } finally {
            close(rs);
            close(stmt);
        }
    }

    /**
     * Adiciona uma coluna a uma tabela MySQL, ignorando erro se já existir.
     * O defaultVal pode ser null (sem DEFAULT).
     */
    protected void addTempColumn(Connection conn, String schema, String tabela,
                                  String coluna, String tipo, String defaultClause) {
        String def = (defaultClause != null && !defaultClause.isEmpty())
                     ? " DEFAULT " + defaultClause : "";
        String sql = "ALTER TABLE `" + schema + "`.`" + tabela + "` "
                   + "ADD COLUMN `" + coluna + "` " + tipo + def;
        execIgnore(conn, sql, "addTempColumn " + coluna);
    }

    /**
     * Remove uma coluna de uma tabela MySQL, ignorando erro se não existir.
     */
    protected void dropTempColumn(Connection conn, String schema, String tabela, String coluna) {
        String sql = "ALTER TABLE `" + schema + "`.`" + tabela + "` "
                   + "DROP COLUMN `" + coluna + "`";
        execIgnore(conn, sql, "dropTempColumn " + coluna);
    }

    /**
     * Registra estatística de inserção no contexto.
     */
    protected void contarInseridos(MigracaoContext ctx, int n) {
        MigracaoContext.StepStats s = ctx.getStats().get(getNome());
        if (s == null) { s = new MigracaoContext.StepStats(getNome()); ctx.getStats().put(getNome(), s); }
        s.inseridos += n;
    }

    protected void contarIgnorados(MigracaoContext ctx, int n) {
        MigracaoContext.StepStats s = ctx.getStats().get(getNome());
        if (s == null) { s = new MigracaoContext.StepStats(getNome()); ctx.getStats().put(getNome(), s); }
        s.ignorados += n;
    }

    protected void contarErros(MigracaoContext ctx, int n) {
        MigracaoContext.StepStats s = ctx.getStats().get(getNome());
        if (s == null) { s = new MigracaoContext.StepStats(getNome()); ctx.getStats().put(getNome(), s); }
        s.erros += n;
    }

    // ── Formatação de telefone (substitui mysql.formatar_telefone) ────────────

    /**
     * Extrai apenas os dígitos de uma string — espelha a function MySQL extrair_numeros().
     * Ex: "(0xx) 9-1234-5678" → "091234​5678"
     */
    protected static String extrairNumeros(String texto) {
        if (texto == null) return "";
        return texto.replaceAll("[^0-9]", "");
    }

    /**
     * Limpa um telefone extraindo somente dígitos e truncando ao limite da coluna.
     * Substitui os replace de prefixos hardcoded que não cobriam todos os casos.
     */
    protected static String limparTelefone(String tel, int maxLen) {
        if (tel == null || tel.isEmpty()) return "";
        String digits = extrairNumeros(tel);
        if (digits.isEmpty()) return "";
        return digits.length() > maxLen ? digits.substring(0, maxLen) : digits;
    }

    /**
     * Limpa e formata um telefone para padrão brasileiro com DDD.
     * Se o número já tiver DDD, mantém. Se não, aplica o dddPadrao.
     */
    protected static String formatarTelefone(String raw, int dddPadrao) {
        if (raw == null || raw.trim().isEmpty()) return "";
        // Remove tudo que não é dígito
        String digits = extrairNumeros(raw);
        if (digits.isEmpty()) return "";
        // Se começar com 0 e tiver mais de 10 dígitos, remove o 0 inicial
        if (digits.startsWith("0") && digits.length() > 10) {
            digits = digits.substring(1);
        }
        // Se não tiver DDD (menos de 10 dígitos), prepend o DDD padrão
        if (digits.length() < 10) {
            digits = String.valueOf(dddPadrao) + digits;
        }
        // Formata: (xx) xxxxx-xxxx ou (xx) xxxx-xxxx
        if (digits.length() == 11) {
            return "(" + digits.substring(0, 2) + ") " + digits.substring(2, 7) + "-" + digits.substring(7);
        } else if (digits.length() == 10) {
            return "(" + digits.substring(0, 2) + ") " + digits.substring(2, 6) + "-" + digits.substring(6);
        }
        return digits; // Retorna como está se não encaixar
    }

    // ── Utilitários ───────────────────────────────────────────────────────────

    /** Abrevia um SQL longo para logging. */
    private static String abbrev(String sql) {
        return sql.length() > 200 ? sql.substring(0, 200) + "..." : sql;
    }

    /** Fecha um Statement silenciosamente. */
    protected static void close(Statement s) {
        if (s != null) try { s.close(); } catch (SQLException ignored) {}
    }

    /** Fecha um PreparedStatement silenciosamente. */
    protected static void close(PreparedStatement ps) {
        if (ps != null) try { ps.close(); } catch (SQLException ignored) {}
    }

    /** Fecha um ResultSet silenciosamente. */
    protected static void close(ResultSet rs) {
        if (rs != null) try { rs.close(); } catch (SQLException ignored) {}
    }
}
