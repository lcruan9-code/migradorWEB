package br.com.lcsistemas.host.step;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;
import br.com.lcsistemas.host.core.MigracaoStep;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

/**
 * Classe abstrata base para todos os Steps de migração HOST.
 * A ORIGEM é MySQL (schema host.*) e o DESTINO é MySQL (lc_sistemas.*).
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
    protected static final int ID_PAGAMENTO_PAGAR   = 1;
    protected static final int ID_PLANOCONTAS_PAGAR = 38;
    protected static final int ID_CONTAMOV_PAGAR    = 1;
    // Receber
    protected static final int ID_PAGAMENTO_REC     = 6;
    protected static final int ID_PLANOCONTAS_REC   = 28;
    protected static final int ID_CONTAMOV_REC      = 1;
    protected static final int ID_CONVENIO          = 0;

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
     * Executa um SQL DML/DDL na conexão informada, lançando MigracaoException em caso de erro.
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
     * Executa um SQL DML ignorando erro (best-effort — apenas loga).
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
     * Adiciona uma coluna na tabela (Desabilitado para Firebird, mantido para compatibilidade).
     */
    protected void addTempColumn(Connection conn, String schema, String tabela,
                                  String coluna, String tipo, String defaultClause) {
        LOG.fine("[" + getNome() + "] addTempColumn " + coluna + " ignorado no Firebird.");
    }

    /**
     * Remove uma coluna de uma tabela (Desabilitado para Firebird).
     */
    protected void dropTempColumn(Connection conn, String schema, String tabela, String coluna) {
        LOG.fine("[" + getNome() + "] dropTempColumn " + coluna + " ignorado no Firebird.");
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
