package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Migra NCMs únicos do Firebird (ESTOQUE.COD_NCM) para lc_sistemas.ncm.
 *
 * Estratégia JDBC bidirecional:
 *   1. SELECT DISTINCT COD_NCM FROM ESTOQUE WHERE ... (Firebird)
 *   2. INSERT INTO lc_sistemas.ncm ... VALUES (?) (MySQL)
 *   3. Guarda mapa codNcm -> id_ncm no contexto para uso do ProdutoStep
 */
public class NcmStep extends StepBase {

    @Override
    public String getNome() { return "NcmStep"; }

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
        execIgnore(ctx.getDestinoConn(),
            "CREATE INDEX idx_ncm_codigo ON lc_sistemas.ncm (codigo)",
            "idx_ncm_codigo ja existe");
    }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        // 1. Limpa destino (mantém id=1)
        exec(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.ncm WHERE id > 1");
        exec(ctx.getDestinoConn(), "ALTER TABLE lc_sistemas.ncm AUTO_INCREMENT = 2");

        // 2. Coleta NCMs únicos do Firebird
        String selectSql =
            "SELECT DISTINCT COD_NCM FROM ESTOQUE " +
            "WHERE CHAR_LENGTH(CAST(COD_NCM AS VARCHAR(20))) = 8 AND COD_NCM > 0";

        String insertSql =
            "INSERT INTO lc_sistemas.ncm " +
            "  (codigo, ex, descricao, aliquota_nacional, aliquota_internacional," +
            "   aliquota_estadual, aliquota_municipal, vigenciainicio, vigenciafim," +
            "   chave, versao, ativo) " +
            "VALUES (?, '', '', 0.000, 0.000, 0.000, 0.000, NULL, NULL, '', '', 1)";

        int ins = 0;
        Statement stFb  = null;
        ResultSet rs    = null;
        PreparedStatement psmy = null;

        try {
            stFb  = ctx.getOrigemConn().createStatement();
            rs    = stFb.executeQuery(selectSql);
            psmy  = ctx.getDestinoConn().prepareStatement(insertSql);

            while (rs.next()) {
                String codNcm = rs.getString(1);
                if (codNcm == null || codNcm.trim().isEmpty()) continue;
                psmy.setString(1, codNcm.trim());
                psmy.addBatch();
                ins++;
            }
            if (ins > 0) psmy.executeBatch();

        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar NCM: " + e.getMessage(), e);
        } finally {
            close(rs); close(stFb); close(psmy);
        }

        contarInseridos(ctx, ins);

        // 3. Monta mapa codNcm -> id_ncm no destino para uso pelo ProdutoStep
        carregarMapaNcm(ctx);
    }

    private void carregarMapaNcm(MigracaoContext ctx) throws MigracaoException {
        Map<String, Integer> mapa = new HashMap<>();
        Statement st = null;
        ResultSet rs = null;
        try {
            st = ctx.getDestinoConn().createStatement();
            rs = st.executeQuery("SELECT id, codigo FROM lc_sistemas.ncm");
            while (rs.next()) {
                mapa.put(rs.getString(2), rs.getInt(1));
            }
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao carregar mapa NCM: " + e.getMessage(), e);
        } finally {
            close(rs); close(st);
        }
        ctx.setMapaNcm(mapa);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.ncm WHERE id > 1",
            "rollback NcmStep");
    }

    @Override
    public void cleanup(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DROP INDEX idx_ncm_codigo ON lc_sistemas.ncm",
            "drop idx_ncm_codigo");
    }
}
