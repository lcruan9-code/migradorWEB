package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * SYSPDV (Firebird) → lc_sistemas.ncm
 *
 * Lê NCMs únicos de PRODUTO.PRONCM no Firebird.
 * Insere em lc_sistemas.ncm e salva mapa PRONCM→id em ctx.
 */
public class NcmStep extends StepBase {

    @Override
    public String getNome() { return "NcmStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb  = ctx.getOrigemConn();
        Connection my  = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.ncm WHERE id > 1");
        // execIgnore(my, "ALTER TABLE lc_sistemas.ncm AUTO_INCREMENT = 1", "reset AI ncm");

        int ins = 0;
        PreparedStatement pst = null;
        Statement stFb = null;
        ResultSet rs = null;
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT DISTINCT PRONCM FROM PRODUTO " +
                "WHERE CHAR_LENGTH(PRONCM) = 8 AND CAST(PRONCM AS DOUBLE PRECISION) > 0");

            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.ncm(codigo, ex, descricao, aliquota_nacional, " +
                "aliquota_internacional, aliquota_estadual, aliquota_municipal, " +
                "vigenciainicio, vigenciafim, chave, versao, ativo) " +
                "VALUES (?,'',' ',0,0,0,0,NULL,NULL,'','',1)");

            while (rs.next()) {
                String proncm = rs.getString(1);
                if (proncm == null || proncm.trim().isEmpty()) continue;
                pst.setString(1, proncm.trim());
                try { pst.executeUpdate(); ins++; }
                catch (SQLException e) { /* duplicado — ignora */ }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo NCM do Firebird: " + e.getMessage(), e);
        } finally {
            close(rs); close(stFb); close(pst);
        }

        // Constrói mapa PRONCM → id para uso no ProdutoStep
        Map<String, Integer> mapa = new HashMap<String, Integer>();
        Statement stMy = null;
        ResultSet rsMy = null;
        try {
            stMy = my.createStatement();
            rsMy = stMy.executeQuery("SELECT id, codigo FROM lc_sistemas.ncm");
            while (rsMy.next()) mapa.put(rsMy.getString(2), rsMy.getInt(1));
        } catch (SQLException e) {
            LOG.warning("[NcmStep] Erro lendo mapa NCM: " + e.getMessage());
        } finally {
            close(rsMy); close(stMy);
        }
        ctx.setMapaNcm(mapa);
        contarInseridos(ctx, ins);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.ncm WHERE id > 1", "rollback NcmStep");
    }
}
