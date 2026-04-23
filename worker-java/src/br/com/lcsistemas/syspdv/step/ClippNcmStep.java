package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Clipp (Firebird) → lc_sistemas.ncm
 * Lê NCMs de TB_EST_PRODUTO.COD_NCM.
 */
public class ClippNcmStep extends StepBase {

    @Override public String getNome() { return "NcmStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.ncm WHERE id > 1");
        // execIgnore(my, "ALTER TABLE lc_sistemas.ncm AUTO_INCREMENT = 1", "reset AI ncm");

        int ins = 0;
        PreparedStatement pst = null; Statement stFb = null; ResultSet rs = null;
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT DISTINCT TRIM(COD_NCM) FROM TB_EST_PRODUTO " +
                "WHERE COD_NCM IS NOT NULL AND CHAR_LENGTH(TRIM(COD_NCM)) >= 4");
            pst = my.prepareStatement(
                "INSERT IGNORE INTO lc_sistemas.ncm(codigo, descricao, datahora_alteracao, ativo) VALUES (?,?,NOW(),1)");
            while (rs.next()) {
                String ncm = rs.getString(1);
                if (ncm == null || ncm.trim().isEmpty()) continue;
                ncm = ncm.trim().replaceAll("[^0-9]", "");
                if (ncm.isEmpty()) continue;
                pst.setString(1, ncm); pst.setString(2, ncm);
                try { pst.executeUpdate(); ins++; } catch (SQLException e) { /* dup */ }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo TB_EST_PRODUTO NCM: " + e.getMessage(), e);
        } finally { close(rs); close(stFb); close(pst); }

        Map<String, Integer> mapa = new HashMap<>();
        Statement stMy = null; ResultSet rsMy = null;
        try {
            stMy = my.createStatement();
            rsMy = stMy.executeQuery("SELECT id, codigo FROM lc_sistemas.ncm");
            while (rsMy.next()) mapa.put(rsMy.getString(2).trim(), rsMy.getInt(1));
        } catch (SQLException e) { LOG.warning("[ClippNcmStep] mapa: " + e.getMessage()); }
        finally { close(rsMy); close(stMy); }

        ctx.setMapaNcm(mapa);
        contarInseridos(ctx, ins);
    }
}
