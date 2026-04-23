package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Clipp (Firebird) → lc_sistemas.cest
 * Lê CESTTs de TB_EST_PRODUTO.COD_CEST.
 */
public class ClippCestStep extends StepBase {

    @Override public String getNome() { return "CestStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.cest WHERE id > 1");
        // execIgnore(my, "ALTER TABLE lc_sistemas.cest AUTO_INCREMENT = 1", "reset AI cest");

        int ins = 0;
        PreparedStatement pst = null; Statement stFb = null; ResultSet rs = null;
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT DISTINCT TRIM(COD_CEST) FROM TB_EST_PRODUTO " +
                "WHERE COD_CEST IS NOT NULL AND CHAR_LENGTH(TRIM(COD_CEST)) >= 4");
            pst = my.prepareStatement(
                "INSERT IGNORE INTO lc_sistemas.cest(cod_cest, descricao, datahora_alteracao, ativo) VALUES (?,?,NOW(),1)");
            while (rs.next()) {
                String cest = rs.getString(1);
                if (cest == null || cest.trim().isEmpty()) continue;
                cest = cest.trim().replaceAll("[^0-9]","");
                if (cest.isEmpty()) continue;
                pst.setString(1, cest); pst.setString(2, cest);
                try { pst.executeUpdate(); ins++; } catch (SQLException e) { /* dup */ }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo TB_EST_PRODUTO CEST: " + e.getMessage(), e);
        } finally { close(rs); close(stFb); close(pst); }

        Map<String, Integer> mapa = new HashMap<>();
        Statement stMy = null; ResultSet rsMy = null;
        try {
            stMy = my.createStatement();
            rsMy = stMy.executeQuery("SELECT id, cod_cest FROM lc_sistemas.cest");
            while (rsMy.next()) mapa.put(rsMy.getString(2).trim(), rsMy.getInt(1));
        } catch (SQLException e) { LOG.warning("[ClippCestStep] mapa: " + e.getMessage()); }
        finally { close(rsMy); close(stMy); }

        ctx.setMapaCest(mapa);
        contarInseridos(ctx, ins);
    }
}
