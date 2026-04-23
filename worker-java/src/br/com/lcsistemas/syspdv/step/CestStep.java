package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * SYSPDV (Firebird) → lc_sistemas.cest
 *
 * Lê CESTs únicos de PRODUTO.PROCEST no Firebird (7 dígitos, ignora 'Escolha').
 * Salva mapa PROCEST→id em ctx para uso no ProdutoStep.
 */
public class CestStep extends StepBase {

    @Override
    public String getNome() { return "CestStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.cest WHERE id > 1");
        // execIgnore(my, "ALTER TABLE lc_sistemas.cest AUTO_INCREMENT = 1", "reset AI cest");

        int ins = 0;
        PreparedStatement pst = null;
        Statement stFb = null;
        ResultSet rs = null;
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT DISTINCT PROCEST FROM PRODUTO " +
                "WHERE CHAR_LENGTH(PROCEST) = 7 AND PROCEST <> 'Escolha'");

            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.cest(cest, ncm, descricao) VALUES (?, '00000000', '')");

            while (rs.next()) {
                String procest = rs.getString(1);
                if (procest == null || procest.trim().isEmpty()) continue;
                pst.setString(1, procest.trim());
                try { pst.executeUpdate(); ins++; }
                catch (SQLException e) { /* duplicado */ }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo CEST do Firebird: " + e.getMessage(), e);
        } finally {
            close(rs); close(stFb); close(pst);
        }

        Map<String, Integer> mapa = new HashMap<String, Integer>();
        Statement stMy = null;
        ResultSet rsMy = null;
        try {
            stMy = my.createStatement();
            rsMy = stMy.executeQuery("SELECT id, cest FROM lc_sistemas.cest");
            while (rsMy.next()) mapa.put(rsMy.getString(2), rsMy.getInt(1));
        } catch (SQLException e) {
            LOG.warning("[CestStep] Erro lendo mapa CEST: " + e.getMessage());
        } finally {
            close(rsMy); close(stMy);
        }
        ctx.setMapaCest(mapa);
        contarInseridos(ctx, ins);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.cest WHERE id > 1", "rollback CestStep");
    }
}
