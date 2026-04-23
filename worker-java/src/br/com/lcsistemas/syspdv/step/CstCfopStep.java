package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * SYSPDV (Firebird) — Pré-carrega mapas de CST/CFOP do MySQL e tributação do Firebird.
 *
 * Não insere dados. Apenas constrói mapas em ctx para uso no ProdutoStep:
 *   - mapaCstA: codigotributario → id (tabela='A', Regime Normal)
 *   - mapaCstB: codigotributario → id (tabela='B', Simples Nacional)
 *   - mapaTributacao: TRBID → {trbtabb, trbcsosn, trbalq} lidos do Firebird
 */
public class CstCfopStep extends StepBase {

    @Override
    public String getNome() { return "CstCfopStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        // Carrega mapas CST do MySQL
        Map<String, Integer> mapaCstA = new HashMap<String, Integer>();
        Map<String, Integer> mapaCstB = new HashMap<String, Integer>();
        Statement stMy = null;
        ResultSet rsMy = null;
        try {
            stMy = my.createStatement();
            rsMy = stMy.executeQuery("SELECT id, codigotributario, tabela FROM lc_sistemas.cst");
            while (rsMy.next()) {
                int id = rsMy.getInt(1);
                String cod = rsMy.getString(2);
                String tab = rsMy.getString(3);
                if ("A".equals(tab)) mapaCstA.put(cod, id);
                else if ("B".equals(tab)) mapaCstB.put(cod, id);
            }
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo lc_sistemas.cst: " + e.getMessage(), e);
        } finally {
            close(rsMy); close(stMy);
        }
        ctx.put("mapaCstA", mapaCstA);
        ctx.put("mapaCstB", mapaCstB);

        // Carrega tributação do Firebird: TRBID → array[trbtabb, trbcsosn, trbalq]
        // TRBID é VARCHAR no Firebird (ex: "T19", "F00") — usar getString, não getInt
        Map<String, String[]> mapaTrib = new HashMap<String, String[]>();
        Statement stFb = null;
        ResultSet rsFb = null;
        try {
            stFb = fb.createStatement();
            rsFb = stFb.executeQuery("SELECT TRBID, TRBTABB, TRBCSOSN, TRBALQ FROM TRIBUTACAO");
            while (rsFb.next()) {
                String trbid  = rsFb.getString(1);
                if (trbid == null) continue;
                trbid = trbid.trim();
                String trbtabb  = rsFb.getString(2);
                String trbcsosn = rsFb.getString(3);
                String trbalq   = rsFb.getString(4);
                mapaTrib.put(trbid, new String[]{
                    trbtabb  == null ? "" : trbtabb.trim(),
                    trbcsosn == null ? "" : trbcsosn.trim(),
                    trbalq   == null ? "" : trbalq.trim()
                });
            }
        } catch (SQLException e) {
            LOG.warning("[CstCfopStep] Erro lendo TRIBUTACAO do Firebird: " + e.getMessage());
        } finally {
            close(rsFb); close(stFb);
        }
        ctx.put("mapaTributacao", mapaTrib);

        LOG.info("[CstCfopStep] CST-A=" + mapaCstA.size() + " CST-B=" + mapaCstB.size()
                 + " Tributacoes=" + mapaTrib.size());
        contarInseridos(ctx, 0);
    }
}
