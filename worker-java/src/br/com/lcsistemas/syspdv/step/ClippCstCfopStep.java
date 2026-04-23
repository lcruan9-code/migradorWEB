package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Clipp — Pré-carrega mapas CST/CFOP do MySQL.
 * Não lê TRIBUTACAO do Firebird (Clipp não tem essa tabela).
 * mapaTributacao fica vazio — tributação será resolvida pelo ProdutoStep via CST/CSOSN.
 */
public class ClippCstCfopStep extends StepBase {

    @Override public String getNome() { return "CstCfopStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection my = ctx.getDestinoConn();

        Map<String, Integer> mapaCstA = new HashMap<>();
        Map<String, Integer> mapaCstB = new HashMap<>();
        Map<Integer, Integer> mapaCfop = new HashMap<>();

        Statement stMy = null; ResultSet rsMy = null;
        try {
            stMy = my.createStatement();
            rsMy = stMy.executeQuery("SELECT id, codigotributario, tabela FROM lc_sistemas.cst");
            while (rsMy.next()) {
                int id = rsMy.getInt(1); String cod = rsMy.getString(2); String tab = rsMy.getString(3);
                if ("A".equals(tab)) mapaCstA.put(cod, id);
                else if ("B".equals(tab)) mapaCstB.put(cod, id);
            }
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo lc_sistemas.cst: " + e.getMessage(), e);
        } finally { close(rsMy); close(stMy); }

        // Mapa CFOP código (int) → id em lc_sistemas.cfop
        Statement stCfop = null; ResultSet rsCfop = null;
        try {
            stCfop = my.createStatement();
            rsCfop = stCfop.executeQuery("SELECT id, cfop FROM lc_sistemas.cfop");
            while (rsCfop.next()) {
                try { mapaCfop.put(Integer.parseInt(rsCfop.getString(2).trim()), rsCfop.getInt(1)); }
                catch (NumberFormatException ignored) {}
            }
        } catch (SQLException e) { LOG.warning("[ClippCstCfopStep] cfop: " + e.getMessage()); }
        finally { close(rsCfop); close(stCfop); }

        ctx.put("mapaCstA", mapaCstA);
        ctx.put("mapaCstB", mapaCstB);
        ctx.put("mapaCfop", mapaCfop);
        ctx.put("mapaTributacao", new HashMap<String, String[]>());

        LOG.info("[ClippCstCfopStep] CST-A=" + mapaCstA.size() + " CST-B=" + mapaCstB.size() + " CFOP=" + mapaCfop.size());
        contarInseridos(ctx, 0);
    }
}
