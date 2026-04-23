package br.com.lcsistemas.host.step;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolve o mapeamento de CST (cst/csosn de host.produtos) para
 * id_cst + id_cfop no destino lc_sistemas.
 *
 * Script base:
 *   -- SIMPLES (tabela B):
 *   update host.produtos p inner join lc_sistemas.cst c on p.csosn = c.codigotributario
 *   set p.id_cst = c.id,
 *       p.id_cfop = CASE p.csosn WHEN 60 THEN '355' WHEN 500 THEN '355' ELSE '289' END
 *   where c.tabela = 'B';
 *
 *   -- NORMAL (tabela A):
 *   update host.produtos p inner join lc_sistemas.cst c on c.codigotributario = (...)
 *   set p.id_cst = c.id, p.id_cfop = CASE p.csosn ... END
 *   where c.tabela = 'A';
 */
public class CstCfopStep extends StepBase {

    @Override
    public String getNome() { return "CstCfopStep"; }

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
        addTempColumn(ctx.getOrigemConn(), "host", "produtos", "id_cst",  "INT(11)", null);
        addTempColumn(ctx.getOrigemConn(), "host", "produtos", "id_cfop", "INT(11)", null);
    }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection destino = ctx.getDestinoConn();
        String regime = ctx.getConfig().getRegimeTributario();

        // Carrega mapas CST e CFOP do destino em memória
        Map<String, Integer> mapaCstA = new HashMap<String, Integer>();
        Map<String, Integer> mapaCstB = new HashMap<String, Integer>();
        Map<Integer, Integer> mapaCfop = new HashMap<Integer, Integer>();

        try {
            Statement st = destino.createStatement();
            ResultSet rs = st.executeQuery("SELECT id, codigotributario FROM lc_sistemas.cst WHERE tabela='A'");
            while (rs.next()) mapaCstA.put(rs.getString(2), rs.getInt(1));
            rs.close();
            rs = st.executeQuery("SELECT id, codigotributario FROM lc_sistemas.cst WHERE tabela='B'");
            while (rs.next()) mapaCstB.put(rs.getString(2), rs.getInt(1));
            rs.close();
            rs = st.executeQuery("SELECT id, codigocfop FROM lc_sistemas.cfop");
            while (rs.next()) mapaCfop.put(rs.getInt(2), rs.getInt(1));
            rs.close();
            st.close();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao carregar CST/CFOP: " + e.getMessage(), e);
        }

        ctx.put("mapaCstA", mapaCstA);
        ctx.put("mapaCstB", mapaCstB);
        ctx.setMapaCfop(mapaCfop);

        int cfop289 = mapaCfop.containsKey(289) ? mapaCfop.get(289) : 289;
        int cfop355 = mapaCfop.containsKey(355) ? mapaCfop.get(355) : 355;
        ctx.put("cfop289", cfop289);
        ctx.put("cfop355", cfop355);
        ctx.put("cstA_00",  mapaCstA.containsKey("00")  ? mapaCstA.get("00")  : 1);
        ctx.put("cstB_102", mapaCstB.containsKey("102") ? mapaCstB.get("102") : 1);

        LOG.info("[CstCfopStep] Regime: " + regime + ". Atualizações de id_cst/id_cfop ocorrerão no Java (em ProdutoStep).");

        int total = mapaCstA.size() + mapaCstB.size();
        contarInseridos(ctx, total);
        LOG.info("[CstCfopStep] Carregados " + total + " CSTs e " + mapaCfop.size() + " CFOPs em memória.");
    }

    @Override
    public void cleanup(MigracaoContext ctx) {
        dropTempColumn(ctx.getOrigemConn(), "host", "produtos", "id_cst");
        dropTempColumn(ctx.getOrigemConn(), "host", "produtos", "id_cfop");
    }
}
