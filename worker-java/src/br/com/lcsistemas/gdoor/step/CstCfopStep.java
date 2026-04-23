package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolve o mapeamento de ST (Situação Tributária do GDOOR) para
 * id_cst + id_cfop no destino lc_sistemas.
 *
 * Carrega mapas do MySQL (lc_sistemas.cst e lc_sistemas.cfop) em memória
 * e armazena no contexto para uso pelo ProdutoStep.
 */
public class CstCfopStep extends StepBase {

    @Override
    public String getNome() { return "CstCfopStep"; }

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
        execIgnore(ctx.getDestinoConn(),
            "CREATE INDEX idx_cst_codigotributario ON lc_sistemas.cst (codigotributario)",
            "idx_cst_codigotributario");
        execIgnore(ctx.getDestinoConn(),
            "CREATE INDEX idx_cst_tabela ON lc_sistemas.cst (tabela)",
            "idx_cst_tabela");
    }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        // Carrega mapa CST Tabela A e B do MySQL
        // mapaCst: codigotributario -> id (prefere Tabela A)
        Map<String, Integer> mapaCstA = new HashMap<>();
        Map<String, Integer> mapaCstB = new HashMap<>();
        Map<Integer, Integer> mapaCfop = new HashMap<>();

        Statement st = null; ResultSet rs = null;
        try {
            st = ctx.getDestinoConn().createStatement();

            rs = st.executeQuery("SELECT id, codigotributario FROM lc_sistemas.cst WHERE tabela='A'");
            while (rs.next()) mapaCstA.put(rs.getString(2), rs.getInt(1));
            rs.close();

            rs = st.executeQuery("SELECT id, codigotributario FROM lc_sistemas.cst WHERE tabela='B'");
            while (rs.next()) mapaCstB.put(rs.getString(2), rs.getInt(1));
            rs.close();

            rs = st.executeQuery("SELECT id, codigocfop FROM lc_sistemas.cfop");
            while (rs.next()) mapaCfop.put(rs.getInt(2), rs.getInt(1));
            rs.close();

        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao carregar CST/CFOP: " + e.getMessage(), e);
        } finally {
            close(rs); close(st);
        }

        // Guarda mapas A e B no contexto. ProdutoStep decidirá qual usar 
        // com base em ctx.getConfig().getRegimeTributario()
        ctx.put("mapaCstA", mapaCstA);
        ctx.put("mapaCstB", mapaCstB);

        ctx.setMapaCfop(mapaCfop);

        // IDs de CFOP fixos para os steps de produto
        // 289 = NF entrada padrão, 355 = ST
        int cfop289 = mapaCfop.containsKey(289) ? mapaCfop.get(289) : 0;
        int cfop355 = mapaCfop.containsKey(355) ? mapaCfop.get(355) : 0;
        ctx.put("cfop289", cfop289);
        ctx.put("cfop355", cfop355);

        // IDs CST mais usados (fallback)
        ctx.put("cstA_00", mapaCstA.containsKey("00") ? mapaCstA.get("00") : 0);
        ctx.put("cstB_102", mapaCstB.containsKey("102") ? mapaCstB.get("102") : 0);

        int total = mapaCstA.size() + mapaCstB.size();
        contarInseridos(ctx, total);
        LOG.info("[CstCfopStep] Carregados " + total + " CSTs e " + mapaCfop.size() + " CFOPs em memória. Regime selecionado: " + ctx.getConfig().getRegimeTributario());
    }

    @Override
    public void cleanup(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DROP INDEX idx_cst_codigotributario ON lc_sistemas.cst",
            "drop idx_cst_codigotributario");
        execIgnore(ctx.getDestinoConn(),
            "DROP INDEX idx_cst_tabela ON lc_sistemas.cst",
            "drop idx_cst_tabela");
    }
}
