package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;

import java.sql.*;
import java.util.Map;

/**
 * Migra o estoque do Firebird (ESTOQUE) para lc_sistemas.produto.
 *
 * Usa os mapas em memória carregados pelos steps anteriores:
 *   - mapaNcm       (codNcm     -> id_ncm)
 *   - mapaUnidade   (und        -> id_unidade)
 *   - mapaCestById  (idCestFb   -> id_cest_mysql)
 *   - mapaCategoria (grupo      -> id_categoria)
 *   - mapaFornecedor(codForn    -> id_fornecedor_mysql)
 *   - mapaCst       (codigoCst  -> id_cst_mysql)
 *   - mapaCfop      (codigoCfop -> id_cfop_mysql)
 */
public class ProdutoStep extends StepBase {

    @Override
    public String getNome() { return "ProdutoStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Map<String, Integer>  mapaNcm        = ctx.getMapaNcm();
        Map<String, Integer>  mapaUnidade    = ctx.getMapaUnidade();
        Map<Integer, Integer> mapaCestById   = ctx.getMapaCestById();
        Map<String, Integer>  mapaCategoria  = ctx.getMapaCategoria();
        Map<Integer, Integer> mapaFornecedor = ctx.getMapaFornecedor();
        
        String regime = ctx.getConfig().getRegimeTributario();
        @SuppressWarnings("unchecked")
        Map<String, Integer> mapaCst = (Map<String, Integer>) (regime.equals("NORMAL") ? ctx.get("mapaCstA") : ctx.get("mapaCstB"));

        // Limpa destino
        exec(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.produto");
        exec(ctx.getDestinoConn(), "ALTER TABLE lc_sistemas.produto AUTO_INCREMENT = 100000");

        String selectSql =
            "SELECT CODIGO, BARRAS, DESCRICAO, UND, COD_NCM, COD_CEST, GRUPO," +
            "       FORNECEDOR AS COD_FORN, ST, PIS_CODIGO, COFINS_CODIGO," +
            "       QTD, PRECO_CUSTO AS CUSTOUNITARIO, PRECO_VENDA AS PRECOVENDA, PRECO_ATACADO AS PRECOATACADO," +
            "       COD_ANP, COD_FABRICANTE," +
            "       SITUACAO, DATA_CADASTRO, CARACTERISTICAS, COR, TAMANHO," +
            "       PRECO_CUSTO " +
            "FROM ESTOQUE " +
            "WHERE DESCRICAO IS NOT NULL AND CHAR_LENGTH(DESCRICAO) >= 1";

        String insertSql =
            "INSERT INTO lc_sistemas.produto " +
            "  (trib_genero, ativo, codigo, pode_balanca, pode_fracionado, referencia," +
            "   datahora_cadastro, datahora_alteracao, codigo_barras, nome, descricao," +
            "   estoque, custo_medio, preco_custo, preco_venda, preco_venda2," +
            "   trib_pissaida, trib_cofinssaida, id_fornecedor, id_cst, id_cfop," +
            "   id_unidade, id_ncm, id_cest, id_categoria, comb_cprodanp, id_empresa) " +
            "VALUES (?,?,?,?,?,?,?,NOW(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        int ins = 0;
        Statement stFb = null; ResultSet rsFb = null;
        PreparedStatement psIns = null;

        try {
            stFb  = ctx.getOrigemConn().createStatement();
            rsFb  = stFb.executeQuery(selectSql);
            psIns = ctx.getDestinoConn().prepareStatement(insertSql);

            int batch = 0;
            while (rsFb.next()) {
                String barras      = nvl(rsFb.getString("BARRAS"));
                String descricao   = nvl(rsFb.getString("DESCRICAO"));
                String und         = nvl(rsFb.getString("UND")).trim();
                if (und.isEmpty()) und = "UN";
                String codNcm      = nvl(rsFb.getString("COD_NCM")).trim();
                int    codCest     = 0;
                try { codCest = Integer.parseInt(nvl(rsFb.getString("COD_CEST")).trim()); } catch(Exception ignored) {}
                String grupo       = nvl(rsFb.getString("GRUPO")).trim().toUpperCase();
                int    codForn     = 0;
                try { codForn = Integer.parseInt(nvl(rsFb.getString("COD_FORN")).trim()); } catch(Exception ignored) {}
                String st          = nvl(rsFb.getString("ST")).trim();
                double qtd         = rsFb.getDouble("QTD");
                double custo       = rsFb.getDouble("CUSTOUNITARIO");
                double precoCusto  = rsFb.getDouble("PRECO_CUSTO");
                double precoVenda  = rsFb.getDouble("PRECOVENDA");
                double precoAtac   = rsFb.getDouble("PRECOATACADO");
                String tribGenero  = codNcm.length() >= 2 ? codNcm.substring(0, 2) : "00";
                String trib_pis    = nvl(rsFb.getString("PIS_CODIGO"));
                String trib_cof    = nvl(rsFb.getString("COFINS_CODIGO"));
                String codAnp      = nvl(rsFb.getString("COD_ANP"));
                String codFabr     = nvl(rsFb.getString("COD_FABRICANTE"));
                String situacao    = nvl(rsFb.getString("SITUACAO"));
                java.sql.Date dataCad = rsFb.getDate("DATA_CADASTRO");
                String caract      = nvl(rsFb.getString("CARACTERISTICAS"));
                String cor         = nvl(rsFb.getString("COR"));
                String tamanho     = nvl(rsFb.getString("TAMANHO"));
                int    codigoProd  = 0;
                try { codigoProd = Integer.parseInt(nvl(rsFb.getString("CODIGO")).trim()); } catch(Exception ignored) {}

                // Resolução de IDs via mapas
                int idNcm       = mapaNcm.getOrDefault(codNcm, 1);
                int idUnidade   = mapaUnidade.getOrDefault(und.isEmpty() ? "UN" : und, 1);
                int idCest      = mapaCestById.getOrDefault(codCest, 1);
                int idCategoria = mapaCategoria.getOrDefault(grupo, 1);
                int idForn      = mapaFornecedor.getOrDefault(codForn, 1);

                // CST: normaliza o código ST do GDOOR para o formato da tabela cst
                String cstNorm = normalizarCst(st, regime);
                int    idCst   = mapaCst.getOrDefault(cstNorm, regime.equals("NORMAL") ? 
                                     ((int) ctx.get("cstA_00") == 0 ? 1 : (int) ctx.get("cstA_00")) : 
                                     ((int) ctx.get("cstB_102") == 0 ? 1 : (int) ctx.get("cstB_102")));

                // CFOP: 355 para CSTs de Substituição, senão 289
                int idCfop = (st.equals("60") || st.equals("500")) ? 355 : 289;

                // Campos de balança
                boolean balanca = barras.length() == 6
                    || und.equalsIgnoreCase("KG")
                    || grupo.equalsIgnoreCase("BALANCA")
                    || grupo.equalsIgnoreCase("BALAN\u00C7A");
                String codigoProdStr = balanca
                    ? (barras.length() > 4 ? barras.substring(barras.length() - 4) : barras)
                    : String.valueOf(codigoProd);
                String codigoBarras = barras.length() > 14 ? "" : barras;
                String referencia   = barras.length() > 14 ? barras : codFabr;

                // Descrição completa
                StringBuilder desc = new StringBuilder(caract);
                if (!cor.isEmpty())     desc.append(" - ").append(cor);
                if (!tamanho.isEmpty()) desc.append(" - ").append(tamanho);

                // Sanitização de números
                if (qtd > 1000000 || qtd < -1000000) qtd = 0;
                if (precoCusto > 1000000) precoCusto = 0;

                String datahora = dataCad != null ? dataCad.toString() + " " + new java.sql.Time(System.currentTimeMillis()).toString() : null;

                psIns.setString(1,  tribGenero);
                psIns.setInt(2,     situacao.equalsIgnoreCase("ATIVO") ? 1 : 0);
                psIns.setString(3,  codigoProdStr);
                psIns.setString(4,  balanca ? "S" : "N");
                psIns.setString(5,  balanca ? "S" : "N");
                psIns.setString(6,  referencia);
                if (datahora != null) psIns.setString(7, datahora);
                else psIns.setNull(7, Types.TIMESTAMP);
                psIns.setString(8,  codigoBarras.startsWith("200") ? "" : codigoBarras);
                psIns.setString(9,  descricao);
                psIns.setString(10, desc.toString());
                psIns.setDouble(11, qtd);
                psIns.setDouble(12, custo);
                psIns.setDouble(13, precoCusto);
                psIns.setDouble(14, precoVenda);
                psIns.setDouble(15, precoAtac);
                psIns.setString(16, trib_pis.isEmpty()  ? "07" : trib_pis);
                psIns.setString(17, trib_cof.isEmpty()  ? "07" : trib_cof);
                psIns.setInt(18,    idForn);
                psIns.setInt(19,    idCst);
                psIns.setInt(20,    idCfop);
                psIns.setInt(21,    idUnidade);
                psIns.setInt(22,    idNcm);
                psIns.setInt(23,    idCest);
                psIns.setInt(24,    idCategoria);
                psIns.setString(25, codAnp);
                psIns.setInt(26,    1);

                psIns.addBatch();
                batch++;

                if (batch % 500 == 0) {
                    psIns.executeBatch();
                    LOG.info("[ProdutoStep] " + batch + " produtos inseridos...");
                }
            }
            if (batch % 500 != 0) psIns.executeBatch();
            ins = batch;

        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar produtos: " + e.getMessage(), e);
        } finally {
            close(rsFb); close(stFb); close(psIns);
        }

        contarInseridos(ctx, ins);
    }

    /** Normaliza o código ST do GDOOR para o formato da tabela lc_sistemas.cst baseado no regime. */
    private String normalizarCst(String st, String regime) {
        if (st == null || st.isEmpty()) return regime.equals("NORMAL") ? "00" : "102";
        st = st.trim();
        
        if (regime.equals("NORMAL")) {
            switch (st) {
                case "400": return "41";
                case "500": return "60";
                case "102": return "00";
                default:    return st;
            }
        } else {
            switch (st) {
                case "0":
                case "10":
                case "20":  return "102";
                case "40":
                case "50":
                case "51":  return "300";
                case "60":  return "500";
                case "90":  return "900";
                default:    return st;
            }
        }
    }

    private String nvl(String s) { return s == null ? "" : s.trim(); }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.produto", "rollback ProdutoStep");
    }
}
