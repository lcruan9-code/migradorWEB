package br.com.lcsistemas.host.step;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;

import java.sql.*;
import java.util.Map;

/**
 * Migra produtos de host.produtos para lc_sistemas.produto.
 *
 * Script base (resumo):
 *   delete from lc_sistemas.produto;
 *   alter table lc_sistemas.produto auto_increment = 100000;
 *
 *   insert into lc_sistemas.produto(codigo, codigo_barras, nome, descricao, referencia,
 *     estoque, preco_venda, preco_compra, preco_custo, trib_genero, peso_liquido, peso_bruto,
 *     trib_icmsaliqredbasecalcsaida, ativo, estoque_minimo, comissao_valor, desconto_max,
 *     datahora_cadastro, origem_produto, trib_icmsaliqsaida, trib_pissaida, trib_pisaliqsaida,
 *     trib_cofinssaida, trib_cofinsaliqsaida, trib_ipisaida, trib_icmsfcpaliq, trib_ipialiqsaida,
 *     preco_venda2, local, preco_promocao, preco_venda3, id_ncm, id_cest, id_categoria,
 *     id_fabricante, id_unidade, id_cfop, id_cst, id_subcategoria, id_empresa)
 *   select id_produto, barras, produto, descricao_complementar, referencia,
 *     estoque, valor_venda, valor_compra, custo, substr(ncm,1,2), ...
 *   from host.produtos where length(trim(produto)) >= 1;
 *
 *   -- Correções de códigos:
 *   UPDATE lc_sistemas.produto set referencia='' where leading 0 from codigo_barras = leading 0 from referencia;
 *   UPDATE lc_sistemas.produto set codigo_barras='' where leading 0 from codigo_barras = leading 0 from codigo;
 *   UPDATE lc_sistemas.produto set referencia='' where leading 0 from referencia = leading 0 from codigo;
 */
public class ProdutoStep extends StepBase {

    @Override
    public String getNome() { return "ProdutoStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection origem  = ctx.getOrigemConn();
        Connection destino = ctx.getDestinoConn();

        // Limpa destino
        exec(destino, "DELETE FROM lc_sistemas.produto");
        exec(destino, "ALTER TABLE lc_sistemas.produto AUTO_INCREMENT = 100000");

        String regime = ctx.getConfig().getRegimeTributario();
        Object objCfop289 = ctx.get("cfop289");
        int cfop289 = objCfop289 != null ? (Integer) objCfop289 : 289;
        Object objCfop355 = ctx.get("cfop355");
        int cfop355 = objCfop355 != null ? (Integer) objCfop355 : 355;
        Object objCstA = ctx.get("cstA_00");
        int cstA_00 = objCstA != null ? (Integer) objCstA : 1;
        Object objCstB = ctx.get("cstB_102");
        int cstB_102= objCstB != null ? (Integer) objCstB : 1;
        
        @SuppressWarnings("unchecked")
        Map<String, Integer> mapaCstA = (Map<String, Integer>) ctx.get("mapaCstA");
        @SuppressWarnings("unchecked")
        Map<String, Integer> mapaCstB = (Map<String, Integer>) ctx.get("mapaCstB");

        String selectSql =
            "SELECT p.ID_PRODUTO, p.BARRAS, p.BARRAS_CX, p.PRODUTO, p.DESCRICAO_COMPLEMENTAR, p.REFERENCIA, p.GTIN," +
            " p.ESTOQUE, p.VALOR_VENDA, p.VALOR_COMPRA, p.CUSTO, p.NCM, p.CEST, p.PESO_LIQUIDO, p.PESO_BRUTO," +
            " p.RED_BASE_ICMS, p.STATUS, p.MINIMO, p.COMISSAO_VISTA, p.DESCONTO, p.DT_CADASTRO, p.ORIGEM," +
            " p.ICMS, p.CST_PISCOFINS_SAIDA, p.ALIQ_PIS_SAIDA, p.ALIQ_COFINS_SAIDA, p.CST_IPI, p.ALIQ_FCP," +
            " p.IPI_PERC, p.VALOR_ATACADO, p.LOCALIZACAO, p.VALOR_PROMOCIONAL, p.VALOR_APRAZO," +
            " p.UNIDADE_COMECIAL, p.CST, p.CSOSN," +
            " g.GRUPO AS NOME_GRUPO, m.MARCA AS NOME_MARCA" +
            " FROM PRODUTOS p" +
            " LEFT JOIN PRODUTOS_GRUPO g ON p.GRUPO = g.ID" +
            " LEFT JOIN PRODUTOS_MARCA m ON p.MARCA = m.ID" +
            " WHERE p.PRODUTO IS NOT NULL AND TRIM(p.PRODUTO) <> ''";

        String insertSql =
            "INSERT INTO lc_sistemas.produto" +
            "  (codigo, codigo_barras, nome, descricao, referencia," +
            "   estoque, preco_venda, preco_compra, preco_custo, trib_genero," +
            "   peso_liquido, peso_bruto, trib_icmsaliqredbasecalcsaida," +
            "   ativo, estoque_minimo, comissao_valor, desconto_max," +
            "   datahora_cadastro, origem_produto, trib_icmsaliqsaida," +
            "   trib_pissaida, trib_pisaliqsaida," +
            "   trib_cofinssaida, trib_cofinsaliqsaida," +
            "   trib_ipisaida, trib_icmsfcpaliq, trib_ipialiqsaida," +
            "   preco_venda2, local, preco_promocao, preco_venda3," +
            "   id_ncm, id_cest, id_categoria, id_fabricante, id_unidade," +
            "   id_cfop, id_cst, id_subcategoria, id_empresa)" +
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        int ins = 0;
        Statement stO = null; ResultSet rsO = null;
        PreparedStatement psI = null;

        try {
            stO  = origem.createStatement();
            rsO  = stO.executeQuery(selectSql);
            psI  = destino.prepareStatement(insertSql);

            int batch = 0;
            while (rsO.next()) {
                String codigo        = String.valueOf(rsO.getLong("ID_PRODUTO"));
                String barras        = nvl(rsO.getString("BARRAS"));
                String barrasCx      = nvl(rsO.getString("BARRAS_CX"));
                String codigoBarras  = barras.length() >= 1 ? barras : barrasCx;
                String nome          = nvl(rsO.getString("PRODUTO"));
                String descricao     = nvl(rsO.getString("DESCRICAO_COMPLEMENTAR"));
                String refStr        = nvl(rsO.getString("REFERENCIA"));
                String gtin          = nvl(rsO.getString("GTIN"));
                String referencia    = refStr.length() >= 1 ? refStr : (refStr.equals(gtin) ? "" : gtin);
                double estoque       = rsO.getDouble("ESTOQUE");
                double precoVenda    = rsO.getDouble("VALOR_VENDA");
                double precoCompra   = rsO.getDouble("VALOR_COMPRA");
                double precoCusto    = rsO.getDouble("CUSTO");
                String ncmRaw        = nvl(rsO.getString("NCM")).replace(",", "").replace(".", "");
                String tribGenero    = ncmRaw.length() >= 2 ? ncmRaw.substring(0, 2) : "00";
                double pesoLiq       = rsO.getDouble("PESO_LIQUIDO");
                double pesoBruto     = rsO.getDouble("PESO_BRUTO");
                double redBase       = rsO.getDouble("RED_BASE_ICMS");
                String status        = nvl(rsO.getString("STATUS"));
                int    ativo         = "INATIVO".equals(status) ? 0 : 1;
                double minimo        = rsO.getDouble("MINIMO");
                double comissao      = rsO.getDouble("COMISSAO_VISTA");
                double desconto      = rsO.getDouble("DESCONTO");
                String dtCadastro    = nvl(rsO.getString("DT_CADASTRO")); 
                // Formato de data p/ java.sql.Timestamp (Firebird retorna yyyy-MM-dd freq)
                if (!dtCadastro.isEmpty() && dtCadastro.length() == 10) dtCadastro += " 00:00:00";
                
                String origem2       = nvl(rsO.getString("ORIGEM"));
                double icms          = rsO.getDouble("ICMS");
                String pisCofins     = nvl(rsO.getString("CST_PISCOFINS_SAIDA"));
                if (pisCofins.length() == 1) pisCofins = "0" + pisCofins;
                String pisSaida      = pisCofins;
                double aliqPis       = rsO.getDouble("ALIQ_PIS_SAIDA");
                String cofinsSaida   = pisCofins;
                double aliqCofins    = rsO.getDouble("ALIQ_COFINS_SAIDA");
                String cstIpi        = nvl(rsO.getString("CST_IPI"));
                double aliqFcp       = rsO.getDouble("ALIQ_FCP");
                double ipiPerc       = rsO.getDouble("IPI_PERC");
                double precoAtac     = rsO.getDouble("VALOR_ATACADO");
                String local         = nvl(rsO.getString("LOCALIZACAO"));
                double precoPromo    = rsO.getDouble("VALOR_PROMOCIONAL");
                double precoAprazo   = rsO.getDouble("VALOR_APRAZO");

                // Mapeamentos Java
                int idNcm = 1;
                if (ctx.getMapaNcm() != null && ctx.getMapaNcm().containsKey(ncmRaw)) {
                    idNcm = ctx.getMapaNcm().get(ncmRaw);
                }

                int idCest = 1;
                String cestRaw = nvl(rsO.getString("CEST"));
                if (ctx.getMapaCest() != null && ctx.getMapaCest().containsKey(cestRaw)) {
                    idCest = ctx.getMapaCest().get(cestRaw);
                }

                int idCategoria = 1;
                String nomeGrupo = nvl(rsO.getString("NOME_GRUPO"));
                if (ctx.getMapaCategoria() != null && ctx.getMapaCategoria().containsKey(nomeGrupo)) {
                    idCategoria = ctx.getMapaCategoria().get(nomeGrupo);
                }

                int idFabricante = 1;
                String nomeMarca = nvl(rsO.getString("NOME_MARCA"));
                if (ctx.getMapaFabricante() != null && ctx.getMapaFabricante().containsKey(nomeMarca)) {
                    idFabricante = ctx.getMapaFabricante().get(nomeMarca);
                }

                int idUnidade = 1;
                String nomeUnd = nvl(rsO.getString("UNIDADE_COMECIAL"));
                if (ctx.getMapaUnidade() != null && ctx.getMapaUnidade().containsKey(nomeUnd)) {
                    idUnidade = ctx.getMapaUnidade().get(nomeUnd);
                }

                // Subcategoria n é mapeada real, entao padrao
                int idSubcat = 1;

                // CST e CFOP
                int idCst = 1;
                int idCfop = cfop289;
                if ("SIMPLES".equalsIgnoreCase(regime)) {
                    int csosn = rsO.getInt("CSOSN");
                    String csosnStr = String.valueOf(csosn);
                    if (mapaCstB != null && mapaCstB.containsKey(csosnStr)) idCst = mapaCstB.get(csosnStr);
                    else idCst = cstB_102;
                    if (csosn == 60 || csosn == 500) idCfop = cfop355;
                } else {
                    String pCst = nvl(rsO.getString("CST"));
                    if (pCst.isEmpty()) pCst = "00";
                    if (pCst.length() == 3) pCst = pCst.substring(1, 3);
                    if (mapaCstA != null && mapaCstA.containsKey(pCst)) idCst = mapaCstA.get(pCst);
                    else idCst = cstA_00;

                    int csosn = rsO.getInt("CSOSN");
                    if (csosn == 60 || csosn == 500 || csosn == 560 || csosn == 260 || csosn == 160) {
                        idCfop = cfop355;
                    }
                }

                psI.setString(1,  codigo);
                psI.setString(2,  codigoBarras);
                psI.setString(3,  nome);
                psI.setString(4,  descricao);
                psI.setString(5,  referencia);
                psI.setDouble(6,  estoque);
                psI.setDouble(7,  precoVenda);
                psI.setDouble(8,  precoCompra);
                psI.setDouble(9,  precoCusto);
                psI.setString(10, tribGenero.isEmpty() ? "00" : tribGenero);
                psI.setDouble(11, pesoLiq);
                psI.setDouble(12, pesoBruto);
                psI.setDouble(13, redBase);
                psI.setInt(14,    ativo);
                psI.setDouble(15, minimo);
                psI.setDouble(16, comissao);
                psI.setDouble(17, desconto);
                if (!dtCadastro.isEmpty()) psI.setString(18, dtCadastro);
                else psI.setNull(18, Types.TIMESTAMP);
                psI.setString(19, origem2);
                psI.setDouble(20, icms);
                psI.setString(21, pisSaida.isEmpty() ? "07" : pisSaida);
                psI.setDouble(22, aliqPis);
                psI.setString(23, cofinsSaida.isEmpty() ? "07" : cofinsSaida);
                psI.setDouble(24, aliqCofins);
                psI.setString(25, cstIpi);
                psI.setDouble(26, aliqFcp);
                psI.setDouble(27, ipiPerc);
                psI.setDouble(28, precoAtac);
                psI.setString(29, local);
                psI.setDouble(30, precoPromo);
                psI.setDouble(31, precoAprazo);
                psI.setInt(32,    idNcm);
                psI.setInt(33,    idCest);
                psI.setInt(34,    idCategoria);
                psI.setInt(35,    idFabricante);
                psI.setInt(36,    idUnidade);
                psI.setInt(37,    idCfop);
                psI.setInt(38,    idCst);
                psI.setInt(39,    idSubcat);
                psI.setInt(40,    ID_EMPRESA);

                psI.addBatch();
                batch++;

                if (batch % 500 == 0) {
                    psI.executeBatch();
                    LOG.info("[ProdutoStep] " + batch + " produtos inseridos...");
                }
            }
            if (batch % 500 != 0) psI.executeBatch();
            ins = batch;

        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar produtos: " + e.getMessage(), e);
        } finally {
            close(rsO); close(stO); close(psI);
        }

        contarInseridos(ctx, ins);
        LOG.info("[ProdutoStep] " + ins + " produtos inseridos.");

        // Correções de códigos duplicados
        LOG.info("[ProdutoStep] Aplicando correções de códigos...");
        execIgnore(destino,
            "UPDATE lc_sistemas.produto SET referencia='' WHERE (TRIM(LEADING '0' FROM codigo_barras)) = (TRIM(LEADING '0' FROM referencia))",
            "corrigir referencia=barras");
        execIgnore(destino,
            "UPDATE lc_sistemas.produto SET codigo_barras='' WHERE (TRIM(LEADING '0' FROM codigo_barras)) = (TRIM(LEADING '0' FROM codigo))",
            "corrigir barras=codigo");
        execIgnore(destino,
            "UPDATE lc_sistemas.produto SET referencia='' WHERE (TRIM(LEADING '0' FROM referencia)) = (TRIM(LEADING '0' FROM codigo))",
            "corrigir referencia=codigo");
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.produto", "rollback ProdutoStep");
    }

    @Override
    public void cleanup(MigracaoContext ctx) {
        // Colunas temporárias adicionadas em prepare dos outros steps
        // Não dropa aqui para evitar conflito — cada step cuida da sua
    }

    private String nvl(String s) { return s == null ? "" : s.trim(); }
}
