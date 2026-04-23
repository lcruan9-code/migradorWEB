package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Clipp (Firebird) → lc_sistemas.produto
 *
 * Une TB_ESTOQUE (dados gerais) + TB_EST_PRODUTO (NCM/CST/barras/estoque).
 * Resolve FKs via mapas dos steps anteriores.
 */
@SuppressWarnings("unchecked")
public class ClippProdutoStep extends StepBase {

    @Override public String getNome() { return "ProdutoStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.produto");
        execIgnore(my, "ALTER TABLE lc_sistemas.produto AUTO_INCREMENT = 100000", "reset AI produto");

        Map<String, Integer>  mapaNcm     = ctx.getMapaNcm();
        Map<String, Integer>  mapaCest    = ctx.getMapaCest();
        Map<String, Integer>  mapaCateg   = ctx.getMapaCategoria();   // String(ID_GRUPO) → id_categ
        Map<String, Integer>  mapaUnidade = ctx.getMapaUnidade();
        Map<Integer,Integer>  mapaForn    = ctx.getMapaFornecedor();
        Map<String, Integer>  mapaSub     = (Map<String, Integer>) ctx.get("mapaSubcategoria"); // String(ID_SUBGRUPO)→id
        Map<String, Integer>  mapaCstA    = (Map<String, Integer>) ctx.get("mapaCstA");
        Map<String, Integer>  mapaCstB    = (Map<String, Integer>) ctx.get("mapaCstB");
        Map<Integer,Integer>  mapaCfop    = (Map<Integer,Integer>) ctx.get("mapaCfop");

        if (mapaSub  == null) mapaSub  = new HashMap<>();
        if (mapaCstA == null) mapaCstA = new HashMap<>();
        if (mapaCstB == null) mapaCstB = new HashMap<>();
        if (mapaCfop == null) mapaCfop = new HashMap<>();

        // ID de CFOP padrão (5102 ou 289 = fallback)
        final int CFOP_PADRAO_VENDA  = 5102;
        final int CFOP_PADRAO_ST     = 5405;
        final int CFOP_FALLBACK_ID   = 289;

        int ins = 0, err = 0;
        PreparedStatement pst = null;
        Statement stFb = null; ResultSet rs = null;
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT e.ID_ESTOQUE, e.DESCRICAO, e.UNI_MEDIDA, e.PRC_VENDA, e.PRC_CUSTO, " +
                "e.PRC_ATACADO, e.QTD_ATACADO, e.MARGEM_LB, e.STATUS, e.DT_CADAST, " +
                "e.ID_GRUPO, e.ID_SUBGRUPO, e.CFOP, e.CST_PIS, e.CST_COFINS, e.ID_TIPOITEM, " +
                "e.ULT_FORNEC, " +
                "p.COD_BARRA, p.REFERENCIA, p.COD_NCM, p.COD_CEST, p.CST, p.CSOSN, " +
                "p.IPI, p.QTD_ATUAL, p.QTD_MINIM, p.PESO, p.ID_NIVEL1 " +
                "FROM TB_ESTOQUE e " +
                "LEFT JOIN TB_EST_PRODUTO p ON p.ID_IDENTIFICADOR = e.ID_ESTOQUE " +
                "WHERE CHAR_LENGTH(TRIM(e.DESCRICAO)) >= 1");

            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.produto(" +
                "pode_fracionado,pode_balanca,datahora_cadastro,datahora_alteracao,estoque_minimo,estoque_max," +
                "codigo,nome,descricao,preco_venda,preco_custo," +
                "preco_venda2,qtd_minimapv2,preco_venda3,qtd_minimapv3," +
                "trib_genero,codigo_barras,referencia," +
                "id_ncm,id_cest,id_categoria,id_subcategoria,id_unidade," +
                "estoque,id_cfop,id_cst,trib_icmsaliqsaida,id_fornecedor,id_empresa)" +
                " VALUES (?,?,?,NOW(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)");

            while (rs.next()) {
                int    idEst   = rs.getInt(1);
                String descr   = rs.getString(2);
                String und     = rs.getString(3);
                double prcVda  = rs.getDouble(4);
                double prcCst  = rs.getDouble(5);
                double prcAtac = rs.getDouble(6);
                double qtdAtac = rs.getDouble(7);
                double margem  = rs.getDouble(8);
                String status  = rs.getString(9);
                Date   dtCad   = safeGetDate(rs, 10);
                int    idGrp   = rs.getInt(11);
                int    idSub   = rs.getInt(12);
                String cfopStr = rs.getString(13);
                String cstPis  = rs.getString(14);
                String cstCof  = rs.getString(15);
                String tipoItem= rs.getString(16);
                int    ultForn = rs.getInt(17);

                String codBarra= rs.getString(18);
                String ref     = rs.getString(19);
                String ncm     = rs.getString(20);
                String cest    = rs.getString(21);
                String cst     = rs.getString(22);
                String csosn   = rs.getString(23);
                double ipi     = rs.getDouble(24);
                double qtdAtual= rs.getDouble(25);
                double estMin  = rs.getDouble(26);
                double peso    = rs.getDouble(27);

                // Resolve FKs
                if (ncm  != null) ncm  = ncm.trim().replaceAll("[^0-9]","");
                if (cest != null) cest = cest.trim().replaceAll("[^0-9]","");
                Integer idNcm   = (ncm  != null && !ncm.isEmpty())  ? mapaNcm.get(ncm)   : null;
                Integer idCest  = (cest != null && !cest.isEmpty()) ? mapaCest.get(cest) : null;
                Integer idCateg = mapaCateg.get(String.valueOf(idGrp));
                Integer idSubc  = mapaSub.get(String.valueOf(idSub));
                Integer idUnid  = (und != null) ? mapaUnidade.get(und.trim()) : null;
                Integer idForn  = mapaForn.get(ultForn);

                // CST/CFOP
                Integer idCstVal = null;
                int     idCfopVal= CFOP_FALLBACK_ID;
                if (cst != null && !cst.trim().isEmpty()) {
                    idCstVal = mapaCstA.get(cst.trim());
                }
                if (idCstVal == null && csosn != null && !csosn.trim().isEmpty()) {
                    idCstVal = mapaCstB.get(csosn.trim());
                }
                if (idCstVal == null) idCstVal = mapaCstB.get("102");

                int cfopCode = CFOP_PADRAO_VENDA;
                if (cfopStr != null) { try { cfopCode = Integer.parseInt(cfopStr.trim()); } catch (NumberFormatException ignored) {} }
                Integer cfopId = mapaCfop.get(cfopCode);
                if (cfopId != null) idCfopVal = cfopId;
                else { cfopId = mapaCfop.get(CFOP_PADRAO_VENDA); if (cfopId != null) idCfopVal = cfopId; }

                // trib_genero: S=09(serviço), C=03(comb), else 00
                String tribGenero = "00";
                if ("S".equals(tipoItem)) tribGenero = "09";
                else if ("C".equals(tipoItem)) tribGenero = "03";

                String aliqIcms = "";

                // Normaliza barras
                String barras = codBarra != null ? codBarra.trim().replaceAll("^0+","") : "";
                String codigoBarras = barras.length() <= 14 ? barras : "";
                String referencia   = ref != null && !ref.trim().isEmpty() ? ref.trim() : (barras.length() > 14 ? barras : "");

                if (prcCst > 1_000_000.0) prcCst = 0.0;
                if (qtdAtual > 1_000_000.0) qtdAtual = 0.0;
                String fracionado = "N";

                try {
                    Timestamp ts = dtCad != null ? new Timestamp(dtCad.getTime()) : new Timestamp(System.currentTimeMillis());
                    pst.setString(1, fracionado); pst.setString(2, fracionado);
                    pst.setTimestamp(3, ts);
                    pst.setDouble(4, estMin); pst.setDouble(5, 0.0);
                    pst.setString(6, String.valueOf(idEst));
                    pst.setString(7, descr.trim()); pst.setString(8, descr.trim());
                    pst.setDouble(9, prcVda); pst.setDouble(10, prcCst);
                    pst.setDouble(11, prcAtac); pst.setDouble(12, qtdAtac);
                    pst.setDouble(13, 0.0); pst.setDouble(14, 0.0);
                    pst.setString(15, tribGenero);
                    pst.setString(16, codigoBarras); pst.setString(17, referencia);
                    if (idNcm   != null) pst.setInt(18, idNcm);   else pst.setNull(18, Types.INTEGER);
                    if (idCest  != null) pst.setInt(19, idCest);  else pst.setNull(19, Types.INTEGER);
                    if (idCateg != null) pst.setInt(20, idCateg); else pst.setNull(20, Types.INTEGER);
                    if (idSubc  != null) pst.setInt(21, idSubc);  else pst.setNull(21, Types.INTEGER);
                    if (idUnid  != null) pst.setInt(22, idUnid);  else pst.setNull(22, Types.INTEGER);
                    pst.setDouble(23, qtdAtual);
                    pst.setInt(24, idCfopVal);
                    if (idCstVal!= null) pst.setInt(25, idCstVal); else pst.setNull(25, Types.INTEGER);
                    pst.setString(26, aliqIcms);
                    if (idForn  != null) pst.setInt(27, idForn);  else pst.setNull(27, Types.INTEGER);
                    pst.executeUpdate(); ins++;
                } catch (SQLException e) {
                    LOG.warning("[ClippProdutoStep] id=" + idEst + ": " + e.getMessage()); err++;
                }
                if ((ins + err) % 200 == 0) { try { my.commit(); } catch (SQLException ex) {} }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo TB_ESTOQUE/TB_EST_PRODUTO: " + e.getMessage(), e);
        } finally { close(rs); close(stFb); close(pst); }

        contarInseridos(ctx, ins); contarErros(ctx, err);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.produto", "rollback ClippProdutoStep");
    }
}
