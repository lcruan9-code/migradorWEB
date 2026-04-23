package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * SYSPDV (Firebird) -> lc_sistemas.produto
 *
 * Le PRODUTO + TRIBUTACAO + ESTOQUE + PRODUTOAUX do Firebird.
 * Resolve todas as FKs usando mapas carregados pelos steps anteriores.
 * Per-row try-catch. Commit a cada 200 linhas.
 *
 * Colunas Firebird: PROCOD, PRODESRDZ, PRODES, PROPRCVDAVAR, PROPRCCST,
 *   PROPRCVDA2, PROQTDMINPRC2, PROPRCVDA3, PROQTDMINPRC3, GENCODIGO,
 *   PROPESVAR, PRODATCADINC, PRONCM, PROCEST, SECCOD, GRPCOD, PROUNID,
 *   TRBID, FORCOD, PROESTMIN, PROESTMAX
 */
@SuppressWarnings("unchecked")
public class ProdutoStep extends StepBase {

    @Override
    public String getNome() { return "ProdutoStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.produto");
        execIgnore(my, "ALTER TABLE lc_sistemas.produto AUTO_INCREMENT = 100000", "reset AI produto");

        Map<String, Integer>   mapaNcm     = ctx.getMapaNcm();
        Map<String, Integer>   mapaCest    = ctx.getMapaCest();
        Map<String, Integer>   mapaCateg   = ctx.getMapaCategoria();
        Map<String, Integer>   mapaUnidade = ctx.getMapaUnidade();
        Map<Integer, Integer>  mapaForn    = ctx.getMapaFornecedor();
        Map<String, Integer>   mapaSub     = (Map<String, Integer>)   ctx.get("mapaSubcategoria");
        Map<String, Integer>   mapaCstA    = (Map<String, Integer>)   ctx.get("mapaCstA");
        Map<String, Integer>   mapaCstB    = (Map<String, Integer>)   ctx.get("mapaCstB");
        Map<String, String[]>  mapaTrib    = (Map<String, String[]>)  ctx.get("mapaTributacao");

        if (mapaSub  == null) mapaSub  = new HashMap<String, Integer>();
        if (mapaCstA == null) mapaCstA = new HashMap<String, Integer>();
        if (mapaCstB == null) mapaCstB = new HashMap<String, Integer>();
        if (mapaTrib == null) mapaTrib = new HashMap<String, String[]>();

        // Mapa PROCOD -> estoque
        Map<Integer, Double> mapaEstoque = new HashMap<Integer, Double>();
        Statement stEst = null; ResultSet rsEst = null;
        try {
            stEst = fb.createStatement();
            rsEst = stEst.executeQuery("SELECT PROCOD, ESTATU FROM ESTOQUE");
            while (rsEst.next()) mapaEstoque.put(rsEst.getInt(1), rsEst.getDouble(2));
        } catch (SQLException e) {
            LOG.warning("[ProdutoStep] Erro lendo ESTOQUE: " + e.getMessage());
        } finally { close(rsEst); close(stEst); }

        // Mapa PROCOD -> primeiro PROCODAUX (sem zeros a esquerda)
        Map<Integer, String> mapaBarras = new HashMap<Integer, String>();
        Statement stAux = null; ResultSet rsAux = null;
        try {
            stAux = fb.createStatement();
            rsAux = stAux.executeQuery("SELECT PROCOD, PROCODAUX FROM PRODUTOAUX");
            while (rsAux.next()) {
                int procod = rsAux.getInt(1);
                String aux = rsAux.getString(2);
                if (aux != null && !mapaBarras.containsKey(procod)) {
                    aux = aux.trim().replaceAll("^0+", "");
                    mapaBarras.put(procod, aux);
                }
            }
        } catch (SQLException e) {
            LOG.warning("[ProdutoStep] Erro lendo PRODUTOAUX: " + e.getMessage());
        } finally { close(rsAux); close(stAux); }

        int ins = 0, err = 0;
        PreparedStatement pst = null;
        Statement stFb = null; ResultSet rs = null;
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT PROCOD, PRODESRDZ, PRODES, PROPRCVDAVAR, PROPRCCST," +
                "  PROPRCVDA2, PROQTDMINPRC2, PROPRCVDA3, PROQTDMINPRC3," +
                "  GENCODIGO, PROPESVAR, PRODATCADINC," +
                "  PRONCM, PROCEST, SECCOD, GRPCOD, PROUNID, TRBID, FORCOD," +
                "  PROESTMIN, PROESTMAX FROM PRODUTO WHERE CHAR_LENGTH(TRIM(PRODES)) >= 1");

            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.produto(" +
                "pode_fracionado,pode_balanca,datahora_cadastro,estoque_minimo,estoque_max," +
                "codigo,nome,descricao,preco_venda,preco_custo," +
                "preco_venda2,qtd_minimapv2,preco_venda3,qtd_minimapv3," +
                "trib_genero,codigo_barras,referencia," +
                "id_ncm,id_cest,id_categoria,id_subcategoria,id_unidade," +
                "estoque,id_cfop,id_cst,trib_icmsaliqsaida,id_fornecedor,id_empresa)" +
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)");

            while (rs.next()) {
                int       procod    = rs.getInt(1);
                String    prodesrdz = rs.getString(2);
                String    prodes    = rs.getString(3);
                double    proprcvda = rs.getDouble(4);
                double    proprccst = rs.getDouble(5);
                double    prcvda2   = rs.getDouble(6);
                double    qtdmin2   = rs.getDouble(7);
                double    prcvda3   = rs.getDouble(8);
                double    qtdmin3   = rs.getDouble(9);
                String    gencodigo = rs.getString(10);
                String    propesvar = rs.getString(11);
                Timestamp prodatcad = rs.getTimestamp(12);
                String    proncm    = rs.getString(13);
                String    procest   = rs.getString(14);
                String    seccod    = rs.getString(15);
                String    grpcod    = rs.getString(16);
                String    prounid   = rs.getString(17);
                String    trbid     = rs.getString(18);
                int       forcod    = rs.getInt(19);
                double    proestmin = rs.getDouble(20);
                double    proestmax = rs.getDouble(21);

                // Resolve FKs via mapas
                Integer idNcm   = mapaNcm.get(proncm  != null ? proncm.trim()  : "");
                Integer idCest  = mapaCest.get(procest != null ? procest.trim() : "");
                Integer idCateg = mapaCateg.get(seccod != null ? seccod.trim() : "");
                String  secT    = seccod != null ? seccod.trim() : "";
                String  grpT    = grpcod != null ? grpcod.trim() : "";
                Integer idSub   = mapaSub.get(secT + "|" + grpT);
                Integer idUnid  = mapaUnidade.get(prounid != null ? prounid.trim() : "");
                Integer idForn  = mapaForn.get(forcod);
                double  estoque = mapaEstoque.getOrDefault(procod, 0.0);

                // Resolve CST/CFOP a partir da tributacao
                Integer idCst    = null;
                int     idCfop   = 289;
                String  aliqIcms = "";
                String[] trib = mapaTrib.get(trbid != null ? trbid.trim() : "");
                if (trib != null) {
                    aliqIcms = trib[2];
                    String trbtabb  = trib[0];
                    String trbcsosn = trib[1];
                    if (!trbtabb.isEmpty()) {
                        idCst = mapaCstA.get(trbtabb);
                        if (idCst != null) idCfop = ("9".equals(trbtabb) || "22".equals(trbtabb)) ? 355 : 289;
                    }
                    if (idCst == null) {
                        String codB = (trbcsosn == null || trbcsosn.isEmpty() || "0".equals(trbcsosn)) ? "102" : trbcsosn;
                        idCst = mapaCstB.get(codB);
                        if (idCst != null) idCfop = ("60".equals(trbcsosn) || "500".equals(trbcsosn)) ? 355 : 289;
                    }
                }

                String codigo       = String.valueOf(procod).replaceAll("^0+", "");
                String barras       = mapaBarras.getOrDefault(procod, "");
                String codigoBarras = barras.length() <= 14 ? barras : "";
                String referencia   = barras.length() >  14 ? barras : "";

                if (proestmax > 1000000.0) proestmax = 0.0;
                if (proprccst > 1000000.0) proprccst = 0.0;
                String nomeExibir = (prodesrdz != null && !prodesrdz.trim().isEmpty())
                                    ? prodesrdz.trim() : prodes.trim();

                try {
                    pst.setString(1, propesvar);  pst.setString(2, propesvar);
                    pst.setTimestamp(3, prodatcad != null ? prodatcad : new Timestamp(System.currentTimeMillis()));
                    pst.setDouble(4, proestmin);  pst.setDouble(5, proestmax);
                    pst.setString(6, codigo);     pst.setString(7, nomeExibir);
                    pst.setString(8, prodes.trim()); pst.setDouble(9, proprcvda);
                    pst.setDouble(10, proprccst); pst.setDouble(11, prcvda2);
                    pst.setDouble(12, qtdmin2);   pst.setDouble(13, prcvda3);
                    pst.setDouble(14, qtdmin3);
                    pst.setString(15, gencodigo != null ? gencodigo.trim() : "");
                    pst.setString(16, codigoBarras); pst.setString(17, referencia);
                    if (idNcm   != null) pst.setInt(18, idNcm);   else pst.setNull(18, Types.INTEGER);
                    if (idCest  != null) pst.setInt(19, idCest);  else pst.setNull(19, Types.INTEGER);
                    if (idCateg != null) pst.setInt(20, idCateg); else pst.setNull(20, Types.INTEGER);
                    if (idSub   != null) pst.setInt(21, idSub);   else pst.setNull(21, Types.INTEGER);
                    if (idUnid  != null) pst.setInt(22, idUnid);  else pst.setNull(22, Types.INTEGER);
                    pst.setDouble(23, estoque);   pst.setInt(24, idCfop);
                    if (idCst   != null) pst.setInt(25, idCst);   else pst.setNull(25, Types.INTEGER);
                    pst.setString(26, aliqIcms);
                    if (idForn  != null) pst.setInt(27, idForn);  else pst.setNull(27, Types.INTEGER);
                    pst.executeUpdate();
                    ins++;
                } catch (SQLException e) {
                    LOG.warning("[ProdutoStep] Erro procod=" + procod + ": " + e.getMessage());
                    err++;
                }
                if ((ins + err) % 200 == 0) { try { my.commit(); } catch (SQLException ex) {} }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo PRODUTO do Firebird: " + e.getMessage(), e);
        } finally { close(rs); close(stFb); close(pst); }

        contarInseridos(ctx, ins);
        contarErros(ctx, err);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.produto", "rollback ProdutoStep");
    }
}
