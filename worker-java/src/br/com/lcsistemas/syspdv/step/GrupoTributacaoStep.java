package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;
import br.com.lcsistemas.syspdv.sql.SqlMemoryStore;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementa inline a lógica exata da Stored Procedure NOVOGRUPOTRIB2.
 *
 * Etapas (espelho fiel da procedure):
 *   1. Busca id_estado em lc_sistemas.estados WHERE uf = p_uf
 *   2. Se estado não encontrado → lança exceção
 *   3. DELETE grupotributacao WHERE id > 1, reset AUTO_INCREMENT = 1
 *   4. UPDATE produto.trib_genero = primeiros 2 dígitos do NCM (onde vazio)
 *   5. INSERT DISTINCT em grupotributacao (nome composto com NCM/CST/ICMS/PIS/COFINS/IPI)
 *   6. CREATE TEMPORARY TABLE TRIBUTACAO
 *   7. INSERT em TRIBUTACAO (produto × nome do grupo — mesmo padrão de nome)
 *   8. UPDATE produto.id_grupotributacao via LEFT JOIN com TRIBUTACAO + grupotributacao
 *   9. DROP TEMPORARY TABLE TRIBUTACAO
 *
 * Pré-requisito (Skill GRUPOTRIBUTACAO): deve rodar DEPOIS do AjustePosMigracaoStep.
 */
public class GrupoTributacaoStep extends StepBase {

    private static final String UF_FALLBACK = "PA";

    @Override
    public String getNome() { return "GrupoTributacaoStep"; }

    // ── prepare: reseta tabela e dropa resíduos ───────────────────────────────
    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
        exec(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.grupotributacao WHERE id > 1");
        execIgnore(ctx.getDestinoConn(),
            "DROP TEMPORARY TABLE IF EXISTS TRIBUTACAO",
            "drop temp TRIBUTACAO residual");
    }

    // ── execute: lógica exata da NOVOGRUPOTRIB2 ───────────────────────────────
    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        SqlMemoryStore store = ctx.getMemoryStore();
        if (store != null) {
            executeInMemory(ctx, store);
        } else {
            executeSQL(ctx);
        }
    }

    // ── executeSQL: original SQL-based logic ──────────────────────────────────
    private void executeSQL(MigracaoContext ctx) throws MigracaoException {

        // ── Resolve UF e id_estado ────────────────────────────────────────────
        String uf = ctx.getConfig().getClienteUf();
        if (uf == null || uf.trim().isEmpty()) uf = UF_FALLBACK;
        uf = uf.trim().toUpperCase();

        int idEstado = buscarIdEstado(ctx, uf);
        if (idEstado <= 0) {
            throw new MigracaoException(getNome(),
                "UF \"" + uf + "\" não encontrada na tabela lc_sistemas.estados. "
                + "Cadastre o estado antes de migrar.", null);
        }

        // ── 1. Corrige trib_genero vazio ──────────────────────────────────────
        execUpdate(ctx.getDestinoConn(),
            "UPDATE lc_sistemas.produto p" +
            "  SET p.trib_genero = (SELECT SUBSTRING(n.codigo, 1, 2) FROM lc_sistemas.ncm n WHERE n.id = p.id_ncm)" +
            "  WHERE p.trib_genero = ''" +
            "  AND EXISTS (SELECT 1 FROM lc_sistemas.ncm n WHERE n.id = p.id_ncm)");

        // ── 2. INSERT DISTINCT em grupotributacao ─────────────────────────────
        // Nome no mesmo padrão da procedure NOVOGRUPOTRIB2
        String nomeConcatInsert =
            "CONCAT('NCM ', ncm.codigo," +
            "       ' CST ', cst.codigotributario," +
            "       ' ICMS ', p.trib_icmsaliqsaida," +
            "       ' RED BC ICMS ', p.trib_icmsaliqredbasecalcsaida," +
            "       ' FCP ', p.trib_icmsfcpaliq," +
            "       ' PIS ', p.trib_pissaida, ' ', p.trib_pisaliqsaida," +
            "       ' COFINS ', p.trib_cofinssaida, ' ', p.trib_cofinsaliqsaida," +
            "       ' IPI ', p.trib_ipisaida, ' ', p.trib_ipialiqsaida," +
            "       ' GT: ', p.id_grupotributacao)";

        int ins = execUpdate(ctx.getDestinoConn(),
            "INSERT INTO lc_sistemas.grupotributacao" +
            "  (nome, id_estado, uf, id_ncm, id_cest, id_cfop," +
            "   id_cfop_bonificacao, id_cfop_devolucao, id_cfop_transferencia," +
            "   ncm, origem, genero, id_cst," +
            "   icms_saida_aliquota, icms_saida_aliquota_red_base_calc," +
            "   pis_saida, cofins_saida, pis_saida_aliquota, cofins_saida_aliquota," +
            "   ipi_cst, ipi_aliquota, ipi_ex, ipi_codigo_enquadramento," +
            "   icms_fcp_aliquota, icms_observacao_fiscal, icms_difererimento_aliquota," +
            "   icms_pode_desonerado, icms_motivo_desonerado," +
            "   icms_indic_deduz_desonerado, icms_codigo_beneficio_fiscal," +
            "   pis_nri, cofins_nri, preco_cmv," +
            "   imendes_codigo_grupo, imendes_codigo_regra," +
            "   datahora_alteracao, ativo)" +
            "  SELECT DISTINCT" +
            "    " + nomeConcatInsert + "," +
            "    " + idEstado + ", '" + uf + "', p.id_ncm, p.id_cest, p.id_cfop," +
            "    414, 23, 311," +
            "    ncm.codigo, p.origem_produto, p.trib_genero, p.id_cst," +
            "    p.trib_icmsaliqsaida, p.trib_icmsaliqredbasecalcsaida," +
            "    p.trib_pissaida, p.trib_cofinssaida," +
            "    p.trib_pisaliqsaida, p.trib_cofinsaliqsaida," +
            "    p.trib_ipisaida, p.trib_ipialiqsaida, '', 999," +
            "    0, '', 0, 'NAO', '', '', '', '', '', 0, '', ''," +
            "    NOW(), '1'" +
            "  FROM lc_sistemas.produto p" +
            "  INNER JOIN lc_sistemas.ncm   ON ncm.id  = p.id_ncm" +
            "  INNER JOIN lc_sistemas.empresa e  ON e.id   = p.id_empresa" +
            "  INNER JOIN lc_sistemas.estados est ON est.id = e.id_estados" +
            "  INNER JOIN lc_sistemas.cst    ON cst.id  = p.id_cst" +
            "  GROUP BY" +
            "    p.id_ncm, p.id_cfop, p.id_cst, p.id_cest, p.origem_produto, p.trib_genero, ncm.codigo, cst.codigotributario, p.id_grupotributacao," +
            "    p.trib_icmsaliqsaida, p.trib_icmsaliqredbasecalcsaida, p.trib_icmsfcpaliq," +
            "    p.trib_pissaida, p.trib_pisaliqsaida," +
            "    p.trib_cofinssaida, p.trib_cofinsaliqsaida," +
            "    p.trib_ipisaida, p.trib_ipialiqsaida");
        contarInseridos(ctx, ins);

        // ── 3. Cria tabela temporária TRIBUTACAO ──────────────────────────────
        exec(ctx.getDestinoConn(),
            "CREATE TEMPORARY TABLE TRIBUTACAO (" +
            "  ID INTEGER NOT NULL AUTO_INCREMENT," +
            "  nome VARCHAR(200)," +
            "  id_produto INT(11)," +
            "  PRIMARY KEY (ID)" +
            ")");

        // ── 4. INSERT em TRIBUTACAO (mesmo padrão de nome) ────────────────────
        String nomeConcatTrib =
            "CONCAT('NCM ', ncm.codigo," +
            "       ' CST ', c.codigotributario," +
            "       ' ICMS ', p.trib_icmsaliqsaida," +
            "       ' RED BC ICMS ', p.trib_icmsaliqredbasecalcsaida," +
            "       ' FCP ', p.trib_icmsfcpaliq," +
            "       ' PIS ', p.trib_pissaida, ' ', p.trib_pisaliqsaida," +
            "       ' COFINS ', p.trib_cofinssaida, ' ', p.trib_cofinsaliqsaida," +
            "       ' IPI ', p.trib_ipisaida, ' ', p.trib_ipialiqsaida," +
            "       ' GT: ', p.id_grupotributacao)";

        execUpdate(ctx.getDestinoConn(),
            "INSERT INTO TRIBUTACAO (nome, id_produto)" +
            "  SELECT " + nomeConcatTrib + ", p.id" +
            "  FROM lc_sistemas.produto p" +
            "  INNER JOIN lc_sistemas.ncm   ON ncm.id = p.id_ncm" +
            "  INNER JOIN lc_sistemas.cst c ON c.id   = p.id_cst" +
            "  INNER JOIN lc_sistemas.cfop cf ON cf.id = p.id_cfop" +
            "  ORDER BY p.id");

        // ── 5. UPDATE produto.id_grupotributacao via LEFT JOIN ────────────────
        execUpdate(ctx.getDestinoConn(),
            "UPDATE lc_sistemas.produto p" +
            "  SET p.id_grupotributacao = (" +
            "    SELECT MIN(g.id) FROM TRIBUTACAO t" +
            "    LEFT JOIN lc_sistemas.grupotributacao g ON g.nome = t.nome" +
            "    WHERE t.id_produto = p.id" +
            "  )" +
            "  WHERE EXISTS (" +
            "    SELECT 1 FROM TRIBUTACAO t WHERE t.id_produto = p.id" +
            "  )");

        // ── 6. Drop temporária ────────────────────────────────────────────────
        execIgnore(ctx.getDestinoConn(),
            "DROP TEMPORARY TABLE TRIBUTACAO",
            "drop temp TRIBUTACAO apos vinculacao");
    }

    // ── cleanup: segurança caso execute() tenha falhado antes do drop ─────────
    @Override
    public void cleanup(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DROP TEMPORARY TABLE IF EXISTS TRIBUTACAO",
            "drop temp TRIBUTACAO cleanup");
    }

    // ── rollback ──────────────────────────────────────────────────────────────
    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "UPDATE lc_sistemas.produto SET id_grupotributacao = 1" +
            " WHERE id_grupotributacao > 1",
            "rollback: resetar id_grupotributacao para o grupo padrao");
        execIgnore(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.grupotributacao WHERE id > 1",
            "rollback: limpar grupotributacao");
        execIgnore(ctx.getDestinoConn(),
            "DROP TEMPORARY TABLE IF EXISTS TRIBUTACAO",
            "rollback: drop temp TRIBUTACAO");
    }

    // ── executeInMemory: in-memory logic ──────────────────────────────────────
    private void executeInMemory(MigracaoContext ctx, SqlMemoryStore store) throws MigracaoException {
        String uf = ctx.getConfig().getClienteUf();
        if (uf == null || uf.trim().isEmpty()) uf = UF_FALLBACK;
        uf = uf.trim().toUpperCase();

        int idEstado = buscarIdEstado(ctx, uf);
        if (idEstado <= 0) throw new MigracaoException(getNome(), "UF \""+uf+"\" não encontrada", null);

        // Build lookup maps: id_ncm -> ncm.codigo, id_cst -> cst.codigotributario
        Map<Integer,String> ncmCodigos = new LinkedHashMap<Integer,String>();
        for (Map<String,Object> row : store.selectAll("ncm")) {
            Object id = row.get("id"); Object cod = row.get("codigo");
            if (id != null && cod != null) ncmCodigos.put(Integer.parseInt(id.toString()), cod.toString());
        }
        Map<Integer,String> cstCodigos = new LinkedHashMap<Integer,String>();
        for (Map<String,Object> row : store.selectAll("cst")) {
            Object id = row.get("id"); Object cod = row.get("codigotributario");
            if (id != null && cod != null) cstCodigos.put(Integer.parseInt(id.toString()), cod.toString());
        }

        // Fix trib_genero for empty produtos
        for (Map<String,Object> p : store.selectAll("produto")) {
            String tg = p.get("trib_genero") != null ? p.get("trib_genero").toString() : "";
            if (tg.isEmpty()) {
                int idNcm = p.get("id_ncm") != null ? Integer.parseInt(p.get("id_ncm").toString()) : 0;
                String ncmCod = ncmCodigos.get(idNcm);
                if (ncmCod != null && ncmCod.length() >= 2) p.put("trib_genero", ncmCod.substring(0,2));
            }
        }

        // Clear grupotributacao (keep id=1 if exists)
        store.clear("grupotributacao");

        String now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        // Build distinct grupotributacao entries
        Map<String,Integer> nomeToId = new LinkedHashMap<String,Integer>();

        for (Map<String,Object> p : store.selectAll("produto")) {
            int idNcm = p.get("id_ncm") != null ? Integer.parseInt(p.get("id_ncm").toString()) : 1;
            int idCst = p.get("id_cst") != null ? Integer.parseInt(p.get("id_cst").toString()) : 15;
            String ncmCod = ncmCodigos.getOrDefault(idNcm, "");
            String cstCod = cstCodigos.getOrDefault(idCst, "");

            String nome = buildGrupoNome(p, ncmCod, cstCod);
            if (nomeToId.containsKey(nome)) continue; // already added

            int idCfop = p.get("id_cfop") != null ? Integer.parseInt(p.get("id_cfop").toString()) : 289;
            int idCest = p.get("id_cest") != null ? Integer.parseInt(p.get("id_cest").toString()) : 1;
            String origem = p.get("origem_produto") != null ? p.get("origem_produto").toString() : "0";
            String genero = p.get("trib_genero") != null ? p.get("trib_genero").toString() : "";
            double icmsAliq = safeDbl(p.get("trib_icmsaliqsaida"));
            double icmsRedBC = safeDbl(p.get("trib_icmsaliqredbasecalcsaida"));
            double icmsFcp = safeDbl(p.get("trib_icmsfcpaliq"));
            String pisSaida = safeStr(p.get("trib_pissaida"));
            double pisAliq = safeDbl(p.get("trib_pisaliqsaida"));
            String cofinsSaida = safeStr(p.get("trib_cofinssaida"));
            double cofinsAliq = safeDbl(p.get("trib_cofinsaliqsaida"));
            String ipiCst = safeStr(p.get("trib_ipisaida"));
            double ipiAliq = safeDbl(p.get("trib_ipialiqsaida"));

            Map<String,Object> gt = new LinkedHashMap<String,Object>();
            gt.put("nome", nome);
            gt.put("uf", uf);
            gt.put("id_estado", idEstado);
            gt.put("id_ncm", idNcm);
            gt.put("id_cest", idCest);
            gt.put("id_cst", idCst);
            gt.put("id_cfop", idCfop);
            gt.put("id_cfop_bonificacao", 414);
            gt.put("id_cfop_devolucao", 23);
            gt.put("id_cfop_transferencia", 311);
            gt.put("ncm", ncmCod);
            gt.put("origem", origem);
            gt.put("genero", genero);
            gt.put("icms_saida_aliquota", icmsAliq);
            gt.put("icms_saida_aliquota_red_base_calc", icmsRedBC);
            gt.put("icms_fcp_aliquota", icmsFcp);
            gt.put("icms_observacao_fiscal", "");
            gt.put("icms_difererimento_aliquota", 0.0);
            gt.put("icms_pode_desonerado", "NAO");
            gt.put("icms_motivo_desonerado", "");
            gt.put("icms_desonerado_aliquota", 0.0);
            gt.put("icms_indic_deduz_desonerado", "");
            gt.put("icms_efetivo_aliquota", 0.0);
            gt.put("icms_efetivo_aliquota_red_base_calc", 0.0);
            gt.put("icms_st_aliquota", 0.0);
            gt.put("icms_st_red_base_calc_aliquota", 0.0);
            gt.put("icms_isencao_aliquota", 0.0);
            gt.put("icms_iva", 0.0);
            gt.put("icms_codigo_beneficio_fiscal", "");
            gt.put("icms_st_aliq_fcp", 0.0);
            gt.put("pis_saida", pisSaida);
            gt.put("pis_saida_aliquota", pisAliq);
            gt.put("pis_nri", "");
            gt.put("cofins_saida", cofinsSaida);
            gt.put("cofins_saida_aliquota", cofinsAliq);
            gt.put("cofins_nri", "");
            gt.put("ipi_cst", ipiCst);
            gt.put("ipi_ex", "");
            gt.put("ipi_aliquota", ipiAliq);
            gt.put("ipi_codigo_enquadramento", "999");
            gt.put("preco_cmv", 0.0);
            gt.put("imendes_codigo_grupo", "");
            gt.put("imendes_codigo_regra", "");
            gt.put("imendes_datahora_alteracao", null);
            gt.put("datahora_alteracao", now);
            gt.put("ativo", "1");

            int newId = store.insert("grupotributacao", gt);
            nomeToId.put(nome, newId);
        }

        // Update id_grupotributacao on each produto
        for (Map<String,Object> p : store.selectAll("produto")) {
            int idNcm = p.get("id_ncm") != null ? Integer.parseInt(p.get("id_ncm").toString()) : 1;
            int idCst = p.get("id_cst") != null ? Integer.parseInt(p.get("id_cst").toString()) : 15;
            String ncmCod = ncmCodigos.getOrDefault(idNcm, "");
            String cstCod = cstCodigos.getOrDefault(idCst, "");
            String nome = buildGrupoNome(p, ncmCod, cstCod);
            Integer gtId = nomeToId.get(nome);
            if (gtId != null) p.put("id_grupotributacao", gtId);
        }

        contarInseridos(ctx, nomeToId.size());
    }

    private String buildGrupoNome(Map<String,Object> p, String ncmCod, String cstCod) {
        return "NCM " + ncmCod
            + " CST " + cstCod
            + " ICMS " + safeDbl(p.get("trib_icmsaliqsaida"))
            + " RED BC ICMS " + safeDbl(p.get("trib_icmsaliqredbasecalcsaida"))
            + " FCP " + safeDbl(p.get("trib_icmsfcpaliq"))
            + " PIS " + safeStr(p.get("trib_pissaida")) + " " + safeDbl(p.get("trib_pisaliqsaida"))
            + " COFINS " + safeStr(p.get("trib_cofinssaida")) + " " + safeDbl(p.get("trib_cofinsaliqsaida"))
            + " IPI " + safeStr(p.get("trib_ipisaida")) + " " + safeDbl(p.get("trib_ipialiqsaida"))
            + " GT: " + (p.get("id_grupotributacao") != null ? p.get("id_grupotributacao") : 1);
    }

    private static String safeStr(Object v) { return v == null ? "" : v.toString(); }
    private static double safeDbl(Object v) {
        if (v == null) return 0.0;
        try { return Double.parseDouble(v.toString()); } catch (NumberFormatException e) { return 0.0; }
    }

    // ── Helper: busca id do estado pela UF ───────────────────────────────────
    private int buscarIdEstado(MigracaoContext ctx, String uf) throws MigracaoException {
        String sql = "SELECT id FROM lc_sistemas.estados WHERE uf = ? LIMIT 1";
        try (PreparedStatement ps = ctx.getDestinoConn().prepareStatement(sql)) {
            ps.setString(1, uf);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new MigracaoException(getNome(),
                "Erro ao buscar id_estado para UF '" + uf + "': " + e.getMessage(), e);
        }
        return 0;
    }
}
