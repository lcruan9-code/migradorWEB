package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
        exec(ctx.getDestinoConn(),
            "ALTER TABLE lc_sistemas.grupotributacao AUTO_INCREMENT = 1");
        execIgnore(ctx.getDestinoConn(),
            "DROP TEMPORARY TABLE IF EXISTS TRIBUTACAO",
            "drop temp TRIBUTACAO residual");
    }

    // ── execute: lógica exata da NOVOGRUPOTRIB2 ───────────────────────────────
    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {

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
            "  INNER JOIN lc_sistemas.ncm n ON n.id = p.id_ncm" +
            "  SET p.trib_genero = SUBSTRING(n.codigo, 1, 2)" +
            "  WHERE p.trib_genero = ''");

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
            "    p.id_ncm, p.id_cfop, p.id_cst," +
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
            "  INNER JOIN TRIBUTACAO t ON t.id_produto = p.id" +
            "  LEFT JOIN lc_sistemas.grupotributacao g ON g.nome = t.nome" +
            "  SET p.id_grupotributacao = g.id");

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
