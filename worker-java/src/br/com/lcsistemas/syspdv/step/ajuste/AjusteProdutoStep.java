package br.com.lcsistemas.syspdv.step.ajuste;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;
import br.com.lcsistemas.syspdv.sql.SqlMemoryStore;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Ajuste pós-inserção da tabela <b>produto</b>.
 *
 * <p>O que faz:
 * <ul>
 *   <li>Remove referências SEM/GTIN e caracteres especiais de códigos</li>
 *   <li>Preenche defaults de todos os campos nulos (FKs, preços, flags S/N, datas)</li>
 *   <li>Calcula margens de lucro com base em preco_venda e preco_custo</li>
 *   <li>Zera preços duplicados (pv2 = pv1, etc.) e suas margens</li>
 *   <li>Aplica UPPER + TRIM + SUBS/SUBS2 em nome e descricao</li>
 *   <li>Normaliza tributação (trib_pissaida, trib_cofinssaida com LPAD)</li>
 * </ul>
 *
 * <p>Tabelas afetadas: {@code lc_sistemas.produto}
 *
 * <p>Pré-requisito: {@code ProdutoStep} deve ter sido executado e inserido os registros.
 *
 * <p>Seleção portal: executa se {@code tudo || sel.contains("PRODUTO")}.
 */
public class AjusteProdutoStep extends AjusteBase {

    @Override
    public String getNome() { return "AjusteProduto"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        boolean tudo = !ctx.getConfig().temSelecao();
        java.util.Set<String> sel = ctx.getConfig().getTabelasSelecionadas();

        if (!tudo && !sel.contains("PRODUTO")) {
            contarInseridos(ctx, 0);
            return;
        }

        SqlMemoryStore store = ctx.getMemoryStore();
        if (store != null) {
            ajustarProdutoMem(store);
        } else {
            Connection c = ctx.getDestinoConn();
            try { c.setAutoCommit(true); }
            catch (Exception e) { LOG.warning("[AjusteProduto] setAutoCommit(true): " + e.getMessage()); }
            try {
                ajustarProduto(c);
            } finally {
                try { c.setAutoCommit(false); }
                catch (Exception e) { LOG.warning("[AjusteProduto] setAutoCommit(false): " + e.getMessage()); }
            }
        }
        contarInseridos(ctx, 0);
    }

    // =========================================================================
    //  IN-MEMORY
    // =========================================================================
    private void ajustarProdutoMem(SqlMemoryStore store) {
        List<Map<String, Object>> rows = store.selectAll("produto");
        for (Map<String, Object> p : rows) {
            // Clear SEM/GTIN
            String ref    = safeStr(p.get("referencia"));
            String cod    = safeStr(p.get("codigo"));
            String codBar = safeStr(p.get("codigo_barras"));

            if (ref.contains("SEM")    || ref.contains("GTIN"))    p.put("referencia",    "");
            if (cod.contains("SEM")    || cod.contains("GTIN"))    p.put("codigo",         "");
            if (codBar.contains("SEM") || codBar.contains("GTIN")) p.put("codigo_barras",  "");

            // Strip *
            ref    = safeStr(p.get("referencia")).replace("*", "").replace(" ", "");
            cod    = safeStr(p.get("codigo")).replace("*", "").replace(" ", "");
            codBar = safeStr(p.get("codigo_barras")).replace("*", "").replace(" ", "");
            p.put("referencia",    ref);
            p.put("codigo",         cod);
            p.put("codigo_barras",  codBar);

            // String nulls to ''
            if (isNull(p.get("nome")))                         p.put("nome", "");
            if (isNull(p.get("descricao")))                    p.put("descricao", "");
            if (isNull(p.get("ex_tipi")))                      p.put("ex_tipi", "");
            if (isNull(p.get("trib_icmsobs")))                 p.put("trib_icmsobs", "");
            if (isNull(p.get("trib_unidadetributavel")))       p.put("trib_unidadetributavel", "");
            if (isNull(p.get("trib_genero")))                  p.put("trib_genero", "");
            if (isNull(p.get("observacoes")))                  p.put("observacoes", "");
            if (isNull(p.get("foto")))                         p.put("foto", "");
            if (isNull(p.get("foto2")))                        p.put("foto2", "");
            if (isNull(p.get("foto3")))                        p.put("foto3", "");
            if (isNull(p.get("local")))                        p.put("local", "");
            if (isNull(p.get("ref_cruzada1")))                 p.put("ref_cruzada1", "");
            if (isNull(p.get("ref_cruzada2")))                 p.put("ref_cruzada2", "");
            if (isNull(p.get("ref_cruzada3")))                 p.put("ref_cruzada3", "");
            if (isNull(p.get("ref_cruzada4")))                 p.put("ref_cruzada4", "");
            if (isNull(p.get("ref_cruzada5")))                 p.put("ref_cruzada5", "");
            if (isNull(p.get("ref_cruzada6")))                 p.put("ref_cruzada6", "");
            if (isNull(p.get("tipo_med")))                     p.put("tipo_med", "");
            if (isNull(p.get("tabela_med")))                   p.put("tabela_med", "");
            if (isNull(p.get("linha_med")))                    p.put("linha_med", "");
            if (isNull(p.get("portaria_med")))                 p.put("portaria_med", "");
            if (isNull(p.get("med_classeterapeutica")))        p.put("med_classeterapeutica", "");
            if (isNull(p.get("med_unidademedida")))            p.put("med_unidademedida", "");
            if (isNull(p.get("med_usoprolongado")))            p.put("med_usoprolongado", "");
            if (isNull(p.get("imendes_codigointerno")))        p.put("imendes_codigointerno", "");
            if (isNull(p.get("imendes_produtonome")))          p.put("imendes_produtonome", "");
            if (isNull(p.get("comb_cprodanp")))                p.put("comb_cprodanp", "");
            if (isNull(p.get("comb_descanp")))                 p.put("comb_descanp", "");

            // Tipo produto
            if (isNull(p.get("tipo_produto"))) p.put("tipo_produto", "PRODUTO");

            // Int FK defaults
            if (isNull(p.get("id_grupotributacao")))    p.put("id_grupotributacao", 1);
            if (isNull(p.get("id_categoria")))          p.put("id_categoria", 1);
            if (isNull(p.get("id_cfop")))               p.put("id_cfop", 289);
            if (isNull(p.get("id_cst")))                p.put("id_cst", 15);
            if (isNull(p.get("id_ncm")))                p.put("id_ncm", 1);
            if (isNull(p.get("id_cest")))               p.put("id_cest", 1);
            if (isNull(p.get("id_fabricante")))         p.put("id_fabricante", 1);
            if (isNull(p.get("id_fornecedor")))         p.put("id_fornecedor", 1);
            if (isNull(p.get("id_unidade")))            p.put("id_unidade", 1);
            if (isNull(p.get("id_subcategoria")))       p.put("id_subcategoria", 1);
            if (isNull(p.get("id_nutricional")))        p.put("id_nutricional", 0);
            if (isNull(p.get("id_unidadeembalagem")))   p.put("id_unidadeembalagem", 0);

            // Unidade atacado
            double qtd2 = safeDbl(p.get("qtd_minimapv2"));
            double pv2  = safeDbl(p.get("preco_venda2"));
            if (qtd2 > 0 && pv2 > 0 && isNull(p.get("id_unidadeatacado2")))
                p.put("id_unidadeatacado2", p.get("id_unidade"));

            double qtd3 = safeDbl(p.get("qtd_minimapv3"));
            double pv3  = safeDbl(p.get("preco_venda3"));
            if (qtd3 > 0 && pv3 > 0 && isNull(p.get("id_unidadeatacado3")))
                p.put("id_unidadeatacado3", p.get("id_unidade"));

            double qtd4 = safeDbl(p.get("qtd_minimapv4"));
            double pv4  = safeDbl(p.get("preco_venda4"));
            if (qtd4 > 0 && pv4 > 0 && isNull(p.get("id_unidadeatacado4")))
                p.put("id_unidadeatacado4", p.get("id_unidade"));

            if (isNull(p.get("id_unidadeatacado2"))) p.put("id_unidadeatacado2", 0);
            if (isNull(p.get("id_unidadeatacado3"))) p.put("id_unidadeatacado3", 0);
            if (isNull(p.get("id_unidadeatacado4"))) p.put("id_unidadeatacado4", 0);

            // Double nulls
            if (isNull(p.get("preco_pmc")))           p.put("preco_pmc", 0.0);
            if (isNull(p.get("preco_custo")))         p.put("preco_custo", 0.0);
            if (isNull(p.get("preco_venda")))         p.put("preco_venda", 0.0);
            if (isNull(p.get("preco_compra")))        p.put("preco_compra", 0.0);
            if (isNull(p.get("valor_compra")))        p.put("valor_compra", 0.0);
            if (isNull(p.get("custo_medio")))         p.put("custo_medio", 0.0);
            if (isNull(p.get("margem_lucro")))        p.put("margem_lucro", 0.0);
            if (isNull(p.get("desconto_max")))        p.put("desconto_max", 0.0);
            if (isNull(p.get("preco_venda2")))        p.put("preco_venda2", 0.0);
            if (isNull(p.get("margem_lucro2")))       p.put("margem_lucro2", 0.0);
            if (isNull(p.get("qtd_minimapv2")))       p.put("qtd_minimapv2", 0.0);
            if (isNull(p.get("desconto_max2")))       p.put("desconto_max2", 0.0);
            if (isNull(p.get("preco_venda3")))        p.put("preco_venda3", 0.0);
            if (isNull(p.get("margem_lucro3")))       p.put("margem_lucro3", 0.0);
            if (isNull(p.get("qtd_minimapv3")))       p.put("qtd_minimapv3", 0.0);
            if (isNull(p.get("desconto_max3")))       p.put("desconto_max3", 0.0);
            if (isNull(p.get("preco_venda4")))        p.put("preco_venda4", 0.0);
            if (isNull(p.get("margem_lucro4")))       p.put("margem_lucro4", 0.0);
            if (isNull(p.get("qtd_minimapv4")))       p.put("qtd_minimapv4", 0.0);
            if (isNull(p.get("desconto_max4")))       p.put("desconto_max4", 0.0);
            if (isNull(p.get("preco_antigo")))        p.put("preco_antigo", 0.0);
            if (isNull(p.get("valor_frete")))         p.put("valor_frete", 0.0);
            if (isNull(p.get("margem_ideal")))        p.put("margem_ideal", 0.0);
            if (isNull(p.get("ipi")))                 p.put("ipi", 0.0);
            if (isNull(p.get("preco_promocao")))      p.put("preco_promocao", 0.0);
            if (isNull(p.get("comissao")))            p.put("comissao", 0.0);
            if (isNull(p.get("comissao_valor")))      p.put("comissao_valor", 0.0);
            if (isNull(p.get("fidelidade_pontos")))   p.put("fidelidade_pontos", 0.0);
            if (isNull(p.get("estoque")))             p.put("estoque", 0.0);
            if (isNull(p.get("estoque_minimo")))      p.put("estoque_minimo", 0.0);
            if (isNull(p.get("estoque_max")))         p.put("estoque_max", 0.0);
            if (isNull(p.get("estoque_tara")))        p.put("estoque_tara", 0.0);
            if (isNull(p.get("peso_bruto")))          p.put("peso_bruto", 0.0);
            if (isNull(p.get("peso_liquido")))        p.put("peso_liquido", 0.0);
            if (isNull(p.get("trib_fatorunidade")))   p.put("trib_fatorunidade", 0.0);
            if (isNull(p.get("trib_icmsaliqsaida")))  p.put("trib_icmsaliqsaida", 0.0);
            if (isNull(p.get("trib_icmsaliqredbasecalcsaida"))) p.put("trib_icmsaliqredbasecalcsaida", 0.0);
            if (isNull(p.get("trib_icmsfcpaliq")))    p.put("trib_icmsfcpaliq", 0.0);
            if (isNull(p.get("trib_issaliqsaida")))   p.put("trib_issaliqsaida", 0.0);
            if (isNull(p.get("trib_ipialiqsaida")))   p.put("trib_ipialiqsaida", 0.0);
            if (isNull(p.get("trib_pisaliqsaida")))   p.put("trib_pisaliqsaida", 0.0);
            if (isNull(p.get("trib_cofinsaliqsaida"))) p.put("trib_cofinsaliqsaida", 0.0);
            if (isNull(p.get("comb_percentualgaspetroleo")))          p.put("comb_percentualgaspetroleo", 0.0);
            if (isNull(p.get("comb_percentualgasnaturalnacional")))   p.put("comb_percentualgasnaturalnacional", 0.0);
            if (isNull(p.get("comb_percentualgasnaturalimportado")))  p.put("comb_percentualgasnaturalimportado", 0.0);
            if (isNull(p.get("comb_valorpartida")))                   p.put("comb_valorpartida", 0.0);
            if (isNull(p.get("comb_percentualbiodiesel")))            p.put("comb_percentualbiodiesel", 0.0);
            if (isNull(p.get("med_precovendafpop")))                  p.put("med_precovendafpop", 0.0);
            if (isNull(p.get("med_margemfpop")))                      p.put("med_margemfpop", 0.0);
            if (isNull(p.get("med_precoVendaFpopBolsaFamilia")))      p.put("med_precoVendaFpopBolsaFamilia", 0.0);
            if (isNull(p.get("med_margemFpopBolsaFamilia")))          p.put("med_margemFpopBolsaFamilia", 0.0);
            if (isNull(p.get("med_apresentacaofpop")))                p.put("med_apresentacaofpop", 0.0);

            double qtdEmb = safeDbl(p.get("qtd_embalagem"));
            if (qtdEmb == 0) p.put("qtd_embalagem", 1.0);

            String qtdDias = safeStr(p.get("qtd_diasvalidade"));
            if (qtdDias.isEmpty()) p.put("qtd_diasvalidade", "0");

            String origem = safeStr(p.get("origem_produto"));
            if (origem.isEmpty()) p.put("origem_produto", "0");

            // Flags
            if (isNull(p.get("ativo")))                    p.put("ativo", 1);
            if (safeStr(p.get("nome")).isEmpty())           p.put("ativo", 0);
            if (isNull(p.get("pode_desconto")))             p.put("pode_desconto", "S");
            if (isNull(p.get("pode_balanca")))              p.put("pode_balanca", "N");
            if (isNull(p.get("pode_fracionado")))           p.put("pode_fracionado", "N");
            if (isNull(p.get("pode_lote")))                 p.put("pode_lote", "N");
            if (isNull(p.get("pode_comissao")))             p.put("pode_comissao", "S");
            if (isNull(p.get("pode_lerpeso")))              p.put("pode_lerpeso", "N");
            if (isNull(p.get("pode_atualizarncm")))         p.put("pode_atualizarncm", "S");
            if (isNull(p.get("pode_producao_propria")))     p.put("pode_producao_propria", "n");
            if (isNull(p.get("med_podeatualizar")))         p.put("med_podeatualizar", "S");

            // Fracionado
            double estoque = safeDbl(p.get("estoque"));
            if (temFracionado(estoque)) {
                if ("N".equals(p.get("pode_balanca")))   p.put("pode_balanca",   "S");
                if ("N".equals(p.get("pode_fracionado"))) p.put("pode_fracionado", "S");
            }

            // Tributação defaults
            String tribIpi    = safeStr(p.get("trib_ipisaida"));
            String tribPis    = safeStr(p.get("trib_pissaida"));
            String tribCofins = safeStr(p.get("trib_cofinssaida"));
            if (tribIpi.isEmpty())    p.put("trib_ipisaida",    "53");
            if (tribPis.isEmpty())    p.put("trib_pissaida",    "07");
            if (tribCofins.isEmpty()) p.put("trib_cofinssaida", "07");

            // Lpad
            tribPis    = lpad2(p.get("trib_pissaida"));
            tribCofins = lpad2(p.get("trib_cofinssaida"));
            p.put("trib_pissaida",    tribPis);
            p.put("trib_cofinssaida", tribCofins);

            String rms = safeStr(p.get("rms_med"));
            if (rms.isEmpty() || "...-".equals(rms)) p.put("rms_med", " .    .    .   - ");

            // Datas
            if (isNull(p.get("datahora_cadastro")))  p.put("datahora_cadastro",  nowTs());
            if (isNull(p.get("datahora_alteracao"))) p.put("datahora_alteracao", nowTs());

            String dataPromoIni = safeStr(p.get("data_promocaoinicial"));
            String dataPromoFim = safeStr(p.get("data_promocaofinal"));
            double precoPromo   = safeDbl(p.get("preco_promocao"));
            if (dataPromoIni.isEmpty() || "0000-00-00".equals(dataPromoIni) || precoPromo == 0)
                p.put("data_promocaoinicial", null);
            if (dataPromoFim.isEmpty() || "0000-00-00".equals(dataPromoFim) || precoPromo == 0)
                p.put("data_promocaofinal", null);

            // Price fixes
            double precoCompra = safeDbl(p.get("preco_compra"));
            double precoCusto  = safeDbl(p.get("preco_custo"));
            double valorCompra = safeDbl(p.get("valor_compra"));

            if (precoCompra == 0) p.put("preco_compra", precoCusto);
            if (precoCusto  == 0) p.put("preco_custo",  precoCompra);
            if (valorCompra == 0) p.put("valor_compra", precoCompra);

            // Margins
            double precoVenda = safeDbl(p.get("preco_venda"));
            precoCusto = safeDbl(p.get("preco_custo"));
            if (precoVenda > 0 && precoCusto > 0)
                p.put("margem_lucro", round2((precoVenda - precoCusto) / precoCusto * 100.0));

            double mpv2 = safeDbl(p.get("preco_venda2"));
            if (mpv2 > 0 && precoCusto > 0)
                p.put("margem_lucro2", round2((mpv2 - precoCusto) / precoCusto * 100.0));

            double mpv3 = safeDbl(p.get("preco_venda3"));
            if (mpv3 > 0 && precoCusto > 0)
                p.put("margem_lucro3", round2((mpv3 - precoCusto) / precoCusto * 100.0));

            double mpv4 = safeDbl(p.get("preco_venda4"));
            if (mpv4 > 0 && precoCusto > 0)
                p.put("margem_lucro4", round2((mpv4 - precoCusto) / precoCusto * 100.0));

            // Margem negativa
            if (safeDbl(p.get("margem_lucro"))  < 0) p.put("margem_lucro",  0.0);
            if (safeDbl(p.get("margem_lucro2")) < 0) p.put("margem_lucro2", 0.0);
            if (safeDbl(p.get("margem_lucro3")) < 0) p.put("margem_lucro3", 0.0);
            if (safeDbl(p.get("margem_lucro4")) < 0) p.put("margem_lucro4", 0.0);

            // Preços duplicados
            double dpv2 = safeDbl(p.get("preco_venda2"));
            double dpv3 = safeDbl(p.get("preco_venda3"));
            double dpv4 = safeDbl(p.get("preco_venda4"));
            double dpv1 = safeDbl(p.get("preco_venda"));

            if (dpv2 == dpv1) {
                p.put("preco_venda2",  0.0);
                p.put("margem_lucro2", 0.0);
            }
            if (dpv3 == dpv1 || dpv3 == dpv2) {
                p.put("preco_venda3",  0.0);
                p.put("margem_lucro3", 0.0);
            }
            if (dpv4 == dpv1 || dpv4 == dpv3 || dpv4 == dpv2) {
                p.put("preco_venda4",  0.0);
                p.put("margem_lucro4", 0.0);
            }

            // Reset unidade atacado se preco = 0
            if (safeDbl(p.get("preco_venda2")) == 0) {
                p.put("id_unidadeatacado2", 0);
                p.put("qtd_minimapv2",      0.0);
            }
            if (safeDbl(p.get("preco_venda3")) == 0) p.put("id_unidadeatacado3", 0);
            if (safeDbl(p.get("preco_venda4")) == 0) p.put("id_unidadeatacado4", 0);

            // Estoque < 0
            estoque = safeDbl(p.get("estoque"));
            if (estoque < 0) p.put("estoque", 0.0);

            // Estoque servico
            if ("SERVICO".equals(safeStr(p.get("tipo_produto")))) p.put("estoque", 0.0);

            // TRIM + UPPER + SUBS
            String nome = safeStr(p.get("nome")).trim().toUpperCase();
            String desc = safeStr(p.get("descricao")).trim().toUpperCase();
            nome = applyAllSubs(nome);
            desc = applyAllSubs(desc);
            p.put("nome",      nome);
            p.put("descricao", desc);
        }
    }

    // =========================================================================
    //  SQL MODE — consolidated UPDATEs (reduced from ~85 to ~18 table scans)
    // =========================================================================
    private void ajustarProduto(Connection c) {
        String t = "lc_sistemas.produto";

        // 1. Code cleanup: SEM/GTIN → '', strip *, trim spaces (1 scan)
        execIgnore(c, "UPDATE "+t+" SET"
            + " referencia    = CASE WHEN referencia    LIKE '%SEM%' OR referencia    LIKE '%GTIN%' THEN ''"
            + "                      ELSE TRIM(REPLACE(referencia,'*','')) END,"
            + " codigo        = CASE WHEN codigo        LIKE '%SEM%' OR codigo        LIKE '%GTIN%' THEN ''"
            + "                      ELSE TRIM(REPLACE(codigo,'*','')) END,"
            + " codigo_barras = CASE WHEN codigo_barras LIKE '%SEM%' OR codigo_barras LIKE '%GTIN%' THEN ''"
            + "                      ELSE TRIM(REPLACE(REPLACE(codigo_barras,'*',''),' ','')) END", t);

        // 2. String nulls → '' (1 scan)
        execIgnore(c, "UPDATE "+t+" SET"
            + " referencia              = COALESCE(referencia,''),"
            + " codigo                  = COALESCE(codigo,''),"
            + " codigo_barras           = COALESCE(codigo_barras,''),"
            + " nome                    = COALESCE(nome,''),"
            + " descricao               = COALESCE(descricao,''),"
            + " ex_tipi                 = COALESCE(ex_tipi,''),"
            + " trib_icmsobs            = COALESCE(trib_icmsobs,''),"
            + " trib_unidadetributavel  = COALESCE(trib_unidadetributavel,''),"
            + " trib_genero             = COALESCE(trib_genero,''),"
            + " observacoes             = COALESCE(observacoes,''),"
            + " foto                    = COALESCE(foto,''),"
            + " foto2                   = COALESCE(foto2,''),"
            + " foto3                   = COALESCE(foto3,''),"
            + " local                   = COALESCE(local,''),"
            + " ref_cruzada1            = COALESCE(ref_cruzada1,''),"
            + " ref_cruzada2            = COALESCE(ref_cruzada2,''),"
            + " ref_cruzada3            = COALESCE(ref_cruzada3,''),"
            + " ref_cruzada4            = COALESCE(ref_cruzada4,''),"
            + " ref_cruzada5            = COALESCE(ref_cruzada5,''),"
            + " ref_cruzada6            = COALESCE(ref_cruzada6,''),"
            + " tipo_produto            = COALESCE(tipo_produto,'PRODUTO'),"
            + " comb_cprodanp           = COALESCE(comb_cprodanp,''),"
            + " comb_descanp            = COALESCE(comb_descanp,'')", t);

        // 3. FK integer defaults (1 scan)
        execIgnore(c, "UPDATE "+t+" SET"
            + " id_grupotributacao  = COALESCE(id_grupotributacao,1),"
            + " id_categoria        = COALESCE(id_categoria,1),"
            + " id_cfop             = COALESCE(id_cfop,289),"
            + " id_cst              = COALESCE(id_cst,15),"
            + " id_ncm              = COALESCE(id_ncm,1),"
            + " id_cest             = COALESCE(id_cest,1),"
            + " id_fabricante       = COALESCE(id_fabricante,1),"
            + " id_fornecedor       = COALESCE(id_fornecedor,1),"
            + " id_unidade          = COALESCE(id_unidade,1),"
            + " id_subcategoria     = COALESCE(id_subcategoria,1),"
            + " id_nutricional      = COALESCE(id_nutricional,0),"
            + " id_unidadeembalagem = COALESCE(id_unidadeembalagem,0),"
            + " origem_produto      = CASE WHEN origem_produto IS NULL OR origem_produto='' THEN '0' ELSE origem_produto END", t);

        // 4. Unidade atacado conditionals (3 targeted scans, order-dependent)
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado2=id_unidade WHERE qtd_minimapv2>0 AND preco_venda2>0 AND id_unidadeatacado2 IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado3=id_unidade WHERE qtd_minimapv3>0 AND preco_venda3>0 AND id_unidadeatacado3 IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado4=id_unidade WHERE qtd_minimapv4>0 AND preco_venda4>0 AND id_unidadeatacado4 IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET"
            + " id_unidadeatacado2 = COALESCE(id_unidadeatacado2,0),"
            + " id_unidadeatacado3 = COALESCE(id_unidadeatacado3,0),"
            + " id_unidadeatacado4 = COALESCE(id_unidadeatacado4,0)", t);

        // 5. All numeric/double nulls → 0, estoque/qtd fixes (1 scan)
        execIgnore(c, "UPDATE "+t+" SET"
            + " trib_fatorunidade   = COALESCE(trib_fatorunidade,0),"
            + " trib_icmsaliqsaida  = COALESCE(trib_icmsaliqsaida,0),"
            + " trib_icmsaliqredbasecalcsaida = COALESCE(trib_icmsaliqredbasecalcsaida,0),"
            + " trib_icmsfcpaliq    = COALESCE(trib_icmsfcpaliq,0),"
            + " trib_issaliqsaida   = COALESCE(trib_issaliqsaida,0),"
            + " trib_ipialiqsaida   = COALESCE(trib_ipialiqsaida,0),"
            + " trib_pisaliqsaida   = COALESCE(trib_pisaliqsaida,0),"
            + " trib_cofinsaliqsaida= COALESCE(trib_cofinsaliqsaida,0),"
            + " preco_pmc           = COALESCE(preco_pmc,0),"
            + " preco_custo         = COALESCE(preco_custo,0),"
            + " preco_venda         = COALESCE(preco_venda,0),"
            + " preco_compra        = COALESCE(preco_compra,0),"
            + " valor_compra        = COALESCE(valor_compra,0),"
            + " custo_medio         = COALESCE(custo_medio,0),"
            + " margem_lucro        = COALESCE(margem_lucro,0),"
            + " desconto_max        = COALESCE(desconto_max,0),"
            + " preco_venda2        = COALESCE(preco_venda2,0),"
            + " margem_lucro2       = COALESCE(margem_lucro2,0),"
            + " qtd_minimapv2       = COALESCE(qtd_minimapv2,0),"
            + " desconto_max2       = COALESCE(desconto_max2,0),"
            + " preco_venda3        = COALESCE(preco_venda3,0),"
            + " margem_lucro3       = COALESCE(margem_lucro3,0),"
            + " qtd_minimapv3       = COALESCE(qtd_minimapv3,0),"
            + " desconto_max3       = COALESCE(desconto_max3,0),"
            + " preco_venda4        = COALESCE(preco_venda4,0),"
            + " margem_lucro4       = COALESCE(margem_lucro4,0),"
            + " qtd_minimapv4       = COALESCE(qtd_minimapv4,0),"
            + " desconto_max4       = COALESCE(desconto_max4,0),"
            + " preco_antigo        = COALESCE(preco_antigo,0),"
            + " valor_frete         = COALESCE(valor_frete,0),"
            + " margem_ideal        = COALESCE(margem_ideal,0),"
            + " ipi                 = COALESCE(ipi,0),"
            + " preco_promocao      = COALESCE(preco_promocao,0),"
            + " comissao            = COALESCE(comissao,0),"
            + " comissao_valor      = COALESCE(comissao_valor,0),"
            + " fidelidade_pontos   = COALESCE(fidelidade_pontos,0),"
            + " estoque             = CASE WHEN estoque IS NULL OR estoque<0 OR tipo_produto='SERVICO' THEN 0 ELSE estoque END,"
            + " estoque_minimo      = COALESCE(estoque_minimo,0),"
            + " estoque_max         = COALESCE(estoque_max,0),"
            + " estoque_tara        = COALESCE(estoque_tara,0),"
            + " qtd_embalagem       = CASE WHEN qtd_embalagem IS NULL OR qtd_embalagem=0 THEN 1 ELSE qtd_embalagem END,"
            + " qtd_diasvalidade    = COALESCE(qtd_diasvalidade,0),"
            + " peso_bruto          = COALESCE(peso_bruto,0),"
            + " peso_liquido        = COALESCE(peso_liquido,0)", t);

        // 6. Comb fields (optional — execIgnore handles missing columns)
        execIgnore(c, "UPDATE "+t+" SET"
            + " comb_percentualgaspetroleo          = COALESCE(comb_percentualgaspetroleo,0),"
            + " comb_percentualgasnaturalnacional   = COALESCE(comb_percentualgasnaturalnacional,0),"
            + " comb_percentualgasnaturalimportado  = COALESCE(comb_percentualgasnaturalimportado,0),"
            + " comb_valorpartida                   = COALESCE(comb_valorpartida,0),"
            + " comb_percentualbiodiesel            = COALESCE(comb_percentualbiodiesel,0)", t);

        // 7. Price cross-fill (order-dependent: 3 sequential scans)
        execIgnore(c, "UPDATE "+t+" SET preco_compra=preco_custo  WHERE preco_compra=0", t);
        execIgnore(c, "UPDATE "+t+" SET preco_custo =preco_compra WHERE preco_custo =0", t);
        execIgnore(c, "UPDATE "+t+" SET valor_compra=preco_compra WHERE valor_compra=0", t);

        // 8. Flags S/N + ativo (1 scan)
        execIgnore(c, "UPDATE "+t+" SET"
            + " ativo                 = COALESCE(ativo,1),"
            + " pode_desconto         = COALESCE(pode_desconto,'S'),"
            + " pode_balanca          = COALESCE(pode_balanca,'N'),"
            + " pode_fracionado       = COALESCE(pode_fracionado,'N'),"
            + " pode_lote             = COALESCE(pode_lote,'N'),"
            + " pode_comissao         = COALESCE(pode_comissao,'S'),"
            + " pode_lerpeso          = COALESCE(pode_lerpeso,'N'),"
            + " pode_atualizarncm     = COALESCE(pode_atualizarncm,'S'),"
            + " pode_producao_propria = COALESCE(pode_producao_propria,'n')", t);
        execIgnore(c, "UPDATE "+t+" SET ativo=0 WHERE nome=''", t);
        execIgnore(c, "UPDATE "+t+" SET"
            + " pode_balanca    = CASE WHEN pode_balanca   ='N' AND SUBSTRING_INDEX(estoque,'.',-1)>0 THEN 'S' ELSE pode_balanca    END,"
            + " pode_fracionado = CASE WHEN pode_fracionado='N' AND SUBSTRING_INDEX(estoque,'.',-1)>0 THEN 'S' ELSE pode_fracionado END", t);

        // 9. Tributação defaults + LPAD (1 scan)
        execIgnore(c, "UPDATE "+t+" SET"
            + " trib_ipisaida    = CASE WHEN trib_ipisaida    IS NULL OR trib_ipisaida   ='' THEN '53' ELSE trib_ipisaida    END,"
            + " trib_pissaida    = LPAD(CASE WHEN trib_pissaida    IS NULL OR trib_pissaida   ='' THEN '7' ELSE trib_pissaida    END,2,'0'),"
            + " trib_cofinssaida = LPAD(CASE WHEN trib_cofinssaida IS NULL OR trib_cofinssaida='' THEN '7' ELSE trib_cofinssaida END,2,'0')", t);

        // 10. Dates + rms_med (1 scan)
        execIgnore(c, "UPDATE "+t+" SET"
            + " datahora_cadastro    = COALESCE(datahora_cadastro, NOW()),"
            + " datahora_alteracao   = COALESCE(datahora_alteracao, NOW()),"
            + " rms_med              = CASE WHEN rms_med IS NULL OR rms_med='...-' THEN ' .    .    .   - ' ELSE rms_med END,"
            + " data_promocaoinicial = CASE WHEN data_promocaoinicial='' OR data_promocaoinicial='0000-00-00' OR preco_promocao=0 THEN NULL ELSE data_promocaoinicial END,"
            + " data_promocaofinal   = CASE WHEN data_promocaofinal  ='' OR data_promocaofinal  ='0000-00-00' OR preco_promocao=0 THEN NULL ELSE data_promocaofinal   END", t);

        // 11. Normalize decimal precision (1 scan)
        execIgnore(c, "UPDATE "+t+" SET"
            + " preco_venda  = CONVERT(preco_venda,  DECIMAL(12,3)),"
            + " preco_venda2 = CONVERT(preco_venda2, DECIMAL(12,3)),"
            + " preco_venda3 = CONVERT(preco_venda3, DECIMAL(12,3)),"
            + " preco_venda4 = CONVERT(preco_venda4, DECIMAL(12,3)),"
            + " preco_custo  = CONVERT(preco_custo,  DECIMAL(12,3)),"
            + " preco_compra = CONVERT(preco_compra, DECIMAL(12,3))", t);

        // 12. Calculate margins (1 scan)
        execIgnore(c, "UPDATE "+t+" SET"
            + " margem_lucro  = CASE WHEN preco_venda >0 AND preco_custo>0 THEN CONVERT((preco_venda -preco_custo)/preco_custo*100,DECIMAL(12,2)) ELSE 0 END,"
            + " margem_lucro2 = CASE WHEN preco_venda2>0 AND preco_custo>0 THEN CONVERT((preco_venda2-preco_custo)/preco_custo*100,DECIMAL(12,2)) ELSE 0 END,"
            + " margem_lucro3 = CASE WHEN preco_venda3>0 AND preco_custo>0 THEN CONVERT((preco_venda3-preco_custo)/preco_custo*100,DECIMAL(12,2)) ELSE 0 END,"
            + " margem_lucro4 = CASE WHEN preco_venda4>0 AND preco_custo>0 THEN CONVERT((preco_venda4-preco_custo)/preco_custo*100,DECIMAL(12,2)) ELSE 0 END", t);

        // 13. Duplicate prices → 0 (3 sequential scans — each reads prior result)
        execIgnore(c, "UPDATE "+t+" SET preco_venda2=0, margem_lucro2=0 WHERE preco_venda2=preco_venda", t);
        execIgnore(c, "UPDATE "+t+" SET preco_venda3=0, margem_lucro3=0 WHERE preco_venda3=preco_venda OR preco_venda3=preco_venda2", t);
        execIgnore(c, "UPDATE "+t+" SET preco_venda4=0, margem_lucro4=0 WHERE preco_venda4=preco_venda OR preco_venda4=preco_venda3 OR preco_venda4=preco_venda2", t);
        execIgnore(c, "UPDATE "+t+" SET"
            + " qtd_minimapv2      = CASE WHEN preco_venda2=0 THEN 0 ELSE qtd_minimapv2    END,"
            + " id_unidadeatacado2 = CASE WHEN preco_venda2=0 THEN 0 ELSE id_unidadeatacado2 END,"
            + " id_unidadeatacado3 = CASE WHEN preco_venda3=0 THEN 0 ELSE id_unidadeatacado3 END,"
            + " id_unidadeatacado4 = CASE WHEN preco_venda4=0 THEN 0 ELSE id_unidadeatacado4 END", t);

        // 14. EAN / medicina / imendes optional fields (execIgnore handles missing columns)
        execIgnore(c, "UPDATE "+t+" SET"
            + " cod_ean           = NULLIF(cod_ean,''),"
            + " codigo_med        = CASE WHEN cod_ean='' THEN NULL ELSE codigo_med END,"
            + " ref_anvisa_med    = NULLIF(ref_anvisa_med,''),"
            + " data_vigencia_med = NULLIF(data_vigencia_med,''),"
            + " edicao_pharmacos  = NULLIF(edicao_pharmacos,''),"
            + " imendes_datahoraalteracacao = NULLIF(imendes_datahoraalteracacao,'')", t);
        execIgnore(c, "UPDATE "+t+" SET"
            + " tipo_med              = COALESCE(tipo_med,''),"
            + " tabela_med            = COALESCE(tabela_med,''),"
            + " linha_med             = COALESCE(linha_med,''),"
            + " portaria_med          = COALESCE(portaria_med,''),"
            + " med_classeterapeutica = COALESCE(med_classeterapeutica,''),"
            + " med_unidademedida     = COALESCE(med_unidademedida,''),"
            + " med_usoprolongado     = COALESCE(med_usoprolongado,''),"
            + " med_podeatualizar     = COALESCE(med_podeatualizar,'S'),"
            + " med_precovendafpop             = COALESCE(med_precovendafpop,0),"
            + " med_margemfpop                 = COALESCE(med_margemfpop,0),"
            + " med_precoVendaFpopBolsaFamilia = COALESCE(med_precoVendaFpopBolsaFamilia,0),"
            + " med_margemFpopBolsaFamilia     = COALESCE(med_margemFpopBolsaFamilia,0),"
            + " med_apresentacaofpop           = COALESCE(med_apresentacaofpop,0),"
            + " imendes_codigointerno          = COALESCE(imendes_codigointerno,''),"
            + " imendes_produtonome            = COALESCE(imendes_produtonome,'')", t);

        // 15. TRIM + UPPER + all text substitutions via chained REPLACE (2 scans: nome, descricao)
        for (String campo : new String[]{"nome", "descricao"}) {
            String expr = buildReplaceChain("TRIM(UPPER(" + campo + "))");
            execIgnore(c, "UPDATE "+t+" SET "+campo+"=TRIM("+expr+")", t);
        }
        execIgnore(c, "UPDATE "+t+" SET codigo=TRIM(codigo), referencia=TRIM(referencia), codigo_barras=TRIM(codigo_barras)", t);
    }

    /** Builds nested REPLACE(REPLACE(..., from, to), ...) chain for all SUBS and SUBS2. */
    private static String buildReplaceChain(String expr) {
        for (String[] s : SUBS)  expr = "REPLACE(" + expr + ",'" + s[0].replace("'","''") + "','" + s[1].replace("'","''") + "')";
        for (String[] s : SUBS2) expr = "REPLACE(" + expr + ",'" + s[0].replace("'","''") + "','" + s[1].replace("'","''") + "')";
        return expr;
    }
}
