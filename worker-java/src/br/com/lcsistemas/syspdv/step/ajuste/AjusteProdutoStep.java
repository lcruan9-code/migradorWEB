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
    //  SQL MODE
    // =========================================================================
    private void ajustarProduto(Connection c) {
        String t = "lc_sistemas.produto";

        execIgnore(c, "UPDATE "+t+" SET referencia   = '' WHERE referencia    LIKE '%SEM%'", t);
        execIgnore(c, "UPDATE "+t+" SET referencia   = '' WHERE referencia    LIKE '%GTIN%'", t);
        execIgnore(c, "UPDATE "+t+" SET codigo       = '' WHERE codigo        LIKE '%SEM%'", t);
        execIgnore(c, "UPDATE "+t+" SET codigo       = '' WHERE codigo        LIKE '%GTIN%'", t);
        execIgnore(c, "UPDATE "+t+" SET codigo_barras= '' WHERE codigo_barras LIKE '%SEM%'", t);
        execIgnore(c, "UPDATE "+t+" SET codigo_barras= '' WHERE codigo_barras LIKE '%GTIN%'", t);
        execIgnore(c, "UPDATE "+t+" SET codigo       = REPLACE(codigo,'*','')", t);
        execIgnore(c, "UPDATE "+t+" SET referencia   = REPLACE(referencia,'*','')", t);
        execIgnore(c, "UPDATE "+t+" SET codigo_barras= REPLACE(codigo_barras,'*','')", t);
        execIgnore(c, "UPDATE "+t+" SET codigo_barras= REPLACE(codigo_barras,' ','')", t);

        // Nulos → ''
        execIgnore(c, "UPDATE "+t+" SET referencia    = '' WHERE referencia    IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET codigo        = '' WHERE codigo        IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET codigo_barras = '' WHERE codigo_barras IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET nome          = '' WHERE nome          IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET descricao     = '' WHERE descricao     IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET ex_tipi       = '' WHERE ex_tipi       IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET trib_icmsobs  = '' WHERE trib_icmsobs  IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET trib_unidadetributavel = '' WHERE trib_unidadetributavel IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET trib_genero   = '' WHERE trib_genero   IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET observacoes   = '' WHERE observacoes   IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET foto          = '' WHERE foto          IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET foto2         = '' WHERE foto2         IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET foto3         = '' WHERE foto3         IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET local         = '' WHERE local         IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET ref_cruzada1  = '' WHERE ref_cruzada1  IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET ref_cruzada2  = '' WHERE ref_cruzada2  IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET ref_cruzada3  = '' WHERE ref_cruzada3  IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET ref_cruzada4  = '' WHERE ref_cruzada4  IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET ref_cruzada5  = '' WHERE ref_cruzada5  IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET ref_cruzada6  = '' WHERE ref_cruzada6  IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET tipo_produto  = 'PRODUTO' WHERE tipo_produto IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET comb_cprodanp = '' WHERE comb_cprodanp IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET comb_descanp  = '' WHERE comb_descanp  IS NULL", t);

        // Nulos → 0 / FKs default
        execIgnore(c, "UPDATE "+t+" SET id_grupotributacao = 1    WHERE id_grupotributacao IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_categoria       = 1    WHERE id_categoria       IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_cfop            = 289  WHERE id_cfop            IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_cst             = 15   WHERE id_cst             IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_ncm             = 1    WHERE id_ncm             IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_cest            = 1    WHERE id_cest            IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_fabricante      = 1    WHERE id_fabricante      IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_fornecedor      = 1    WHERE id_fornecedor      IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidade         = 1    WHERE id_unidade         IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_subcategoria    = 1    WHERE id_subcategoria    IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_nutricional     = 0    WHERE id_nutricional     IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeembalagem= 0    WHERE id_unidadeembalagem IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado2 = id_unidade WHERE qtd_minimapv2 > 0 AND preco_venda2 > 0 AND id_unidadeatacado2 IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado3 = id_unidade WHERE qtd_minimapv3 > 0 AND preco_venda3 > 0 AND id_unidadeatacado3 IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado4 = id_unidade WHERE qtd_minimapv4 > 0 AND preco_venda4 > 0 AND id_unidadeatacado4 IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado2 = 0 WHERE id_unidadeatacado2 IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado3 = 0 WHERE id_unidadeatacado3 IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado4 = 0 WHERE id_unidadeatacado4 IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado2 = 0 WHERE qtd_minimapv2 <= 0 AND preco_venda2 <= 0", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado3 = 0 WHERE qtd_minimapv3 <= 0 AND preco_venda3 <= 0", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado4 = 0 WHERE qtd_minimapv4 <= 0 AND preco_venda4 <= 0", t);
        execIgnore(c, "UPDATE "+t+" SET origem_produto     = 0 WHERE origem_produto IS NULL OR origem_produto = ''", t);
        execIgnore(c, "UPDATE "+t+" SET trib_fatorunidade  = 0.000 WHERE trib_fatorunidade IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET trib_icmsaliqsaida = 0.000 WHERE trib_icmsaliqsaida IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET trib_icmsaliqredbasecalcsaida = 0.000 WHERE trib_icmsaliqredbasecalcsaida IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET trib_icmsfcpaliq   = 0.000 WHERE trib_icmsfcpaliq  IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET trib_issaliqsaida  = 0.000 WHERE trib_issaliqsaida IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET trib_ipialiqsaida  = 0.000 WHERE trib_ipialiqsaida IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET trib_pisaliqsaida  = 0.000 WHERE trib_pisaliqsaida IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET trib_cofinsaliqsaida = 0.000 WHERE trib_cofinsaliqsaida IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET preco_pmc          = 0.000 WHERE preco_pmc         IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET preco_custo        = 0.000 WHERE preco_custo       IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET preco_venda        = 0.000 WHERE preco_venda       IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET preco_compra = preco_custo  WHERE preco_compra IS NULL OR preco_compra = '0.000'", t);
        execIgnore(c, "UPDATE "+t+" SET preco_custo  = preco_compra WHERE preco_custo  IS NULL OR preco_custo  = '0.000'", t);
        execIgnore(c, "UPDATE "+t+" SET valor_compra = preco_compra WHERE valor_compra IS NULL OR valor_compra = '0.000'", t);
        execIgnore(c, "UPDATE "+t+" SET custo_medio       = 0.000 WHERE custo_medio       IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro      = 0.000 WHERE margem_lucro      IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET desconto_max      = 0.000 WHERE desconto_max      IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET preco_venda2      = 0.000 WHERE preco_venda2      IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro2     = 0.000 WHERE margem_lucro2     IS NULL OR preco_venda2 = 0.000", t);
        execIgnore(c, "UPDATE "+t+" SET qtd_minimapv2     = 0.000 WHERE qtd_minimapv2     IS NULL OR preco_venda2 = 0.000", t);
        execIgnore(c, "UPDATE "+t+" SET desconto_max2     = 0.000 WHERE desconto_max2     IS NULL OR preco_venda2 = 0.000", t);
        execIgnore(c, "UPDATE "+t+" SET preco_venda3      = 0.000 WHERE preco_venda3      IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro3     = 0.000 WHERE margem_lucro3     IS NULL OR preco_venda3 = 0.000", t);
        execIgnore(c, "UPDATE "+t+" SET qtd_minimapv3     = 0.000 WHERE qtd_minimapv3     IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET desconto_max3     = 0.000 WHERE desconto_max3     IS NULL OR preco_venda3 = 0.000", t);
        execIgnore(c, "UPDATE "+t+" SET preco_venda4      = 0.000 WHERE preco_venda4      IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro4     = 0.000 WHERE margem_lucro4     IS NULL OR preco_venda4 = 0.000", t);
        execIgnore(c, "UPDATE "+t+" SET qtd_minimapv4     = 0.000 WHERE qtd_minimapv4     IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET desconto_max4     = 0.000 WHERE desconto_max4     IS NULL OR preco_venda4 = 0.000", t);
        execIgnore(c, "UPDATE "+t+" SET preco_antigo      = 0.000 WHERE preco_antigo      IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET valor_frete       = 0.000 WHERE valor_frete       IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET margem_ideal      = 0.000 WHERE margem_ideal      IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET ipi              = 0.000 WHERE ipi               IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET preco_promocao   = 0.000 WHERE preco_promocao    IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET comissao         = 0.000 WHERE comissao          IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET comissao_valor   = 0.000 WHERE comissao_valor    IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET fidelidade_pontos= 0.000 WHERE fidelidade_pontos IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET estoque          = 0.000 WHERE estoque           IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET estoque          = 0.000 WHERE tipo_produto      = 'SERVICO'", t);
        execIgnore(c, "UPDATE "+t+" SET estoque          = 0     WHERE estoque           < 0", t);
        execIgnore(c, "UPDATE "+t+" SET estoque_minimo   = 0.000 WHERE estoque_minimo    IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET estoque_max      = 0.000 WHERE estoque_max       IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET estoque_tara     = 0.000 WHERE estoque_tara      IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET qtd_embalagem    = 1.000 WHERE qtd_embalagem IS NULL OR qtd_embalagem = 0", t);
        execIgnore(c, "UPDATE "+t+" SET qtd_diasvalidade = 0     WHERE qtd_diasvalidade  IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET peso_bruto       = 0.000 WHERE peso_bruto        IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET peso_liquido     = 0.000 WHERE peso_liquido      IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET comb_percentualgaspetroleo         = 0.000 WHERE comb_percentualgaspetroleo         IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET comb_percentualgasnaturalnacional  = 0.000 WHERE comb_percentualgasnaturalnacional  IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET comb_percentualgasnaturalimportado = 0.000 WHERE comb_percentualgasnaturalimportado IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET comb_valorpartida                  = 0.000 WHERE comb_valorpartida                  IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET comb_percentualbiodiesel           = 0.000 WHERE comb_percentualbiodiesel           IS NULL", t);

        // Flags S/N
        execIgnore(c, "UPDATE "+t+" SET ativo               = 1     WHERE ativo               IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET ativo               = 0     WHERE nome                = ''", t);
        execIgnore(c, "UPDATE "+t+" SET pode_desconto       = 'S'   WHERE pode_desconto       IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET pode_balanca        = 'N'   WHERE pode_balanca        IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET pode_fracionado     = 'N'   WHERE pode_fracionado     IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET pode_lote           = 'N'   WHERE pode_lote           IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET pode_comissao       = 'S'   WHERE pode_comissao       IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET pode_lerpeso        = 'N'   WHERE pode_lerpeso        IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET pode_atualizarncm  = 'S'   WHERE pode_atualizarncm   IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET pode_producao_propria = 'n' WHERE pode_producao_propria IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET pode_balanca    = IF(SUBSTRING_INDEX(estoque,'.',-1)>0,'S','N') WHERE pode_balanca    = 'N'", t);
        execIgnore(c, "UPDATE "+t+" SET pode_fracionado = IF(SUBSTRING_INDEX(estoque,'.',-1)>0,'S','N') WHERE pode_fracionado = 'N'", t);

        // Tributação defaults
        execIgnore(c, "UPDATE "+t+" SET trib_ipisaida    = '53' WHERE trib_ipisaida    IS NULL OR trib_ipisaida    = ''", t);
        execIgnore(c, "UPDATE "+t+" SET trib_pissaida    = '07' WHERE trib_pissaida    IS NULL OR trib_pissaida    = ''", t);
        execIgnore(c, "UPDATE "+t+" SET trib_cofinssaida = '07' WHERE trib_cofinssaida IS NULL OR trib_cofinssaida = ''", t);
        execIgnore(c, "UPDATE "+t+" SET trib_pissaida    = LPAD(trib_pissaida,2,'0')", t);
        execIgnore(c, "UPDATE "+t+" SET trib_cofinssaida = LPAD(trib_cofinssaida,2,'0')", t);

        // Datas
        execIgnore(c, "UPDATE "+t+" SET datahora_cadastro   = NOW() WHERE datahora_cadastro   IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET datahora_alteracao  = NOW() WHERE datahora_alteracao  IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET data_promocaoinicial = NULL WHERE data_promocaoinicial = '' OR data_promocaoinicial = '0000-00-00' OR preco_promocao = 0.000", t);
        execIgnore(c, "UPDATE "+t+" SET data_promocaofinal   = NULL WHERE data_promocaofinal   = '' OR data_promocaofinal   = '0000-00-00' OR preco_promocao = 0.000", t);

        // Normaliza decimais
        execIgnore(c, "UPDATE "+t+" SET preco_venda  = CONVERT(preco_venda,  DECIMAL(12,3))", t);
        execIgnore(c, "UPDATE "+t+" SET preco_venda2 = CONVERT(preco_venda2, DECIMAL(12,3))", t);
        execIgnore(c, "UPDATE "+t+" SET preco_venda3 = CONVERT(preco_venda3, DECIMAL(12,3))", t);
        execIgnore(c, "UPDATE "+t+" SET preco_venda4 = CONVERT(preco_venda4, DECIMAL(12,3))", t);
        execIgnore(c, "UPDATE "+t+" SET preco_custo  = CONVERT(preco_custo,  DECIMAL(12,3))", t);
        execIgnore(c, "UPDATE "+t+" SET preco_compra = CONVERT(preco_compra, DECIMAL(12,3))", t);

        // Margens
        execIgnore(c, "UPDATE "+t+" SET margem_lucro  = (preco_venda  - preco_custo)/preco_custo * 100 WHERE preco_venda  > 0", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro2 = (preco_venda2 - preco_custo)/preco_custo * 100 WHERE preco_venda2 > 0", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro3 = (preco_venda3 - preco_custo)/preco_custo * 100 WHERE preco_venda3 > 0", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro4 = (preco_venda4 - preco_custo)/preco_custo * 100 WHERE preco_venda4 > 0", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro  = CONVERT(margem_lucro,  DECIMAL(12,2))", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro2 = CONVERT(margem_lucro2, DECIMAL(12,2))", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro3 = CONVERT(margem_lucro3, DECIMAL(12,2))", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro4 = CONVERT(margem_lucro4, DECIMAL(12,2))", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro  = '0.000' WHERE margem_lucro  IS NULL OR margem_lucro  < 0", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro2 = '0.000' WHERE margem_lucro2 IS NULL OR margem_lucro2 < 0", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro3 = '0.000' WHERE margem_lucro3 IS NULL OR margem_lucro3 < 0", t);
        execIgnore(c, "UPDATE "+t+" SET margem_lucro4 = '0.000' WHERE margem_lucro4 IS NULL OR margem_lucro4 < 0", t);

        // Preços duplicados
        execIgnore(c, "UPDATE "+t+" SET preco_venda2 = 0, margem_lucro2 = 0 WHERE preco_venda2 = preco_venda", t);
        execIgnore(c, "UPDATE "+t+" SET preco_venda3 = 0, margem_lucro3 = 0 WHERE preco_venda3 = preco_venda  OR preco_venda3 = preco_venda2", t);
        execIgnore(c, "UPDATE "+t+" SET preco_venda4 = 0, margem_lucro4 = 0 WHERE preco_venda4 = preco_venda  OR preco_venda4 = preco_venda3 OR preco_venda4 = preco_venda2", t);
        execIgnore(c, "UPDATE "+t+" SET qtd_minimapv2 = 0.000 WHERE preco_venda2 = 0.000", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado2 = 0 WHERE preco_venda2 = 0.000", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado3 = 0 WHERE preco_venda3 = 0.000", t);
        execIgnore(c, "UPDATE "+t+" SET id_unidadeatacado4 = 0 WHERE preco_venda4 = 0.000", t);

        // EAN / medicamento / imendes (ignorados se colunas não existirem)
        execIgnore(c, "UPDATE "+t+" SET cod_ean      = NULL WHERE cod_ean      = ''", t);
        execIgnore(c, "UPDATE "+t+" SET codigo_med   = NULL WHERE cod_ean      = ''", t);
        execIgnore(c, "UPDATE "+t+" SET tipo_med     = '' WHERE tipo_med       IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET tabela_med   = '' WHERE tabela_med     IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET linha_med    = '' WHERE linha_med      IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET ref_anvisa_med = NULL WHERE ref_anvisa_med = ''", t);
        execIgnore(c, "UPDATE "+t+" SET portaria_med = '' WHERE portaria_med   IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET rms_med      = ' .    .    .   - ' WHERE rms_med IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET rms_med      = ' .    .    .   - ' WHERE rms_med = '...-'", t);
        execIgnore(c, "UPDATE "+t+" SET data_vigencia_med   = NULL WHERE data_vigencia_med   = ''", t);
        execIgnore(c, "UPDATE "+t+" SET edicao_pharmacos    = NULL WHERE edicao_pharmacos    = ''", t);
        execIgnore(c, "UPDATE "+t+" SET med_classeterapeutica         = '' WHERE med_classeterapeutica         IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET med_unidademedida             = '' WHERE med_unidademedida             IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET med_usoprolongado             = '' WHERE med_usoprolongado             IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET med_podeatualizar             = 'S' WHERE med_podeatualizar            IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET med_precovendafpop            = 0.000 WHERE med_precovendafpop         IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET med_margemfpop                = 0.000 WHERE med_margemfpop             IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET med_precoVendaFpopBolsaFamilia= 0.000 WHERE med_precoVendaFpopBolsaFamilia IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET med_margemFpopBolsaFamilia    = 0.000 WHERE med_margemFpopBolsaFamilia    IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET med_apresentacaofpop          = 0.000 WHERE med_apresentacaofpop           IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET imendes_codigointerno         = '' WHERE imendes_codigointerno         IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET imendes_produtonome           = '' WHERE imendes_produtonome           IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET imendes_datahoraalteracacao   = NULL WHERE imendes_datahoraalteracacao = ''", t);

        // TRIM + UPPER + SUBS
        execIgnore(c, "UPDATE "+t+" SET nome      = TRIM(UPPER(nome))", t);
        execIgnore(c, "UPDATE "+t+" SET descricao = TRIM(UPPER(descricao))", t);
        execIgnore(c, "UPDATE "+t+" SET codigo    = TRIM(codigo)", t);
        execIgnore(c, "UPDATE "+t+" SET referencia= TRIM(referencia)", t);
        execIgnore(c, "UPDATE "+t+" SET codigo_barras = TRIM(codigo_barras)", t);

        for (String campo : new String[]{"nome", "descricao"}) {
            for (String[] s : SUBS)  execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
            for (String[] s : SUBS2) execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
            execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+", LEFT("+campo+",1),'') WHERE LEFT("+campo+",1)  = ' '", t);
            execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+", RIGHT("+campo+",1),'') WHERE RIGHT("+campo+",1) = ' '", t);
        }
    }
}
