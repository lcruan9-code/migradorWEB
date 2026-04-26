package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;
import br.com.lcsistemas.syspdv.sql.SqlMemoryStore;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Ajuste pós-inserção de TODAS as tabelas migradas:
 *   fornecedor, produto, categoria, subcategoria, fabricante, cliente, unidade.
 * Popula também as tabelas de movimentação de estoque (ajusteestoque, estoque, estoquesaldo).
 *
 * DEVE rodar ANTES de GrupoTributacaoStep — o grupo tributário depende
 * de dados normalizados (tipo, IDs de FK, campos de tributo).
 *
 * Todos os UPDATEs usam execIgnore(): colunas inexistentes são ignoradas
 * silenciosamente, garantindo compatibilidade com diferentes versões do banco.
 *
 * Complementa o AjustePosMigracaoStep (que executa scripts externos opcionais).
 * Este step é hardcoded e sempre executa, independente de arquivos externos.
 *
 * VERSÃO IN-MEMORY: Se SqlMemoryStore disponível (modo SQL Output), aplica
 * todas as correções diretamente nos maps em memória, evitando UPDATEs no SQL dump.
 */
public class AjusteGeralStep extends StepBase {

    @Override public String getNome() { return "AjusteGeral"; }

    /**
     * SUBS — acentos/caracteres especiais (unicode direto).
     * Aplicado em todos os campos de texto das tabelas migradas.
     */
    private static final String[][] SUBS = {
        {"\u00C0","A"},{"\u00C1","A"},{"\u00C3","A"},{"\u00C2","A"},
        {"\u00C8","E"},{"\u00C9","E"},{"\u00CA","E"},
        {"\u00CC","I"},{"\u00CD","I"},{"\u00CE","I"},
        {"\u00D9","U"},{"\u00DA","U"},{"\u00DB","U"},{"\u00DC","U"},
        {"\u00C7","C"},{"\u00D5","O"},
        {"A\u00A7A\u00A3","CA"},  // sequência corrompida → CA
        {"\u00B0"," "},{"\u00BA"," "},{"\u00AA"," "},
        {"\u0192",""},   // ƒ
        {"\u2030",""},   // ‰
        {"\u2022",""},   // •
        {"\u0160",""},   // Š
        {"\u2021",""},   // ‡
        {"  "," "}
    };

    /**
     * SUBS2 — mojibake (UTF-8 bytes lidos como Latin-1).
     * Cobre casos de dupla-codificação frequentes em bancos exportados com charset errado.
     */
    private static final String[][] SUBS2 = {
        {"\u00C3\u00A7\u00C3\u00A3","CA"},
        {"\u00C3\u00A7","C"},
        {"\u00C3\u00A3","A"},
        {"\u00C3\u00A2","A"},
        {"\u00C3\u00A0","A"},
        {"\u00C3\u00A1","A"},
        {"\u00C3\u00A9","E"},
        {"\u00C3\u00AA","E"},
        {"\u00C3\u00AD","I"},
        {"\u00C3\u00BA","U"},
        {"\u00C3\u00B5","O"},
        {"\u00C3\u009F",""},
        {"\u00C2\u00B0",""},
        {"\u00C2\u00BA",""},
        {"<span id='product_description'>",""},
        {"\n\n",""},
        {"\n",""},
    };

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        SqlMemoryStore store = ctx.getMemoryStore();
        if (store != null) {
            // In-memory mode: apply all corrections directly to store rows
            int cidadeDefault = ctx.getConfig().getCidadeDefaultId();
            int estadoDefault = ctx.getConfig().getEstadoDefaultId();
            ajustarFornecedorMem(store);
            ajustarProdutoMem(store);
            ajustarCategoriaMem(store, "categoria");
            ajustarCategoriaMem(store, "subcategoria");
            ajustarCategoriaMem(store, "fabricante");
            ajustarUnidadeMem(store);
            ajustarClienteMem(store, cidadeDefault, estadoDefault);
            popularEstoqueMem(store);
        } else {
            // SQL mode (real MySQL / H2).
            // autoCommit=true: cada UPDATE é transação própria — elimina qualquer
            // acúmulo de MVCC/undo no H2 (causa de OOM com 5000+ produtos).
            Connection c = ctx.getDestinoConn();
            int cidadeDefault = ctx.getConfig().getCidadeDefaultId();
            int estadoDefault = ctx.getConfig().getEstadoDefaultId();

            try { c.setAutoCommit(true); }
            catch (Exception e) { LOG.warning("[AjusteGeral] setAutoCommit(true): " + e.getMessage()); }

            try {
                ajustarFornecedor(c);
                ajustarProduto(c);
                ajustarCategoria(c);
                ajustarSubcategoria(c);
                ajustarFabricante(c);
                ajustarUnidade(c);
                ajustarCliente(c, cidadeDefault, estadoDefault);
                popularEstoque(c);
            } finally {
                // Restaura autoCommit=false para o commit() do executor externo
                try { c.setAutoCommit(false); }
                catch (Exception e) { LOG.warning("[AjusteGeral] setAutoCommit(false): " + e.getMessage()); }
            }
        }
        contarInseridos(ctx, 0);
    }

    // =========================================================================
    //  HELPER — commit intermediário (libera buffer de UNDO do H2)
    // =========================================================================
    private void commitSilent(java.sql.Connection c, String fase) {
        try {
            c.commit();
            LOG.fine("[AjusteGeral] commit parcial OK: " + fase);
        } catch (java.sql.SQLException e) {
            LOG.warning("[AjusteGeral] commit parcial AVISO (" + fase + "): " + e.getMessage());
        }
    }

    // =========================================================================
    //  HELPERS — in-memory utilities
    // =========================================================================
    private static String nowTs() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }
    private static String nowDate() {
        return new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    }
    private static String safeStr(Object v) { return v == null ? "" : v.toString(); }
    private static double safeDbl(Object v) {
        if (v == null) return 0.0;
        try { return Double.parseDouble(v.toString()); } catch (NumberFormatException e) { return 0.0; }
    }
    private static int safeInt(Object v) {
        if (v == null) return 0;
        try { return Integer.parseInt(v.toString()); } catch (NumberFormatException e) { return 0; }
    }
    private static boolean isNullOrEmpty(Object v) { return v == null || v.toString().isEmpty(); }
    private static boolean isNull(Object v)        { return v == null; }
    private static String stripDoc(Object v) {
        if (v == null) return "";
        return v.toString().replaceAll("[\\-/\\. ,]","");
    }
    private static boolean temFracionado(Object v) {
        if (v == null) return false;
        String s = v.toString();
        int dot = s.indexOf('.'); if (dot < 0) return false;
        try { return Integer.parseInt(s.substring(dot+1)) > 0; } catch (Exception e) { return false; }
    }
    private static double round2(double d) { return Math.round(d * 100.0) / 100.0; }
    private static double round3(double d) { return Math.round(d * 1000.0) / 1000.0; }
    private static String lpad2(Object v) {
        if (v == null || v.toString().isEmpty()) return "07";
        try { return String.format("%02d", Integer.parseInt(v.toString())); }
        catch (NumberFormatException e) { return v.toString(); }
    }
    private static String applyAllSubs(String s) {
        if (s == null) return s;
        for (String[] sub : SUBS)  s = s.replace(sub[0], sub[1]);
        for (String[] sub : SUBS2) s = s.replace(sub[0], sub[1]);
        return s;
    }
    private static String formatCnpj(String d) {
        if (d == null || d.length() != 14) return d;
        return d.substring(0,2)+"."+d.substring(2,5)+"."+d.substring(5,8)+"/"+d.substring(8,12)+"-"+d.substring(12);
    }
    private static String formatCpf(String d) {
        if (d == null || d.length() != 11) return d;
        return d.substring(0,3)+"."+d.substring(3,6)+"."+d.substring(6,9)+"-"+d.substring(9);
    }

    // =========================================================================
    //  FORNECEDOR — in-memory
    // =========================================================================
    private void ajustarFornecedorMem(SqlMemoryStore store) {
        List<Map<String,Object>> rows = store.selectAll("fornecedor");
        for (Map<String,Object> r : rows) {
            // Strip CNPJ
            String cnpj = stripDoc(r.get("cnpj_cpf"));
            r.put("cnpj_cpf", cnpj);

            // Strip CEP
            String cep = stripDoc(r.get("cep"));
            r.put("cep", cep);

            // Tipo
            r.put("tipo", cnpj.length() <= 11 ? "F" : "J");

            // Defaults
            if (isNull(r.get("tipo_fornecedor"))) r.put("tipo_fornecedor", "Outros");
            if (isNull(r.get("id_planoContas"))) r.put("id_planoContas", 0);
            if (isNull(r.get("nome"))) r.put("nome", "");
            if (isNull(r.get("razao_social"))) r.put("razao_social", "");
            if (isNull(r.get("ie"))) r.put("ie", "");
            if (isNull(r.get("endereco"))) r.put("endereco", "");
            if (isNull(r.get("numero"))) r.put("numero", "");
            if (isNull(r.get("bairro"))) r.put("bairro", "");
            if (isNull(r.get("email_site"))) r.put("email_site", "");
            if (isNull(r.get("obs"))) r.put("obs", "");
            if (isNull(r.get("ativo"))) r.put("ativo", 1);

            String fone = safeStr(r.get("fone"));
            String fax = safeStr(r.get("fax"));
            if (fone.isEmpty()) r.put("fone", "(  )     -    ");
            if (fax.isEmpty()) r.put("fax", "(  )     -    ");

            // Nome fallbacks
            String nome = safeStr(r.get("nome"));
            String razao = safeStr(r.get("razao_social"));
            if (nome.isEmpty()) r.put("nome", razao);
            if (razao.isEmpty()) r.put("razao_social", nome);

            // Upper + SUBS
            nome = safeStr(r.get("nome")).toUpperCase();
            razao = safeStr(r.get("razao_social")).toUpperCase();
            String endereco = safeStr(r.get("endereco")).toUpperCase();
            String bairro = safeStr(r.get("bairro")).toUpperCase();

            nome = applyAllSubs(nome);
            razao = applyAllSubs(razao);
            endereco = applyAllSubs(endereco);
            bairro = applyAllSubs(bairro);

            r.put("nome", nome);
            r.put("razao_social", razao);
            r.put("endereco", endereco);
            r.put("bairro", bairro);

            // Format CNPJ/CPF
            cnpj = safeStr(r.get("cnpj_cpf"));
            if (cnpj.length() == 14 && !cnpj.contains(".")) {
                r.put("cnpj_cpf", formatCnpj(cnpj));
            } else if (cnpj.length() == 11 && !cnpj.contains(".")) {
                r.put("cnpj_cpf", formatCpf(cnpj));
            }
        }
    }

    // =========================================================================
    //  PRODUTO — in-memory
    // =========================================================================
    private void ajustarProdutoMem(SqlMemoryStore store) {
        List<Map<String,Object>> rows = store.selectAll("produto");
        for (Map<String,Object> p : rows) {
            // Clear SEM/GTIN
            String ref = safeStr(p.get("referencia"));
            String cod = safeStr(p.get("codigo"));
            String codBar = safeStr(p.get("codigo_barras"));

            if (ref.contains("SEM") || ref.contains("GTIN")) p.put("referencia", "");
            if (cod.contains("SEM") || cod.contains("GTIN")) p.put("codigo", "");
            if (codBar.contains("SEM") || codBar.contains("GTIN")) p.put("codigo_barras", "");

            // Strip *
            ref = safeStr(p.get("referencia")).replace("*","").replace(" ","");
            cod = safeStr(p.get("codigo")).replace("*","").replace(" ","");
            codBar = safeStr(p.get("codigo_barras")).replace("*","").replace(" ","");
            p.put("referencia", ref);
            p.put("codigo", cod);
            p.put("codigo_barras", codBar);

            // String nulls to ''
            if (isNull(p.get("nome"))) p.put("nome", "");
            if (isNull(p.get("descricao"))) p.put("descricao", "");
            if (isNull(p.get("ex_tipi"))) p.put("ex_tipi", "");
            if (isNull(p.get("trib_icmsobs"))) p.put("trib_icmsobs", "");
            if (isNull(p.get("trib_unidadetributavel"))) p.put("trib_unidadetributavel", "");
            if (isNull(p.get("trib_genero"))) p.put("trib_genero", "");
            if (isNull(p.get("observacoes"))) p.put("observacoes", "");
            if (isNull(p.get("foto"))) p.put("foto", "");
            if (isNull(p.get("foto2"))) p.put("foto2", "");
            if (isNull(p.get("foto3"))) p.put("foto3", "");
            if (isNull(p.get("local"))) p.put("local", "");
            if (isNull(p.get("ref_cruzada1"))) p.put("ref_cruzada1", "");
            if (isNull(p.get("ref_cruzada2"))) p.put("ref_cruzada2", "");
            if (isNull(p.get("ref_cruzada3"))) p.put("ref_cruzada3", "");
            if (isNull(p.get("ref_cruzada4"))) p.put("ref_cruzada4", "");
            if (isNull(p.get("ref_cruzada5"))) p.put("ref_cruzada5", "");
            if (isNull(p.get("ref_cruzada6"))) p.put("ref_cruzada6", "");
            if (isNull(p.get("tipo_med"))) p.put("tipo_med", "");
            if (isNull(p.get("tabela_med"))) p.put("tabela_med", "");
            if (isNull(p.get("linha_med"))) p.put("linha_med", "");
            if (isNull(p.get("portaria_med"))) p.put("portaria_med", "");
            if (isNull(p.get("med_classeterapeutica"))) p.put("med_classeterapeutica", "");
            if (isNull(p.get("med_unidademedida"))) p.put("med_unidademedida", "");
            if (isNull(p.get("med_usoprolongado"))) p.put("med_usoprolongado", "");
            if (isNull(p.get("imendes_codigointerno"))) p.put("imendes_codigointerno", "");
            if (isNull(p.get("imendes_produtonome"))) p.put("imendes_produtonome", "");
            if (isNull(p.get("comb_cprodanp"))) p.put("comb_cprodanp", "");
            if (isNull(p.get("comb_descanp"))) p.put("comb_descanp", "");

            // Tipo produto
            if (isNull(p.get("tipo_produto"))) p.put("tipo_produto", "PRODUTO");

            // Int FK defaults
            if (isNull(p.get("id_grupotributacao"))) p.put("id_grupotributacao", 1);
            if (isNull(p.get("id_categoria"))) p.put("id_categoria", 1);
            if (isNull(p.get("id_cfop"))) p.put("id_cfop", 289);
            if (isNull(p.get("id_cst"))) p.put("id_cst", 15);
            if (isNull(p.get("id_ncm"))) p.put("id_ncm", 1);
            if (isNull(p.get("id_cest"))) p.put("id_cest", 1);
            if (isNull(p.get("id_fabricante"))) p.put("id_fabricante", 1);
            if (isNull(p.get("id_fornecedor"))) p.put("id_fornecedor", 1);
            if (isNull(p.get("id_unidade"))) p.put("id_unidade", 1);
            if (isNull(p.get("id_subcategoria"))) p.put("id_subcategoria", 1);
            if (isNull(p.get("id_nutricional"))) p.put("id_nutricional", 0);
            if (isNull(p.get("id_unidadeembalagem"))) p.put("id_unidadeembalagem", 0);

            // Unidade atacado
            double qtd2 = safeDbl(p.get("qtd_minimapv2"));
            double pv2 = safeDbl(p.get("preco_venda2"));
            if (qtd2 > 0 && pv2 > 0 && isNull(p.get("id_unidadeatacado2"))) {
                p.put("id_unidadeatacado2", p.get("id_unidade"));
            }
            double qtd3 = safeDbl(p.get("qtd_minimapv3"));
            double pv3 = safeDbl(p.get("preco_venda3"));
            if (qtd3 > 0 && pv3 > 0 && isNull(p.get("id_unidadeatacado3"))) {
                p.put("id_unidadeatacado3", p.get("id_unidade"));
            }
            double qtd4 = safeDbl(p.get("qtd_minimapv4"));
            double pv4 = safeDbl(p.get("preco_venda4"));
            if (qtd4 > 0 && pv4 > 0 && isNull(p.get("id_unidadeatacado4"))) {
                p.put("id_unidadeatacado4", p.get("id_unidade"));
            }

            if (isNull(p.get("id_unidadeatacado2"))) p.put("id_unidadeatacado2", 0);
            if (isNull(p.get("id_unidadeatacado3"))) p.put("id_unidadeatacado3", 0);
            if (isNull(p.get("id_unidadeatacado4"))) p.put("id_unidadeatacado4", 0);

            // Double nulls
            if (isNull(p.get("preco_pmc"))) p.put("preco_pmc", 0.0);
            if (isNull(p.get("preco_custo"))) p.put("preco_custo", 0.0);
            if (isNull(p.get("preco_venda"))) p.put("preco_venda", 0.0);
            if (isNull(p.get("preco_compra"))) p.put("preco_compra", 0.0);
            if (isNull(p.get("valor_compra"))) p.put("valor_compra", 0.0);
            if (isNull(p.get("custo_medio"))) p.put("custo_medio", 0.0);
            if (isNull(p.get("margem_lucro"))) p.put("margem_lucro", 0.0);
            if (isNull(p.get("desconto_max"))) p.put("desconto_max", 0.0);
            if (isNull(p.get("preco_venda2"))) p.put("preco_venda2", 0.0);
            if (isNull(p.get("margem_lucro2"))) p.put("margem_lucro2", 0.0);
            if (isNull(p.get("qtd_minimapv2"))) p.put("qtd_minimapv2", 0.0);
            if (isNull(p.get("desconto_max2"))) p.put("desconto_max2", 0.0);
            if (isNull(p.get("preco_venda3"))) p.put("preco_venda3", 0.0);
            if (isNull(p.get("margem_lucro3"))) p.put("margem_lucro3", 0.0);
            if (isNull(p.get("qtd_minimapv3"))) p.put("qtd_minimapv3", 0.0);
            if (isNull(p.get("desconto_max3"))) p.put("desconto_max3", 0.0);
            if (isNull(p.get("preco_venda4"))) p.put("preco_venda4", 0.0);
            if (isNull(p.get("margem_lucro4"))) p.put("margem_lucro4", 0.0);
            if (isNull(p.get("qtd_minimapv4"))) p.put("qtd_minimapv4", 0.0);
            if (isNull(p.get("desconto_max4"))) p.put("desconto_max4", 0.0);
            if (isNull(p.get("preco_antigo"))) p.put("preco_antigo", 0.0);
            if (isNull(p.get("valor_frete"))) p.put("valor_frete", 0.0);
            if (isNull(p.get("margem_ideal"))) p.put("margem_ideal", 0.0);
            if (isNull(p.get("ipi"))) p.put("ipi", 0.0);
            if (isNull(p.get("preco_promocao"))) p.put("preco_promocao", 0.0);
            if (isNull(p.get("comissao"))) p.put("comissao", 0.0);
            if (isNull(p.get("comissao_valor"))) p.put("comissao_valor", 0.0);
            if (isNull(p.get("fidelidade_pontos"))) p.put("fidelidade_pontos", 0.0);
            if (isNull(p.get("estoque"))) p.put("estoque", 0.0);
            if (isNull(p.get("estoque_minimo"))) p.put("estoque_minimo", 0.0);
            if (isNull(p.get("estoque_max"))) p.put("estoque_max", 0.0);
            if (isNull(p.get("estoque_tara"))) p.put("estoque_tara", 0.0);
            if (isNull(p.get("peso_bruto"))) p.put("peso_bruto", 0.0);
            if (isNull(p.get("peso_liquido"))) p.put("peso_liquido", 0.0);
            if (isNull(p.get("trib_fatorunidade"))) p.put("trib_fatorunidade", 0.0);
            if (isNull(p.get("trib_icmsaliqsaida"))) p.put("trib_icmsaliqsaida", 0.0);
            if (isNull(p.get("trib_icmsaliqredbasecalcsaida"))) p.put("trib_icmsaliqredbasecalcsaida", 0.0);
            if (isNull(p.get("trib_icmsfcpaliq"))) p.put("trib_icmsfcpaliq", 0.0);
            if (isNull(p.get("trib_issaliqsaida"))) p.put("trib_issaliqsaida", 0.0);
            if (isNull(p.get("trib_ipialiqsaida"))) p.put("trib_ipialiqsaida", 0.0);
            if (isNull(p.get("trib_pisaliqsaida"))) p.put("trib_pisaliqsaida", 0.0);
            if (isNull(p.get("trib_cofinsaliqsaida"))) p.put("trib_cofinsaliqsaida", 0.0);
            if (isNull(p.get("comb_percentualgaspetroleo"))) p.put("comb_percentualgaspetroleo", 0.0);
            if (isNull(p.get("comb_percentualgasnaturalnacional"))) p.put("comb_percentualgasnaturalnacional", 0.0);
            if (isNull(p.get("comb_percentualgasnaturalimportado"))) p.put("comb_percentualgasnaturalimportado", 0.0);
            if (isNull(p.get("comb_valorpartida"))) p.put("comb_valorpartida", 0.0);
            if (isNull(p.get("comb_percentualbiodiesel"))) p.put("comb_percentualbiodiesel", 0.0);
            if (isNull(p.get("med_precovendafpop"))) p.put("med_precovendafpop", 0.0);
            if (isNull(p.get("med_margemfpop"))) p.put("med_margemfpop", 0.0);
            if (isNull(p.get("med_precoVendaFpopBolsaFamilia"))) p.put("med_precoVendaFpopBolsaFamilia", 0.0);
            if (isNull(p.get("med_margemFpopBolsaFamilia"))) p.put("med_margemFpopBolsaFamilia", 0.0);
            if (isNull(p.get("med_apresentacaofpop"))) p.put("med_apresentacaofpop", 0.0);

            double qtdEmb = safeDbl(p.get("qtd_embalagem"));
            if (qtdEmb == 0) p.put("qtd_embalagem", 1.0);

            String qtdDias = safeStr(p.get("qtd_diasvalidade"));
            if (qtdDias.isEmpty()) p.put("qtd_diasvalidade", "0");

            String origem = safeStr(p.get("origem_produto"));
            if (origem.isEmpty()) p.put("origem_produto", "0");

            // Flags
            if (isNull(p.get("ativo"))) p.put("ativo", 1);
            if (safeStr(p.get("nome")).isEmpty()) p.put("ativo", 0);
            if (isNull(p.get("pode_desconto"))) p.put("pode_desconto", "S");
            if (isNull(p.get("pode_balanca"))) p.put("pode_balanca", "N");
            if (isNull(p.get("pode_fracionado"))) p.put("pode_fracionado", "N");
            if (isNull(p.get("pode_lote"))) p.put("pode_lote", "N");
            if (isNull(p.get("pode_comissao"))) p.put("pode_comissao", "S");
            if (isNull(p.get("pode_lerpeso"))) p.put("pode_lerpeso", "N");
            if (isNull(p.get("pode_atualizarncm"))) p.put("pode_atualizarncm", "S");
            if (isNull(p.get("pode_producao_propria"))) p.put("pode_producao_propria", "n");
            if (isNull(p.get("med_podeatualizar"))) p.put("med_podeatualizar", "S");

            // Fracionado
            double estoque = safeDbl(p.get("estoque"));
            if (temFracionado(estoque)) {
                if ("N".equals(p.get("pode_balanca"))) p.put("pode_balanca", "S");
                if ("N".equals(p.get("pode_fracionado"))) p.put("pode_fracionado", "S");
            }

            // Strings
            String tribIpi = safeStr(p.get("trib_ipisaida"));
            if (tribIpi.isEmpty()) p.put("trib_ipisaida", "53");
            String tribPis = safeStr(p.get("trib_pissaida"));
            if (tribPis.isEmpty()) p.put("trib_pissaida", "07");
            String tribCofins = safeStr(p.get("trib_cofinssaida"));
            if (tribCofins.isEmpty()) p.put("trib_cofinssaida", "07");

            // Lpad
            tribPis = lpad2(p.get("trib_pissaida"));
            tribCofins = lpad2(p.get("trib_cofinssaida"));
            p.put("trib_pissaida", tribPis);
            p.put("trib_cofinssaida", tribCofins);

            String rms = safeStr(p.get("rms_med"));
            if (rms.isEmpty() || "...-".equals(rms)) p.put("rms_med", " .    .    .   - ");

            // Dates
            if (isNull(p.get("datahora_cadastro"))) p.put("datahora_cadastro", nowTs());
            if (isNull(p.get("datahora_alteracao"))) p.put("datahora_alteracao", nowTs());

            String dataPromoIni = safeStr(p.get("data_promocaoinicial"));
            String dataPromoFim = safeStr(p.get("data_promocaofinal"));
            double precoPromo = safeDbl(p.get("preco_promocao"));
            if (dataPromoIni.isEmpty() || "0000-00-00".equals(dataPromoIni) || precoPromo == 0) {
                p.put("data_promocaoinicial", null);
            }
            if (dataPromoFim.isEmpty() || "0000-00-00".equals(dataPromoFim) || precoPromo == 0) {
                p.put("data_promocaofinal", null);
            }

            // Price fixes
            double precoCompra = safeDbl(p.get("preco_compra"));
            double precoCusto = safeDbl(p.get("preco_custo"));
            double valorCompra = safeDbl(p.get("valor_compra"));

            if (precoCompra == 0) p.put("preco_compra", precoCusto);
            if (precoCusto == 0) p.put("preco_custo", precoCompra);
            if (valorCompra == 0) p.put("valor_compra", precoCompra);

            // Margins
            double precoVenda = safeDbl(p.get("preco_venda"));
            precoCusto = safeDbl(p.get("preco_custo"));
            if (precoVenda > 0 && precoCusto > 0) {
                double margem = round2((precoVenda - precoCusto) / precoCusto * 100.0);
                p.put("margem_lucro", margem);
            }

            pv2 = safeDbl(p.get("preco_venda2"));
            if (pv2 > 0 && precoCusto > 0) {
                double margem2 = round2((pv2 - precoCusto) / precoCusto * 100.0);
                p.put("margem_lucro2", margem2);
            }

            pv3 = safeDbl(p.get("preco_venda3"));
            if (pv3 > 0 && precoCusto > 0) {
                double margem3 = round2((pv3 - precoCusto) / precoCusto * 100.0);
                p.put("margem_lucro3", margem3);
            }

            pv4 = safeDbl(p.get("preco_venda4"));
            if (pv4 > 0 && precoCusto > 0) {
                double margem4 = round2((pv4 - precoCusto) / precoCusto * 100.0);
                p.put("margem_lucro4", margem4);
            }

            // Margem negativa
            double margem = safeDbl(p.get("margem_lucro"));
            if (margem < 0) p.put("margem_lucro", 0.0);
            double margem2 = safeDbl(p.get("margem_lucro2"));
            if (margem2 < 0) p.put("margem_lucro2", 0.0);
            double margem3 = safeDbl(p.get("margem_lucro3"));
            if (margem3 < 0) p.put("margem_lucro3", 0.0);
            double margem4 = safeDbl(p.get("margem_lucro4"));
            if (margem4 < 0) p.put("margem_lucro4", 0.0);

            // Preços duplicados
            pv2 = safeDbl(p.get("preco_venda2"));
            pv3 = safeDbl(p.get("preco_venda3"));
            pv4 = safeDbl(p.get("preco_venda4"));
            precoVenda = safeDbl(p.get("preco_venda"));

            if (pv2 == precoVenda) {
                p.put("preco_venda2", 0.0);
                p.put("margem_lucro2", 0.0);
            }
            if (pv3 == precoVenda || pv3 == pv2) {
                p.put("preco_venda3", 0.0);
                p.put("margem_lucro3", 0.0);
            }
            if (pv4 == precoVenda || pv4 == pv3 || pv4 == pv2) {
                p.put("preco_venda4", 0.0);
                p.put("margem_lucro4", 0.0);
            }

            // Reset unidade atacado se preco = 0
            if (safeDbl(p.get("preco_venda2")) == 0) {
                p.put("id_unidadeatacado2", 0);
                p.put("qtd_minimapv2", 0.0);
            }
            if (safeDbl(p.get("preco_venda3")) == 0) {
                p.put("id_unidadeatacado3", 0);
            }
            if (safeDbl(p.get("preco_venda4")) == 0) {
                p.put("id_unidadeatacado4", 0);
            }

            // Estoque < 0
            estoque = safeDbl(p.get("estoque"));
            if (estoque < 0) p.put("estoque", 0.0);

            // Estoque servico
            String tipoProduto = safeStr(p.get("tipo_produto"));
            if ("SERVICO".equals(tipoProduto)) p.put("estoque", 0.0);

            // TRIM + UPPER + SUBS
            String nome = safeStr(p.get("nome")).trim().toUpperCase();
            String desc = safeStr(p.get("descricao")).trim().toUpperCase();
            nome = applyAllSubs(nome);
            desc = applyAllSubs(desc);
            p.put("nome", nome);
            p.put("descricao", desc);
        }
    }

    // =========================================================================
    //  CATEGORIA/SUBCATEGORIA/FABRICANTE — in-memory
    // =========================================================================
    private void ajustarCategoriaMem(SqlMemoryStore store, String table) {
        List<Map<String,Object>> rows = store.selectAll(table);
        for (Map<String,Object> r : rows) {
            if (isNull(r.get("nome"))) r.put("nome", "");
            String nome = safeStr(r.get("nome")).trim().toUpperCase();
            nome = applyAllSubs(nome);
            r.put("nome", nome);
        }
    }

    // =========================================================================
    //  UNIDADE — in-memory
    // =========================================================================
    private void ajustarUnidadeMem(SqlMemoryStore store) {
        List<Map<String,Object>> rows = store.selectAll("unidade");
        for (Map<String,Object> r : rows) {
            if (isNull(r.get("nome"))) r.put("nome", "");
            if (isNull(r.get("descricao"))) r.put("descricao", "");
            String nome = safeStr(r.get("nome")).trim().toUpperCase();
            String desc = safeStr(r.get("descricao")).trim().toUpperCase();
            nome = applyAllSubs(nome);
            desc = applyAllSubs(desc);
            r.put("nome", nome);
            r.put("descricao", desc);
        }
    }

    // =========================================================================
    //  CLIENTE — in-memory
    // =========================================================================
    private void ajustarClienteMem(SqlMemoryStore store, int cidadeDefault, int estadoDefault) {
        List<Map<String,Object>> rows = store.selectAll("cliente");
        for (Map<String,Object> c : rows) {
            // Strip CPF/CNPJ
            String cpf = stripDoc(c.get("cpf_cnpj"));
            c.put("cpf_cnpj", cpf);

            // Tipo
            String tipo = "F";
            if (cpf.length() == 11) tipo = "F";
            else if (cpf.length() == 14) tipo = "J";
            else if (!cpf.isEmpty()) tipo = "E";
            c.put("tipo", tipo);

            // Estrangeiro
            if ("E".equals(tipo)) {
                c.put("id_cidade", 5565);
                c.put("id_cidade2", 5565);
                c.put("id_estado", 28);
                c.put("id_estado2", 28);
                c.put("id_pais", "78");
            }

            // Strip CEP
            String cep = stripDoc(c.get("cep"));
            c.put("cep", cep);

            // String nulls to ''
            if (isNull(c.get("ie"))) c.put("ie", "");
            if (isNull(c.get("im"))) c.put("im", "");
            if (isNull(c.get("rg"))) c.put("rg", "");
            if (isNull(c.get("isuf"))) c.put("isuf", "");
            if (isNull(c.get("razao_social"))) c.put("razao_social", "");
            if (isNull(c.get("endereco"))) c.put("endereco", "");
            if (isNull(c.get("referencia"))) c.put("referencia", "");
            if (isNull(c.get("bairro"))) c.put("bairro", "");
            if (isNull(c.get("telefone"))) c.put("telefone", "");
            if (isNull(c.get("tel_comercial"))) c.put("tel_comercial", "");
            if (isNull(c.get("fax"))) c.put("fax", "");
            if (isNull(c.get("obs"))) c.put("obs", "");
            if (isNull(c.get("foto"))) c.put("foto", "");
            if (isNull(c.get("numero_contrato"))) c.put("numero_contrato", "");
            if (isNull(c.get("orgao"))) c.put("orgao", "");
            if (isNull(c.get("referencias"))) c.put("referencias", "");
            if (isNull(c.get("comercial_1"))) c.put("comercial_1", "");
            if (isNull(c.get("comercial_2"))) c.put("comercial_2", "");
            if (isNull(c.get("comercial_3"))) c.put("comercial_3", "");
            if (isNull(c.get("bancaria_1"))) c.put("bancaria_1", "");
            if (isNull(c.get("bancaria_2"))) c.put("bancaria_2", "");
            if (isNull(c.get("pai_adi"))) c.put("pai_adi", "");
            if (isNull(c.get("mae_adi"))) c.put("mae_adi", "");
            if (isNull(c.get("sexo_adi"))) c.put("sexo_adi", "");
            if (isNull(c.get("estcivil_adi"))) c.put("estcivil_adi", "");
            if (isNull(c.get("apelido_adi"))) c.put("apelido_adi", "");
            if (isNull(c.get("email_adi"))) c.put("email_adi", "");
            if (isNull(c.get("empresa"))) c.put("empresa", "");
            if (isNull(c.get("fone_emp"))) c.put("fone_emp", "");
            if (isNull(c.get("endereco_emp"))) c.put("endereco_emp", "");
            if (isNull(c.get("numero_emp"))) c.put("numero_emp", "");
            if (isNull(c.get("cep_emp"))) c.put("cep_emp", "");
            if (isNull(c.get("bairro_emp"))) c.put("bairro_emp", "");
            if (isNull(c.get("cargo_emp"))) c.put("cargo_emp", "");
            if (isNull(c.get("conjuje"))) c.put("conjuje", "");
            if (isNull(c.get("cpf_conj"))) c.put("cpf_conj", "");
            if (isNull(c.get("rg_conj"))) c.put("rg_conj", "");
            if (isNull(c.get("empresa_conj"))) c.put("empresa_conj", "");
            if (isNull(c.get("fone_conj"))) c.put("fone_conj", "");
            if (isNull(c.get("endereco_conj"))) c.put("endereco_conj", "");
            if (isNull(c.get("numero_conj"))) c.put("numero_conj", "");
            if (isNull(c.get("cep_conj"))) c.put("cep_conj", "");
            if (isNull(c.get("bairro_conj"))) c.put("bairro_conj", "");
            if (isNull(c.get("cargo_conj"))) c.put("cargo_conj", "");
            if (isNull(c.get("filiacao_endereco"))) c.put("filiacao_endereco", "");
            if (isNull(c.get("filiacao_referencia"))) c.put("filiacao_referencia", "");
            if (isNull(c.get("filiacao_numero"))) c.put("filiacao_numero", "");
            if (isNull(c.get("filiacao_cep"))) c.put("filiacao_cep", "");
            if (isNull(c.get("filiacao_bairro"))) c.put("filiacao_bairro", "");
            if (isNull(c.get("avalista_nome"))) c.put("avalista_nome", "");
            if (isNull(c.get("avalista_rg"))) c.put("avalista_rg", "");
            if (isNull(c.get("avalista_endereco"))) c.put("avalista_endereco", "");
            if (isNull(c.get("avalista_numero"))) c.put("avalista_numero", "");
            if (isNull(c.get("avalista_cep"))) c.put("avalista_cep", "");
            if (isNull(c.get("avalista_bairro"))) c.put("avalista_bairro", "");
            if (isNull(c.get("avalista_empresa"))) c.put("avalista_empresa", "");
            if (isNull(c.get("avalista_cargo"))) c.put("avalista_cargo", "");
            if (isNull(c.get("endereco2"))) c.put("endereco2", "");
            if (isNull(c.get("numero2"))) c.put("numero2", "");
            if (isNull(c.get("referencia2"))) c.put("referencia2", "");
            if (isNull(c.get("cep2"))) c.put("cep2", "");
            if (isNull(c.get("bairro2"))) c.put("bairro2", "");

            // Numero default
            String numero = safeStr(c.get("numero"));
            if (numero.isEmpty()) c.put("numero", "SN");

            // ie_indicador
            String ie = safeStr(c.get("ie"));
            if (isNull(c.get("ie_indicador"))) c.put("ie_indicador", "9");
            if (ie.length() >= 1 && "J".equals(tipo)) c.put("ie_indicador", "1");

            // Fones default
            if (isNull(c.get("filiacao_fonemae"))) c.put("filiacao_fonemae", "(  )     -    ");
            if (isNull(c.get("filiacao_fonepai"))) c.put("filiacao_fonepai", "(  )     -    ");
            if (isNull(c.get("avalista_cpf"))) c.put("avalista_cpf", "   .   .   -  ");
            if (isNull(c.get("avalista_fone"))) c.put("avalista_fone", "(  )    -    ");

            // Int nulls
            if (isNull(c.get("limite_credito"))) c.put("limite_credito", 0);
            if (isNull(c.get("poupanca"))) c.put("poupanca", 0);
            if (isNull(c.get("id_vendedor"))) c.put("id_vendedor", 0);
            if (isNull(c.get("id_cidades_adi"))) c.put("id_cidades_adi", 0);
            if (isNull(c.get("id_estados_adi"))) c.put("id_estados_adi", 0);
            if (isNull(c.get("id_cidades_emp"))) c.put("id_cidades_emp", 0);
            if (isNull(c.get("id_estados_emp"))) c.put("id_estados_emp", 0);
            if (isNull(c.get("id_cidades_conj"))) c.put("id_cidades_conj", 0);
            if (isNull(c.get("id_estados_conj"))) c.put("id_estados_conj", 0);
            if (isNull(c.get("filiacao_idcidade"))) c.put("filiacao_idcidade", 0);
            if (isNull(c.get("filiacao_idestado"))) c.put("filiacao_idestado", 0);
            if (isNull(c.get("avalista_renda"))) c.put("avalista_renda", 0);
            if (isNull(c.get("avalista_idcidade"))) c.put("avalista_idcidade", 0);
            if (isNull(c.get("avalista_idestado"))) c.put("avalista_idestado", 0);
            if (isNull(c.get("renda_emp"))) c.put("renda_emp", 0);
            if (isNull(c.get("renda_conj"))) c.put("renda_conj", 0);

            // Flags
            if (isNull(c.get("ativo"))) c.put("ativo", 1);
            if (isNull(c.get("id_pais"))) c.put("id_pais", "34");
            if (isNull(c.get("pode_aprazo"))) c.put("pode_aprazo", "S");
            if (isNull(c.get("pode_cartacobranca"))) c.put("pode_cartacobranca", "S");
            if (isNull(c.get("tabela_preco"))) c.put("tabela_preco", "NORMAL");
            if (isNull(c.get("data_cadastro"))) c.put("data_cadastro", nowDate());
            if (isNull(c.get("datahora_alteracao"))) c.put("datahora_alteracao", nowTs());

            // Email lowercase
            String email = safeStr(c.get("email_adi")).toLowerCase();
            c.put("email_adi", email);

            // Nome fallbacks
            String nome = safeStr(c.get("nome"));
            String razao = safeStr(c.get("razao_social"));
            if (nome.isEmpty()) c.put("nome", razao);
            if ("J".equals(tipo) && razao.isEmpty()) c.put("razao_social", nome);

            // TRIM + UPPER + SUBS
            nome = safeStr(c.get("nome")).trim().toUpperCase();
            razao = safeStr(c.get("razao_social")).trim().toUpperCase();
            String endereco = safeStr(c.get("endereco")).trim().toUpperCase();
            String ref = safeStr(c.get("referencia")).trim().toUpperCase();
            String bairro = safeStr(c.get("bairro")).trim().toUpperCase();

            nome = applyAllSubs(nome);
            razao = applyAllSubs(razao);
            endereco = applyAllSubs(endereco);
            ref = applyAllSubs(ref);
            bairro = applyAllSubs(bairro);

            c.put("nome", nome);
            c.put("razao_social", razao);
            c.put("endereco", endereco);
            c.put("referencia", ref);
            c.put("bairro", bairro);

            // TRIM
            numero = safeStr(c.get("numero")).trim();
            cep = safeStr(c.get("cep")).trim();
            ie = safeStr(c.get("ie")).trim();
            String rg = safeStr(c.get("rg")).trim();
            cpf = safeStr(c.get("cpf_cnpj")).trim();
            c.put("numero", numero);
            c.put("cep", cep);
            c.put("ie", ie);
            c.put("rg", rg);
            c.put("cpf_cnpj", cpf);

            // Format CNPJ/CPF
            if (cpf.length() == 14 && !cpf.contains(".")) {
                c.put("cpf_cnpj", formatCnpj(cpf));
            } else if (cpf.length() == 11 && !cpf.contains(".")) {
                c.put("cpf_cnpj", formatCpf(cpf));
            }

            // Fallback cidade/estado
            tipo = safeStr(c.get("tipo"));
            int idCidade = safeInt(c.get("id_cidade"));
            int idCidade2 = safeInt(c.get("id_cidade2"));
            int idEstado = safeInt(c.get("id_estado"));
            int idEstado2 = safeInt(c.get("id_estado2"));

            if (cidadeDefault > 0 && !"E".equals(tipo)) {
                if (idCidade == 0) c.put("id_cidade", cidadeDefault);
                if (idCidade2 == 0) c.put("id_cidade2", cidadeDefault);
            }
            if (estadoDefault > 0 && !"E".equals(tipo)) {
                if (idEstado == 0) c.put("id_estado", estadoDefault);
                if (idEstado2 == 0) c.put("id_estado2", estadoDefault);
            }

            // Ativo = 0 if nome empty
            nome = safeStr(c.get("nome"));
            if (nome.isEmpty()) c.put("ativo", 0);
        }
    }

    // =========================================================================
    //  ESTOQUE — in-memory
    // =========================================================================
    private void popularEstoqueMem(SqlMemoryStore store) {
        store.clear("ajusteestoque");
        store.clear("estoque");
        store.clear("estoquesaldo");
        String now = nowTs();
        List<Map<String,Object>> produtos = store.selectAll("produto");
        for (Map<String,Object> p : produtos) {
            double estoque = safeDbl(p.get("estoque"));
            String tipo = safeStr(p.get("tipo_produto"));
            int idProduto = safeInt(p.get("id"));
            if (estoque <= 0 || !"PRODUTO".equals(tipo)) continue;

            // ajusteestoque
            Map<String,Object> ae = new java.util.LinkedHashMap<String,Object>();
            ae.put("id_empresa", 1);
            ae.put("id_localestoque", 1);
            ae.put("id_naturezaoperacao", 13);
            ae.put("id_usuario", 1);
            ae.put("id_produto", idProduto);
            ae.put("id_lote", 0);
            ae.put("estoque_desejado", estoque);
            ae.put("estoque_antigo", 0.0);
            ae.put("diferenca", estoque);
            ae.put("data_hora", now);
            ae.put("obs", "MIGRACAO");
            ae.put("status", "AEC");
            int aeId = store.insert("ajusteestoque", ae);

            // estoque
            Map<String,Object> es = new java.util.LinkedHashMap<String,Object>();
            es.put("id_empresa", 1);
            es.put("id_localestoque", 1);
            es.put("id_naturezaoperacao", 13);
            es.put("id_controle", aeId);
            es.put("id_produto", idProduto);
            es.put("id_lote", 0);
            es.put("quantidade", estoque);
            es.put("data_hora", now);
            es.put("operacao", "E");
            es.put("tipo", "AE");
            es.put("descricao_tipo", "AJUSTE DE ESTOQUE RAPIDO");
            es.put("descricao", "AJUSTE DE ESTOQUE RAPIDO");
            store.insert("estoque", es);

            // estoquesaldo
            Map<String,Object> esal = new java.util.LinkedHashMap<String,Object>();
            esal.put("id_empresa", 1);
            esal.put("id_produto", idProduto);
            esal.put("id_localestoque", 1);
            esal.put("quantidade", estoque);
            esal.put("datahora_alteracao", now);
            store.insert("estoquesaldo", esal);
        }
    }

    // =========================================================================
    //  SQL MODE — original implementations (unchanged)
    // =========================================================================
    private void ajustarFornecedor(Connection c) {
        String t = "lc_sistemas.fornecedor";

        execIgnore(c, "UPDATE "+t+" SET cnpj_cpf = REPLACE(cnpj_cpf,'-','')", t);
        execIgnore(c, "UPDATE "+t+" SET cnpj_cpf = REPLACE(cnpj_cpf,'/','')", t);
        execIgnore(c, "UPDATE "+t+" SET cnpj_cpf = REPLACE(cnpj_cpf,'.','')", t);
        execIgnore(c, "UPDATE "+t+" SET cnpj_cpf = REPLACE(cnpj_cpf,' ','')", t);
        execIgnore(c, "UPDATE "+t+" SET cnpj_cpf = REPLACE(cnpj_cpf,',','')", t);
        execIgnore(c, "UPDATE "+t+" SET cnpj_cpf = '' WHERE cnpj_cpf IS NULL", t);

        execIgnore(c, "UPDATE "+t+" SET cep = REPLACE(cep,'-','')", t);
        execIgnore(c, "UPDATE "+t+" SET cep = REPLACE(cep,'/','')", t);
        execIgnore(c, "UPDATE "+t+" SET cep = REPLACE(cep,'.','')", t);
        execIgnore(c, "UPDATE "+t+" SET cep = REPLACE(cep,' ','')", t);

        execIgnore(c, "UPDATE "+t+" SET tipo = 'F' WHERE LENGTH(cnpj_cpf) <= 11", t);
        execIgnore(c, "UPDATE "+t+" SET tipo = 'J' WHERE LENGTH(cnpj_cpf) > 11", t);

        execIgnore(c, "UPDATE "+t+" SET tipo_fornecedor = 'Outros' WHERE tipo_fornecedor IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_planoContas  = 0  WHERE id_planoContas IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET nome        = '' WHERE nome IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET razao_social= '' WHERE razao_social IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET ie          = '' WHERE ie IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET endereco    = '' WHERE endereco IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET numero      = '' WHERE numero IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET bairro      = '' WHERE bairro IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET cep         = '' WHERE cep IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET email_site  = '' WHERE email_site IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET obs         = '' WHERE obs IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET ativo       = 1  WHERE ativo IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET fone = '(  )     -    ' WHERE fone IS NULL OR fone = ''", t);
        execIgnore(c, "UPDATE "+t+" SET fax  = '(  )     -    ' WHERE fax  IS NULL OR fax  = ''", t);

        execIgnore(c, "UPDATE "+t+" SET nome         = razao_social WHERE nome IS NULL OR nome = ''", t);
        execIgnore(c, "UPDATE "+t+" SET razao_social = nome         WHERE razao_social IS NULL OR razao_social = ''", t);

        execIgnore(c, "UPDATE "+t+" SET nome         = UPPER(nome)", t);
        execIgnore(c, "UPDATE "+t+" SET razao_social = UPPER(razao_social)", t);
        execIgnore(c, "UPDATE "+t+" SET endereco     = UPPER(endereco)", t);
        execIgnore(c, "UPDATE "+t+" SET bairro       = UPPER(bairro)", t);

        execIgnore(c,
            "UPDATE "+t+" SET cnpj_cpf = CONCAT("
            + "SUBSTRING(cnpj_cpf,1,2),'.',SUBSTRING(cnpj_cpf,3,3),'.',"
            + "SUBSTRING(cnpj_cpf,6,3),'/',SUBSTRING(cnpj_cpf,9,4),'-',SUBSTRING(cnpj_cpf,13,2))"
            + " WHERE LENGTH(cnpj_cpf)=14 AND cnpj_cpf NOT LIKE ('%.%')", t);
        execIgnore(c,
            "UPDATE "+t+" SET cnpj_cpf = CONCAT("
            + "SUBSTRING(cnpj_cpf,1,3),'.',SUBSTRING(cnpj_cpf,4,3),'.',"
            + "SUBSTRING(cnpj_cpf,7,3),'-',SUBSTRING(cnpj_cpf,10,2))"
            + " WHERE LENGTH(cnpj_cpf)=11 AND cnpj_cpf NOT LIKE ('%.%')", t);

        for (String campo : new String[]{"nome","razao_social","endereco","bairro"}) {
            for (String[] s : SUBS)  execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
            for (String[] s : SUBS2) execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
        }
    }

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

        for (String campo : new String[]{"nome","descricao"}) {
            for (String[] s : SUBS)  execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
            for (String[] s : SUBS2) execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
            execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+", LEFT("+campo+",1),'') WHERE LEFT("+campo+",1)  = ' '", t);
            execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+", RIGHT("+campo+",1),'') WHERE RIGHT("+campo+",1) = ' '", t);
        }
    }

    private void ajustarNomeTabela(Connection c, String tabela) {
        String t = "lc_sistemas." + tabela;
        execIgnore(c, "UPDATE "+t+" SET nome = '' WHERE nome IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET nome = TRIM(UPPER(nome))", t);
        for (String[] s : SUBS)  execIgnore(c, "UPDATE "+t+" SET nome = REPLACE(nome,'"+s[0]+"','"+s[1]+"')", t);
        for (String[] s : SUBS2) execIgnore(c, "UPDATE "+t+" SET nome = REPLACE(nome,'"+s[0]+"','"+s[1]+"')", t);
        execIgnore(c, "UPDATE "+t+" SET nome = REPLACE(nome, LEFT(nome,1),'') WHERE LEFT(nome,1) = ' '", t);
        execIgnore(c, "UPDATE "+t+" SET nome = REPLACE(nome, RIGHT(nome,1),'') WHERE RIGHT(nome,1) = ' '", t);
    }

    private void ajustarCategoria(Connection c)    { ajustarNomeTabela(c, "categoria"); }
    private void ajustarSubcategoria(Connection c) { ajustarNomeTabela(c, "subcategoria"); }
    private void ajustarFabricante(Connection c)   { ajustarNomeTabela(c, "fabricante"); }

    private void ajustarUnidade(Connection c) {
        String t = "lc_sistemas.unidade";
        execIgnore(c, "UPDATE "+t+" SET nome = '' WHERE nome IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET descricao = '' WHERE descricao IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET nome      = TRIM(UPPER(nome))", t);
        execIgnore(c, "UPDATE "+t+" SET descricao = TRIM(UPPER(descricao))", t);
        for (String campo : new String[]{"nome","descricao"}) {
            for (String[] s : SUBS)  execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
            for (String[] s : SUBS2) execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
            execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+", LEFT("+campo+",1),'') WHERE LEFT("+campo+",1)  = ' '", t);
            execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+", RIGHT("+campo+",1),'') WHERE RIGHT("+campo+",1) = ' '", t);
        }
    }

    private void popularEstoque(Connection c) {
        execIgnore(c, "DELETE FROM lc_sistemas.ajusteestoque", "limpar ajusteestoque");
        // execIgnore(c, "ALTER TABLE lc_sistemas.ajusteestoque AUTO_INCREMENT = 1", "reset AI ajusteestoque");
        execIgnore(c,
            "INSERT INTO lc_sistemas.ajusteestoque "
            + "(id_empresa, id_localestoque, id_naturezaoperacao, id_usuario, "
            + " id_produto, id_lote, estoque_desejado, estoque_antigo, diferenca, "
            + " data_hora, obs, status) "
            + "SELECT 1, 1, 13, 1, id, 0, estoque, 0.000, estoque, "
            + "  CONCAT(DATE(NOW()),' ',TIME(NOW())), 'MIGRACAO', 'AEC' "
            + "FROM lc_sistemas.produto "
            + "WHERE estoque > 0 AND tipo_produto = 'PRODUTO' AND id_empresa = 1",
            "inserir ajusteestoque");

        execIgnore(c, "DELETE FROM lc_sistemas.estoque", "limpar estoque");
        // execIgnore(c, "ALTER TABLE lc_sistemas.estoque AUTO_INCREMENT = 1", "reset AI estoque");
        execIgnore(c,
            "INSERT INTO lc_sistemas.estoque "
            + "(id_empresa, id_localestoque, id_naturezaoperacao, id_controle, "
            + " id_produto, id_lote, quantidade, data_hora, operacao, tipo, "
            + " descricao_tipo, descricao) "
            + "SELECT 1, 1, 13, id, id_produto, 0, diferenca, data_hora, "
            + "  'E', 'AE', 'AJUSTE DE ESTOQUE RAPIDO', 'AJUSTE DE ESTOQUE RAPIDO' "
            + "FROM lc_sistemas.ajusteestoque WHERE id_empresa = 1",
            "inserir estoque");

        execIgnore(c, "DELETE FROM lc_sistemas.estoquesaldo", "limpar estoquesaldo");
        // execIgnore(c, "ALTER TABLE lc_sistemas.estoquesaldo AUTO_INCREMENT = 1", "reset AI estoquesaldo");
        execIgnore(c,
            "INSERT INTO lc_sistemas.estoquesaldo "
            + "(id_empresa, id_produto, id_localestoque, quantidade, datahora_alteracao) "
            + "SELECT 1, id, 1, estoque, NOW() "
            + "FROM lc_sistemas.produto "
            + "WHERE estoque > 0 AND tipo_produto = 'PRODUTO' AND id_empresa = 1",
            "inserir estoquesaldo");
    }

    private void ajustarCliente(Connection c, int cidadeDefault, int estadoDefault) {
        String t = "lc_sistemas.cliente";

        execIgnore(c, "UPDATE "+t+" SET cpf_cnpj = REPLACE(cpf_cnpj,'-','')", t);
        execIgnore(c, "UPDATE "+t+" SET cpf_cnpj = REPLACE(cpf_cnpj,'/','')", t);
        execIgnore(c, "UPDATE "+t+" SET cpf_cnpj = REPLACE(cpf_cnpj,'.','')", t);
        execIgnore(c, "UPDATE "+t+" SET cpf_cnpj = REPLACE(cpf_cnpj,' ','')", t);
        execIgnore(c, "UPDATE "+t+" SET cpf_cnpj = REPLACE(cpf_cnpj,'_','')", t);
        execIgnore(c, "UPDATE "+t+" SET cpf_cnpj = '' WHERE cpf_cnpj IS NULL", t);

        execIgnore(c, "UPDATE "+t+" SET tipo = 'F' WHERE LENGTH(cpf_cnpj) = 11", t);
        execIgnore(c, "UPDATE "+t+" SET tipo = 'J' WHERE LENGTH(cpf_cnpj) = 14", t);
        execIgnore(c, "UPDATE "+t+" SET tipo = 'E' WHERE LENGTH(cpf_cnpj) > 11 AND LENGTH(cpf_cnpj) < 14", t);
        execIgnore(c, "UPDATE "+t+" SET tipo = 'E' WHERE LENGTH(cpf_cnpj) < 11 AND cpf_cnpj <> ''", t);
        execIgnore(c, "UPDATE "+t+" SET tipo = 'E' WHERE LENGTH(cpf_cnpj) > 14", t);
        execIgnore(c, "UPDATE "+t+" SET tipo = 'F' WHERE cpf_cnpj = '' OR cpf_cnpj IS NULL", t);

        execIgnore(c, "UPDATE "+t+" SET id_cidade  = 5565 WHERE tipo = 'E'", t);
        execIgnore(c, "UPDATE "+t+" SET id_cidade2 = 5565 WHERE tipo = 'E'", t);
        execIgnore(c, "UPDATE "+t+" SET id_estado  = 28   WHERE tipo = 'E'", t);
        execIgnore(c, "UPDATE "+t+" SET id_estado2 = 28   WHERE tipo = 'E'", t);
        execIgnore(c, "UPDATE "+t+" SET id_pais    = '78' WHERE tipo = 'E'", t);

        for (String ch : new String[]{"-","/","."," "}) {
            execIgnore(c, "UPDATE "+t+" SET cep = REPLACE(cep,'"+ch+"','')", t);
        }
        execIgnore(c, "UPDATE "+t+" SET cep = '' WHERE cep IS NULL", t);

        for (String campo : new String[]{
                "ie","im","rg","isuf","razao_social","endereco","referencia","bairro",
                "telefone","tel_comercial","fax","obs","foto","numero_contrato","orgao",
                "referencias","comercial_1","comercial_2","comercial_3","bancaria_1","bancaria_2",
                "pai_adi","mae_adi","sexo_adi","estcivil_adi","apelido_adi","email_adi",
                "empresa","fone_emp","endereco_emp","numero_emp","cep_emp","bairro_emp","cargo_emp",
                "conjuje","cpf_conj","rg_conj","empresa_conj","fone_conj",
                "endereco_conj","numero_conj","cep_conj","bairro_conj","cargo_conj",
                "filiacao_endereco","filiacao_referencia","filiacao_numero","filiacao_cep","filiacao_bairro",
                "avalista_nome","avalista_rg","avalista_endereco","avalista_numero",
                "avalista_cep","avalista_bairro","avalista_empresa","avalista_cargo",
                "endereco2","numero2","referencia2","cep2","bairro2"
        }) {
            execIgnore(c, "UPDATE "+t+" SET "+campo+" = '' WHERE "+campo+" IS NULL", t);
        }

        execIgnore(c, "UPDATE "+t+" SET numero = 'SN' WHERE numero IS NULL OR numero = ''", t);
        execIgnore(c, "UPDATE "+t+" SET ie_indicador    = '9'              WHERE ie_indicador    IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET ie_indicador    = '1'              WHERE LENGTH(ie)>=1 AND tipo='J'", t);
        execIgnore(c, "UPDATE "+t+" SET filiacao_fonemae= '(  )     -    ' WHERE filiacao_fonemae IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET filiacao_fonepai= '(  )     -    ' WHERE filiacao_fonepai IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET avalista_cpf    = '   .   .   -  ' WHERE avalista_cpf IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET avalista_fone   = '(  )    -    '  WHERE avalista_fone IS NULL", t);

        for (String campo : new String[]{
                "limite_credito","poupanca","id_vendedor",
                "id_cidades_adi","id_estados_adi","id_cidades_emp","id_estados_emp",
                "id_cidades_conj","id_estados_conj","filiacao_idcidade","filiacao_idestado",
                "avalista_renda","avalista_idcidade","avalista_idestado","renda_emp","renda_conj"
        }) {
            execIgnore(c, "UPDATE "+t+" SET "+campo+" = 0 WHERE "+campo+" IS NULL", t);
        }

        execIgnore(c, "UPDATE "+t+" SET ativo              = 1          WHERE ativo              IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET id_pais            = '34'       WHERE id_pais            IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET pode_aprazo        = 'S'        WHERE pode_aprazo        IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET pode_cartacobranca = 'S'        WHERE pode_cartacobranca IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET tabela_preco       = 'NORMAL'   WHERE tabela_preco       IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET data_cadastro      = DATE(NOW()) WHERE data_cadastro      IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET datahora_alteracao = NOW()       WHERE datahora_alteracao IS NULL", t);

        execIgnore(c, "UPDATE "+t+" SET email_adi = LOWER(email_adi)", t);

        execIgnore(c, "UPDATE "+t+" SET nome         = razao_social WHERE nome IS NULL OR nome = ''", t);
        execIgnore(c, "UPDATE "+t+" SET razao_social = nome WHERE tipo='J' AND (razao_social IS NULL OR razao_social='')", t);
        execIgnore(c, "UPDATE "+t+" SET nome         = TRIM(UPPER(nome))", t);
        execIgnore(c, "UPDATE "+t+" SET razao_social = TRIM(UPPER(razao_social))", t);
        execIgnore(c, "UPDATE "+t+" SET endereco     = TRIM(UPPER(endereco))", t);
        execIgnore(c, "UPDATE "+t+" SET referencia   = TRIM(UPPER(referencia))", t);
        execIgnore(c, "UPDATE "+t+" SET bairro       = TRIM(UPPER(bairro))", t);

        for (String campo : new String[]{"nome","razao_social","endereco","referencia","bairro"}) {
            for (String[] s : SUBS)  execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
            for (String[] s : SUBS2) execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
        }

        for (String campo : new String[]{"numero","cep","ie","rg","cpf_cnpj"}) {
            execIgnore(c, "UPDATE "+t+" SET "+campo+" = TRIM("+campo+")", t);
        }

        if (cidadeDefault > 0) {
            execIgnore(c,
                "UPDATE "+t+" SET id_cidade  = " + cidadeDefault +
                " WHERE (id_cidade  IS NULL OR id_cidade  = 0) AND tipo <> 'E'", t);
            execIgnore(c,
                "UPDATE "+t+" SET id_cidade2 = " + cidadeDefault +
                " WHERE (id_cidade2 IS NULL OR id_cidade2 = 0) AND tipo <> 'E'", t);
        }
        if (estadoDefault > 0) {
            execIgnore(c,
                "UPDATE "+t+" SET id_estado  = " + estadoDefault +
                " WHERE (id_estado  IS NULL OR id_estado  = 0) AND tipo <> 'E'", t);
            execIgnore(c,
                "UPDATE "+t+" SET id_estado2 = " + estadoDefault +
                " WHERE (id_estado2 IS NULL OR id_estado2 = 0) AND tipo <> 'E'", t);
        }

        execIgnore(c, "UPDATE "+t+" SET ativo = 0 WHERE nome IS NULL OR nome = ''", t);
    }
}
