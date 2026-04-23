package br.com.lcsistemas.host.step;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;
import java.sql.Connection;

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
        Connection c = ctx.getDestinoConn();
        int cidadeDefault = ctx.getConfig().getCidadeDefaultId();
        int estadoDefault = ctx.getConfig().getEstadoDefaultId();
        ajustarFornecedor(c);
        ajustarProduto(c);
        ajustarCategoria(c);
        ajustarSubcategoria(c);
        ajustarFabricante(c);
        ajustarUnidade(c);
        ajustarCliente(c, cidadeDefault, estadoDefault);
        popularEstoque(c);
        contarInseridos(ctx, 0);
    }

    // =========================================================================
    //  FORNECEDOR
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

    // =========================================================================
    //  PRODUTO
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

        for (String campo : new String[]{"nome","descricao"}) {
            for (String[] s : SUBS)  execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
            for (String[] s : SUBS2) execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
            execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+", LEFT("+campo+",1),'') WHERE LEFT("+campo+",1)  = ' '", t);
            execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+", RIGHT("+campo+",1),'') WHERE RIGHT("+campo+",1) = ' '", t);
        }
    }

    // =========================================================================
    //  CATEGORIA, SUBCATEGORIA, FABRICANTE
    // =========================================================================
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

    // =========================================================================
    //  UNIDADE
    // =========================================================================
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

    // =========================================================================
    //  ESTOQUE — popula ajusteestoque, estoque e estoquesaldo
    // =========================================================================
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

    // =========================================================================
    //  CLIENTE
    // =========================================================================
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
