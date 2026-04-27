package br.com.lcsistemas.syspdv.step.ajuste;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;
import br.com.lcsistemas.syspdv.sql.SqlMemoryStore;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Ajuste pós-inserção da tabela <b>fornecedor</b>.
 *
 * <p>O que faz:
 * <ul>
 *   <li>Normaliza CNPJ/CPF e CEP (remove pontuação)</li>
 *   <li>Determina tipo "F" ou "J" pelo comprimento do documento</li>
 *   <li>Preenche defaults de campos nulos (tipo_fornecedor, nome, ie, endereço, etc.)</li>
 *   <li>Aplica UPPER + SUBS/SUBS2 nos campos de texto</li>
 *   <li>Formata CNPJ e CPF com pontuação</li>
 * </ul>
 *
 * <p>Tabelas afetadas: {@code lc_sistemas.fornecedor}
 *
 * <p>Pré-requisito: {@code FornecedorStep} deve ter sido executado e inserido os registros.
 *
 * <p>Seleção portal: executa se {@code tudo || sel.contains("FORNECEDORES")}.
 */
public class AjusteFornecedorStep extends AjusteBase {

    @Override
    public String getNome() { return "AjusteFornecedor"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        boolean tudo = !ctx.getConfig().temSelecao();
        java.util.Set<String> sel = ctx.getConfig().getTabelasSelecionadas();

        if (!tudo && !sel.contains("FORNECEDORES")) {
            contarInseridos(ctx, 0);
            return;
        }

        SqlMemoryStore store = ctx.getMemoryStore();
        if (store != null) {
            ajustarFornecedorMem(store);
        } else {
            Connection c = ctx.getDestinoConn();
            try { c.setAutoCommit(true); }
            catch (Exception e) { LOG.warning("[AjusteFornecedor] setAutoCommit(true): " + e.getMessage()); }
            try {
                ajustarFornecedor(c);
            } finally {
                try { c.setAutoCommit(false); }
                catch (Exception e) { LOG.warning("[AjusteFornecedor] setAutoCommit(false): " + e.getMessage()); }
            }
        }
        contarInseridos(ctx, 0);
    }

    // =========================================================================
    //  IN-MEMORY
    // =========================================================================
    private void ajustarFornecedorMem(SqlMemoryStore store) {
        List<Map<String, Object>> rows = store.selectAll("fornecedor");
        for (Map<String, Object> r : rows) {
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
            if (isNull(r.get("id_planoContas")))  r.put("id_planoContas", 0);
            if (isNull(r.get("nome")))             r.put("nome", "");
            if (isNull(r.get("razao_social")))     r.put("razao_social", "");
            if (isNull(r.get("ie")))               r.put("ie", "");
            if (isNull(r.get("endereco")))         r.put("endereco", "");
            if (isNull(r.get("numero")))           r.put("numero", "");
            if (isNull(r.get("bairro")))           r.put("bairro", "");
            if (isNull(r.get("email_site")))       r.put("email_site", "");
            if (isNull(r.get("obs")))              r.put("obs", "");
            if (isNull(r.get("ativo")))            r.put("ativo", 1);

            String fone = safeStr(r.get("fone"));
            String fax  = safeStr(r.get("fax"));
            if (fone.isEmpty()) r.put("fone", "(  )     -    ");
            if (fax.isEmpty())  r.put("fax",  "(  )     -    ");

            // Nome fallbacks
            String nome  = safeStr(r.get("nome"));
            String razao = safeStr(r.get("razao_social"));
            if (nome.isEmpty())  r.put("nome",         razao);
            if (razao.isEmpty()) r.put("razao_social", nome);

            // Upper + SUBS
            nome     = safeStr(r.get("nome")).toUpperCase();
            razao    = safeStr(r.get("razao_social")).toUpperCase();
            String endereco = safeStr(r.get("endereco")).toUpperCase();
            String bairro   = safeStr(r.get("bairro")).toUpperCase();

            nome     = applyAllSubs(nome);
            razao    = applyAllSubs(razao);
            endereco = applyAllSubs(endereco);
            bairro   = applyAllSubs(bairro);

            r.put("nome",         nome);
            r.put("razao_social", razao);
            r.put("endereco",     endereco);
            r.put("bairro",       bairro);

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
    //  SQL MODE
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

        for (String campo : new String[]{"nome", "razao_social", "endereco", "bairro"}) {
            for (String[] s : SUBS)  execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
            for (String[] s : SUBS2) execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
        }
    }
}
