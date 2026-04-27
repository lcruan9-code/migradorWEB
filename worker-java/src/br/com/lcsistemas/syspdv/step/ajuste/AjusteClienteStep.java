package br.com.lcsistemas.syspdv.step.ajuste;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;
import br.com.lcsistemas.syspdv.sql.SqlMemoryStore;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Ajuste pós-inserção da tabela <b>cliente</b>.
 *
 * <p>O que faz:
 * <ul>
 *   <li>Normaliza CPF/CNPJ e CEP (remove pontuação)</li>
 *   <li>Determina tipo "F", "J" ou "E" pelo comprimento do documento</li>
 *   <li>Define cidade/estado de estrangeiros (id_cidade=5565, id_estado=28, id_pais=78)</li>
 *   <li>Preenche defaults em todos os campos nulos (endereço, referências, filiação, etc.)</li>
 *   <li>Aplica UPPER + TRIM + SUBS/SUBS2 nos campos de texto</li>
 *   <li>Formata CPF/CNPJ com pontuação</li>
 *   <li>Aplica cidade/estado padrão do config para registros sem localização</li>
 * </ul>
 *
 * <p>Tabelas afetadas: {@code lc_sistemas.cliente}
 *
 * <p>Pré-requisito: {@code ClienteStep} deve ter sido executado e inserido os registros.
 *
 * <p>Seleção portal: executa se {@code tudo || sel.contains("CLIENTE")}.
 */
public class AjusteClienteStep extends AjusteBase {

    @Override
    public String getNome() { return "AjusteCliente"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        boolean tudo = !ctx.getConfig().temSelecao();
        java.util.Set<String> sel = ctx.getConfig().getTabelasSelecionadas();

        if (!tudo && !sel.contains("CLIENTE")) {
            contarInseridos(ctx, 0);
            return;
        }

        int cidadeDefault = ctx.getConfig().getCidadeDefaultId();
        int estadoDefault = ctx.getConfig().getEstadoDefaultId();

        SqlMemoryStore store = ctx.getMemoryStore();
        if (store != null) {
            ajustarClienteMem(store, cidadeDefault, estadoDefault);
        } else {
            Connection c = ctx.getDestinoConn();
            try { c.setAutoCommit(true); }
            catch (Exception e) { LOG.warning("[AjusteCliente] setAutoCommit(true): " + e.getMessage()); }
            try {
                ajustarCliente(c, cidadeDefault, estadoDefault);
            } finally {
                try { c.setAutoCommit(false); }
                catch (Exception e) { LOG.warning("[AjusteCliente] setAutoCommit(false): " + e.getMessage()); }
            }
        }
        contarInseridos(ctx, 0);
    }

    // =========================================================================
    //  IN-MEMORY
    // =========================================================================
    private void ajustarClienteMem(SqlMemoryStore store, int cidadeDefault, int estadoDefault) {
        List<Map<String, Object>> rows = store.selectAll("cliente");
        for (Map<String, Object> c : rows) {
            // Strip CPF/CNPJ
            String cpf = stripDoc(c.get("cpf_cnpj"));
            c.put("cpf_cnpj", cpf);

            // Tipo
            String tipo = "F";
            if      (cpf.length() == 11)                           tipo = "F";
            else if (cpf.length() == 14)                           tipo = "J";
            else if (!cpf.isEmpty())                               tipo = "E";
            c.put("tipo", tipo);

            // Estrangeiro
            if ("E".equals(tipo)) {
                c.put("id_cidade",  5565);
                c.put("id_cidade2", 5565);
                c.put("id_estado",  28);
                c.put("id_estado2", 28);
                c.put("id_pais",    "78");
            }

            // Strip CEP
            String cep = stripDoc(c.get("cep"));
            c.put("cep", cep);

            // String nulls to ''
            if (isNull(c.get("ie")))                  c.put("ie", "");
            if (isNull(c.get("im")))                  c.put("im", "");
            if (isNull(c.get("rg")))                  c.put("rg", "");
            if (isNull(c.get("isuf")))                c.put("isuf", "");
            if (isNull(c.get("razao_social")))         c.put("razao_social", "");
            if (isNull(c.get("endereco")))             c.put("endereco", "");
            if (isNull(c.get("referencia")))           c.put("referencia", "");
            if (isNull(c.get("bairro")))               c.put("bairro", "");
            if (isNull(c.get("telefone")))             c.put("telefone", "");
            if (isNull(c.get("tel_comercial")))        c.put("tel_comercial", "");
            if (isNull(c.get("fax")))                  c.put("fax", "");
            if (isNull(c.get("obs")))                  c.put("obs", "");
            if (isNull(c.get("foto")))                 c.put("foto", "");
            if (isNull(c.get("numero_contrato")))      c.put("numero_contrato", "");
            if (isNull(c.get("orgao")))                c.put("orgao", "");
            if (isNull(c.get("referencias")))          c.put("referencias", "");
            if (isNull(c.get("comercial_1")))          c.put("comercial_1", "");
            if (isNull(c.get("comercial_2")))          c.put("comercial_2", "");
            if (isNull(c.get("comercial_3")))          c.put("comercial_3", "");
            if (isNull(c.get("bancaria_1")))           c.put("bancaria_1", "");
            if (isNull(c.get("bancaria_2")))           c.put("bancaria_2", "");
            if (isNull(c.get("pai_adi")))              c.put("pai_adi", "");
            if (isNull(c.get("mae_adi")))              c.put("mae_adi", "");
            if (isNull(c.get("sexo_adi")))             c.put("sexo_adi", "");
            if (isNull(c.get("estcivil_adi")))         c.put("estcivil_adi", "");
            if (isNull(c.get("apelido_adi")))          c.put("apelido_adi", "");
            if (isNull(c.get("email_adi")))            c.put("email_adi", "");
            if (isNull(c.get("empresa")))              c.put("empresa", "");
            if (isNull(c.get("fone_emp")))             c.put("fone_emp", "");
            if (isNull(c.get("endereco_emp")))         c.put("endereco_emp", "");
            if (isNull(c.get("numero_emp")))           c.put("numero_emp", "");
            if (isNull(c.get("cep_emp")))              c.put("cep_emp", "");
            if (isNull(c.get("bairro_emp")))           c.put("bairro_emp", "");
            if (isNull(c.get("cargo_emp")))            c.put("cargo_emp", "");
            if (isNull(c.get("conjuje")))              c.put("conjuje", "");
            if (isNull(c.get("cpf_conj")))             c.put("cpf_conj", "");
            if (isNull(c.get("rg_conj")))              c.put("rg_conj", "");
            if (isNull(c.get("empresa_conj")))         c.put("empresa_conj", "");
            if (isNull(c.get("fone_conj")))            c.put("fone_conj", "");
            if (isNull(c.get("endereco_conj")))        c.put("endereco_conj", "");
            if (isNull(c.get("numero_conj")))          c.put("numero_conj", "");
            if (isNull(c.get("cep_conj")))             c.put("cep_conj", "");
            if (isNull(c.get("bairro_conj")))          c.put("bairro_conj", "");
            if (isNull(c.get("cargo_conj")))           c.put("cargo_conj", "");
            if (isNull(c.get("filiacao_endereco")))    c.put("filiacao_endereco", "");
            if (isNull(c.get("filiacao_referencia")))  c.put("filiacao_referencia", "");
            if (isNull(c.get("filiacao_numero")))      c.put("filiacao_numero", "");
            if (isNull(c.get("filiacao_cep")))         c.put("filiacao_cep", "");
            if (isNull(c.get("filiacao_bairro")))      c.put("filiacao_bairro", "");
            if (isNull(c.get("avalista_nome")))        c.put("avalista_nome", "");
            if (isNull(c.get("avalista_rg")))          c.put("avalista_rg", "");
            if (isNull(c.get("avalista_endereco")))    c.put("avalista_endereco", "");
            if (isNull(c.get("avalista_numero")))      c.put("avalista_numero", "");
            if (isNull(c.get("avalista_cep")))         c.put("avalista_cep", "");
            if (isNull(c.get("avalista_bairro")))      c.put("avalista_bairro", "");
            if (isNull(c.get("avalista_empresa")))     c.put("avalista_empresa", "");
            if (isNull(c.get("avalista_cargo")))       c.put("avalista_cargo", "");
            if (isNull(c.get("endereco2")))            c.put("endereco2", "");
            if (isNull(c.get("numero2")))              c.put("numero2", "");
            if (isNull(c.get("referencia2")))          c.put("referencia2", "");
            if (isNull(c.get("cep2")))                 c.put("cep2", "");
            if (isNull(c.get("bairro2")))              c.put("bairro2", "");

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
            if (isNull(c.get("avalista_cpf")))     c.put("avalista_cpf",    "   .   .   -  ");
            if (isNull(c.get("avalista_fone")))    c.put("avalista_fone",   "(  )    -    ");

            // Int nulls
            if (isNull(c.get("limite_credito")))     c.put("limite_credito", 0);
            if (isNull(c.get("poupanca")))           c.put("poupanca", 0);
            if (isNull(c.get("id_vendedor")))        c.put("id_vendedor", 0);
            if (isNull(c.get("id_cidades_adi")))     c.put("id_cidades_adi", 0);
            if (isNull(c.get("id_estados_adi")))     c.put("id_estados_adi", 0);
            if (isNull(c.get("id_cidades_emp")))     c.put("id_cidades_emp", 0);
            if (isNull(c.get("id_estados_emp")))     c.put("id_estados_emp", 0);
            if (isNull(c.get("id_cidades_conj")))    c.put("id_cidades_conj", 0);
            if (isNull(c.get("id_estados_conj")))    c.put("id_estados_conj", 0);
            if (isNull(c.get("filiacao_idcidade")))  c.put("filiacao_idcidade", 0);
            if (isNull(c.get("filiacao_idestado")))  c.put("filiacao_idestado", 0);
            if (isNull(c.get("avalista_renda")))     c.put("avalista_renda", 0);
            if (isNull(c.get("avalista_idcidade")))  c.put("avalista_idcidade", 0);
            if (isNull(c.get("avalista_idestado")))  c.put("avalista_idestado", 0);
            if (isNull(c.get("renda_emp")))          c.put("renda_emp", 0);
            if (isNull(c.get("renda_conj")))         c.put("renda_conj", 0);

            // Flags
            if (isNull(c.get("ativo")))                c.put("ativo", 1);
            if (isNull(c.get("id_pais")))              c.put("id_pais", "34");
            if (isNull(c.get("pode_aprazo")))          c.put("pode_aprazo", "S");
            if (isNull(c.get("pode_cartacobranca")))   c.put("pode_cartacobranca", "S");
            if (isNull(c.get("tabela_preco")))         c.put("tabela_preco", "NORMAL");
            if (isNull(c.get("data_cadastro")))        c.put("data_cadastro", nowDate());
            if (isNull(c.get("datahora_alteracao")))   c.put("datahora_alteracao", nowTs());

            // Email lowercase
            String email = safeStr(c.get("email_adi")).toLowerCase();
            c.put("email_adi", email);

            // Nome fallbacks
            String nome  = safeStr(c.get("nome"));
            String razao = safeStr(c.get("razao_social"));
            if (nome.isEmpty())                              c.put("nome",         razao);
            if ("J".equals(tipo) && razao.isEmpty())         c.put("razao_social", nome);

            // TRIM + UPPER + SUBS
            nome     = safeStr(c.get("nome")).trim().toUpperCase();
            razao    = safeStr(c.get("razao_social")).trim().toUpperCase();
            String endereco  = safeStr(c.get("endereco")).trim().toUpperCase();
            String ref       = safeStr(c.get("referencia")).trim().toUpperCase();
            String bairro    = safeStr(c.get("bairro")).trim().toUpperCase();

            nome     = applyAllSubs(nome);
            razao    = applyAllSubs(razao);
            endereco = applyAllSubs(endereco);
            ref      = applyAllSubs(ref);
            bairro   = applyAllSubs(bairro);

            c.put("nome",         nome);
            c.put("razao_social", razao);
            c.put("endereco",     endereco);
            c.put("referencia",   ref);
            c.put("bairro",       bairro);

            // TRIM
            numero = safeStr(c.get("numero")).trim();
            cep    = safeStr(c.get("cep")).trim();
            ie     = safeStr(c.get("ie")).trim();
            String rg = safeStr(c.get("rg")).trim();
            cpf    = safeStr(c.get("cpf_cnpj")).trim();
            c.put("numero",    numero);
            c.put("cep",       cep);
            c.put("ie",        ie);
            c.put("rg",        rg);
            c.put("cpf_cnpj",  cpf);

            // Format CNPJ/CPF
            if (cpf.length() == 14 && !cpf.contains(".")) {
                c.put("cpf_cnpj", formatCnpj(cpf));
            } else if (cpf.length() == 11 && !cpf.contains(".")) {
                c.put("cpf_cnpj", formatCpf(cpf));
            }

            // Fallback cidade/estado
            tipo = safeStr(c.get("tipo"));
            int idCidade  = safeInt(c.get("id_cidade"));
            int idCidade2 = safeInt(c.get("id_cidade2"));
            int idEstado  = safeInt(c.get("id_estado"));
            int idEstado2 = safeInt(c.get("id_estado2"));

            if (cidadeDefault > 0 && !"E".equals(tipo)) {
                if (idCidade  == 0) c.put("id_cidade",  cidadeDefault);
                if (idCidade2 == 0) c.put("id_cidade2", cidadeDefault);
            }
            if (estadoDefault > 0 && !"E".equals(tipo)) {
                if (idEstado  == 0) c.put("id_estado",  estadoDefault);
                if (idEstado2 == 0) c.put("id_estado2", estadoDefault);
            }

            // Ativo = 0 if nome empty
            nome = safeStr(c.get("nome"));
            if (nome.isEmpty()) c.put("ativo", 0);
        }
    }

    // =========================================================================
    //  SQL MODE
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

        for (String ch : new String[]{"-", "/", ".", " "}) {
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

        for (String campo : new String[]{"nome", "razao_social", "endereco", "referencia", "bairro"}) {
            for (String[] s : SUBS)  execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
            for (String[] s : SUBS2) execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
        }

        for (String campo : new String[]{"numero", "cep", "ie", "rg", "cpf_cnpj"}) {
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
