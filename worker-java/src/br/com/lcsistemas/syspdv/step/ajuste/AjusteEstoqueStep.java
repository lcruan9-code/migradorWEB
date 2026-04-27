package br.com.lcsistemas.syspdv.step.ajuste;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;
import br.com.lcsistemas.syspdv.sql.SqlMemoryStore;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Popula as tabelas de movimentação de estoque com base nos produtos migrados.
 *
 * <p>O que faz:
 * <ul>
 *   <li>Limpa e repopula {@code ajusteestoque}: um registro de "AJUSTE DE ESTOQUE RAPIDO" por produto
 *       que tenha estoque positivo e tipo = "PRODUTO"</li>
 *   <li>Limpa e repopula {@code estoque}: entradas de tipo "AE" vinculadas ao ajusteestoque</li>
 *   <li>Limpa e repopula {@code estoquesaldo}: saldo por produto/local/empresa</li>
 * </ul>
 *
 * <p>Tabelas afetadas:
 * {@code lc_sistemas.ajusteestoque}, {@code lc_sistemas.estoque}, {@code lc_sistemas.estoquesaldo}
 *
 * <p>Pré-requisito: {@code AjusteProdutoStep} deve ter sido executado (estoque corrigido e
 * tipo_produto normalizado).
 *
 * <p>Seleção portal: executa se qualquer uma das três chaves estiver presente:
 * {@code ESTOQUE}, {@code AJUSTE_ESTOQUE} ou {@code ESTOQUE_SALDO}; ou se for migração completa.
 */
public class AjusteEstoqueStep extends AjusteBase {

    @Override
    public String getNome() { return "AjusteEstoque"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        boolean tudo = !ctx.getConfig().temSelecao();
        java.util.Set<String> sel = ctx.getConfig().getTabelasSelecionadas();

        boolean temEstoque = tudo
            || sel.contains("ESTOQUE")
            || sel.contains("AJUSTE_ESTOQUE")
            || sel.contains("ESTOQUE_SALDO");

        if (!temEstoque) {
            contarInseridos(ctx, 0);
            return;
        }

        SqlMemoryStore store = ctx.getMemoryStore();
        if (store != null) {
            popularEstoqueMem(store);
        } else {
            Connection c = ctx.getDestinoConn();
            try { c.setAutoCommit(true); }
            catch (Exception e) { LOG.warning("[AjusteEstoque] setAutoCommit(true): " + e.getMessage()); }
            try {
                popularEstoque(c);
            } finally {
                try { c.setAutoCommit(false); }
                catch (Exception e) { LOG.warning("[AjusteEstoque] setAutoCommit(false): " + e.getMessage()); }
            }
        }
        contarInseridos(ctx, 0);
    }

    // =========================================================================
    //  IN-MEMORY
    // =========================================================================
    private void popularEstoqueMem(SqlMemoryStore store) {
        store.clear("ajusteestoque");
        store.clear("estoque");
        store.clear("estoquesaldo");
        String now = nowTs();
        List<Map<String, Object>> produtos = store.selectAll("produto");
        for (Map<String, Object> p : produtos) {
            double estoque = safeDbl(p.get("estoque"));
            String tipo    = safeStr(p.get("tipo_produto"));
            int idProduto  = safeInt(p.get("id"));
            if (estoque <= 0 || !"PRODUTO".equals(tipo)) continue;

            // ajusteestoque
            Map<String, Object> ae = new LinkedHashMap<>();
            ae.put("id_empresa",           1);
            ae.put("id_localestoque",       1);
            ae.put("id_naturezaoperacao",   13);
            ae.put("id_usuario",            1);
            ae.put("id_produto",            idProduto);
            ae.put("id_lote",              0);
            ae.put("estoque_desejado",      estoque);
            ae.put("estoque_antigo",        0.0);
            ae.put("diferenca",            estoque);
            ae.put("data_hora",            now);
            ae.put("obs",                  "MIGRACAO");
            ae.put("status",               "AEC");
            int aeId = store.insert("ajusteestoque", ae);

            // estoque
            Map<String, Object> es = new LinkedHashMap<>();
            es.put("id_empresa",          1);
            es.put("id_localestoque",      1);
            es.put("id_naturezaoperacao",  13);
            es.put("id_controle",         aeId);
            es.put("id_produto",          idProduto);
            es.put("id_lote",             0);
            es.put("quantidade",          estoque);
            es.put("data_hora",           now);
            es.put("operacao",            "E");
            es.put("tipo",                "AE");
            es.put("descricao_tipo",      "AJUSTE DE ESTOQUE RAPIDO");
            es.put("descricao",           "AJUSTE DE ESTOQUE RAPIDO");
            store.insert("estoque", es);

            // estoquesaldo
            Map<String, Object> esal = new LinkedHashMap<>();
            esal.put("id_empresa",          1);
            esal.put("id_produto",          idProduto);
            esal.put("id_localestoque",      1);
            esal.put("quantidade",          estoque);
            esal.put("datahora_alteracao",  now);
            store.insert("estoquesaldo", esal);
        }
    }

    // =========================================================================
    //  SQL MODE
    // =========================================================================
    private void popularEstoque(Connection c) {
        execIgnore(c, "DELETE FROM lc_sistemas.ajusteestoque", "limpar ajusteestoque");
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
        execIgnore(c,
            "INSERT INTO lc_sistemas.estoquesaldo "
            + "(id_empresa, id_produto, id_localestoque, quantidade, datahora_alteracao) "
            + "SELECT 1, id, 1, estoque, NOW() "
            + "FROM lc_sistemas.produto "
            + "WHERE estoque > 0 AND tipo_produto = 'PRODUTO' AND id_empresa = 1",
            "inserir estoquesaldo");
    }
}
