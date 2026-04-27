package br.com.lcsistemas.syspdv.step.ajuste;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;
import br.com.lcsistemas.syspdv.sql.SqlMemoryStore;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Ajuste pós-inserção da tabela <b>unidade</b>.
 *
 * <p>O que faz:
 * <ul>
 *   <li>Normaliza nulos para string vazia nos campos {@code nome} e {@code descricao}</li>
 *   <li>Aplica TRIM + UPPER + SUBS/SUBS2 em ambos os campos</li>
 * </ul>
 *
 * <p>Tabelas afetadas: {@code lc_sistemas.unidade}
 *
 * <p>Pré-requisito: {@code UnidadeStep} deve ter sido executado e inserido os registros.
 *
 * <p>Seleção portal: executa se {@code tudo || sel.contains("UNIDADE")}.
 */
public class AjusteUnidadeStep extends AjusteBase {

    @Override
    public String getNome() { return "AjusteUnidade"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        boolean tudo = !ctx.getConfig().temSelecao();
        java.util.Set<String> sel = ctx.getConfig().getTabelasSelecionadas();

        if (!tudo && !sel.contains("UNIDADE")) {
            contarInseridos(ctx, 0);
            return;
        }

        SqlMemoryStore store = ctx.getMemoryStore();
        if (store != null) {
            ajustarUnidadeMem(store);
        } else {
            Connection c = ctx.getDestinoConn();
            try { c.setAutoCommit(true); }
            catch (Exception e) { LOG.warning("[AjusteUnidade] setAutoCommit(true): " + e.getMessage()); }
            try {
                ajustarUnidade(c);
            } finally {
                try { c.setAutoCommit(false); }
                catch (Exception e) { LOG.warning("[AjusteUnidade] setAutoCommit(false): " + e.getMessage()); }
            }
        }
        contarInseridos(ctx, 0);
    }

    // =========================================================================
    //  IN-MEMORY
    // =========================================================================
    private void ajustarUnidadeMem(SqlMemoryStore store) {
        List<Map<String, Object>> rows = store.selectAll("unidade");
        for (Map<String, Object> r : rows) {
            if (isNull(r.get("nome")))      r.put("nome",      "");
            if (isNull(r.get("descricao"))) r.put("descricao", "");
            String nome = safeStr(r.get("nome")).trim().toUpperCase();
            String desc = safeStr(r.get("descricao")).trim().toUpperCase();
            nome = applyAllSubs(nome);
            desc = applyAllSubs(desc);
            r.put("nome",      nome);
            r.put("descricao", desc);
        }
    }

    // =========================================================================
    //  SQL MODE
    // =========================================================================
    private void ajustarUnidade(Connection c) {
        String t = "lc_sistemas.unidade";
        execIgnore(c, "UPDATE "+t+" SET nome = '' WHERE nome IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET descricao = '' WHERE descricao IS NULL", t);
        execIgnore(c, "UPDATE "+t+" SET nome      = TRIM(UPPER(nome))", t);
        execIgnore(c, "UPDATE "+t+" SET descricao = TRIM(UPPER(descricao))", t);
        for (String campo : new String[]{"nome", "descricao"}) {
            for (String[] s : SUBS)  execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
            for (String[] s : SUBS2) execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+",'"+s[0]+"','"+s[1]+"')", t);
            execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+", LEFT("+campo+",1),'') WHERE LEFT("+campo+",1)  = ' '", t);
            execIgnore(c, "UPDATE "+t+" SET "+campo+" = REPLACE("+campo+", RIGHT("+campo+",1),'') WHERE RIGHT("+campo+",1) = ' '", t);
        }
    }
}
