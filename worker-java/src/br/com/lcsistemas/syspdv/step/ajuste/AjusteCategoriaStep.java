package br.com.lcsistemas.syspdv.step.ajuste;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;
import br.com.lcsistemas.syspdv.sql.SqlMemoryStore;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Ajuste pós-inserção das tabelas <b>categoria</b>, <b>subcategoria</b> e <b>fabricante</b>.
 *
 * <p>O que faz (para cada tabela selecionada):
 * <ul>
 *   <li>Normaliza nulo para string vazia no campo {@code nome}</li>
 *   <li>Aplica TRIM + UPPER + SUBS/SUBS2 no campo {@code nome}</li>
 * </ul>
 *
 * <p>Tabelas afetadas:
 * {@code lc_sistemas.categoria}, {@code lc_sistemas.subcategoria}, {@code lc_sistemas.fabricante}
 *
 * <p>Pré-requisito: {@code CategoriaStep}, {@code SubcategoriaStep} e {@code FabricanteStep}
 * devem ter sido executados.
 *
 * <p>Seleção portal: cada sub-tabela verifica sua chave individualmente
 * ({@code CATEGORIA}, {@code SUBCATEGORIA}, {@code FABRICANTE}).
 */
public class AjusteCategoriaStep extends AjusteBase {

    @Override
    public String getNome() { return "AjusteCategoria"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        boolean tudo = !ctx.getConfig().temSelecao();
        java.util.Set<String> sel = ctx.getConfig().getTabelasSelecionadas();

        boolean temCategoria    = tudo || sel.contains("CATEGORIA");
        boolean temSubcategoria = tudo || sel.contains("SUBCATEGORIA");
        boolean temFabricante   = tudo || sel.contains("FABRICANTE");

        if (!temCategoria && !temSubcategoria && !temFabricante) {
            contarInseridos(ctx, 0);
            return;
        }

        SqlMemoryStore store = ctx.getMemoryStore();
        if (store != null) {
            if (temCategoria)    ajustarCategoriaMem(store, "categoria");
            if (temSubcategoria) ajustarCategoriaMem(store, "subcategoria");
            if (temFabricante)   ajustarCategoriaMem(store, "fabricante");
        } else {
            Connection c = ctx.getDestinoConn();
            try { c.setAutoCommit(true); }
            catch (Exception e) { LOG.warning("[AjusteCategoria] setAutoCommit(true): " + e.getMessage()); }
            try {
                if (temCategoria)    ajustarNomeTabela(c, "categoria");
                if (temSubcategoria) ajustarNomeTabela(c, "subcategoria");
                if (temFabricante)   ajustarNomeTabela(c, "fabricante");
            } finally {
                try { c.setAutoCommit(false); }
                catch (Exception e) { LOG.warning("[AjusteCategoria] setAutoCommit(false): " + e.getMessage()); }
            }
        }
        contarInseridos(ctx, 0);
    }

    // =========================================================================
    //  IN-MEMORY
    // =========================================================================
    private void ajustarCategoriaMem(SqlMemoryStore store, String table) {
        List<Map<String, Object>> rows = store.selectAll(table);
        for (Map<String, Object> r : rows) {
            if (isNull(r.get("nome"))) r.put("nome", "");
            String nome = safeStr(r.get("nome")).trim().toUpperCase();
            nome = applyAllSubs(nome);
            r.put("nome", nome);
        }
    }

    // =========================================================================
    //  SQL MODE
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
}
